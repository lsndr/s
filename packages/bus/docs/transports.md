# Transports

A transport moves events between publishers and handlers.

## Supported transports

- [Redis Streams](#redis-streams) (`RedisStreamsTransport`)
- [RabbitMQ](#rabbitmq) (`RabbitmqTransport`)
- Any custom transport implementing the `Transport` interface

## Redis Streams

Requires Redis 8.2 or later.

```typescript
BusModule.forRoot({
  transport: RedisStreamsTransport.withConfig({
    url: 'redis://localhost:6379',
    group: 'billing',
  }),
});
```

### Options

| Option            | Default      | Description                                                                                                                                   |
| ----------------- | ------------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`             | _required_   | Redis connection URL.                                                                                                                         |
| `group`           | _required_   | Consumer group name. Instances with the same `group` split the entries between them. A different `group` receives every new entry on its own. |
| `consumer`        | `<hostname>` | Consumer name in a group. An event is delivered to a single consumer of the group.                                                            |
| `retries`         | see below    | How failed entries are retried, see [`retries`](#retries).                                                                                    |
| `keyPrefix`       | `events:`    | Prefix of all keys: streams and the hashes that record succeeded handlers.                                                                    |
| `concurrency`     | `10`         | Slots of each stream on one instance, see [Concurrency](#concurrency).                                                                        |
| `trim`            | see below    | How streams are trimmed, see [`trim`](#trim).                                                                                                 |
| `shutdownTimeout` | none         | Milliseconds to wait for entries in flight on shutdown. Unset waits for all of them.                                                          |

#### `retries`

| Option            | Default                | Description                                                                                                                         |
| ----------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `max`             | `5`                    | Retries before an entry is dropped.                                                                                                 |
| `delay`           | `300000`               | Milliseconds an entry has to be pending before it is claimed again. Also how long after a crash a consumer's entries are recovered. |
| `reclaimInterval` | `max(delay / 4, 1000)` | Milliseconds between checks for entries to retry.                                                                                   |

#### `trim`

| Option      | Default | Description                                                                                      |
| ----------- | ------- | ------------------------------------------------------------------------------------------------ |
| `maxLength` | `100`   | Approximate stream length above which entries are trimmed.                                       |
| `onlyAcked` | `true`  | Trim only entries every group has acknowledged. `false` makes the cap hard and can lose entries. |

### Concurrency

Each stream has `concurrency` slots on each instance. A stream is read only while it has a free slot, and entries free their slot when they finish. There is no global limit across streams.

Slots decide when a stream is read, not how many entries it runs. A read returns up to `concurrency` entries and they run right away, so a stream can run up to `2 * concurrency - 1` read entries at once, and reclaimed entries come on top of that.

Slots keep the slowest handler from stopping the reads of the other streams. A stream with no free slot is left out of the next read, so it can only hold back its own entries. All streams are still read with a single blocking read on one connection, so no connection per stream is needed.

The `redis.stream.*` [metrics](telemetry.md#metrics) show how full the slots are.

### Delivery guarantees

Delivery is **at-least-once**: make handlers idempotent.

- An entry is acknowledged with `XACK` once every handler has succeeded or failed permanently.
- A failed entry stays pending and is claimed again, by any instance, once it has been pending for `retries.delay` ms. This repeats up to `retries.max` times. After that the entry is logged as permanently failed and acknowledged, so it is tried at most `retries.max + 1` times.
- Entries of a crashed consumer are recovered the same way, and each recovery counts as a retry. While handlers run, their entry is kept from looking abandoned.
- Handlers that succeeded are recorded, so a redelivery only runs the handlers that haven't succeeded yet. If recording fails, that handler may run again.
- If Redis fails while an entry is processed, the entry stays pending and is retried.
- With `trim.onlyAcked: false`, trimming can drop entries that haven't been handled, see [Trimming](#trimming).

### Trimming

Publishing uses `XADD … MAXLEN ~ <trim.maxLength> ACKED`. Entries are trimmed only after every consumer group has acknowledged them, so `trim.maxLength` is a soft cap that unacknowledged entries can exceed.

With `trim.onlyAcked: false` the cap is hard: unhandled entries are trimmed too, and lost. Trimming is approximate and removes whole stream nodes (100 entries each by default).

### Consumers and shutdown

- The consumer name defaults to the host name, so instances on one host share it and appear as one consumer. This doesn't change distribution or recovery: new entries go to whichever instance reads them, and any consumer can reclaim pending entries after `retries.delay`.
- On shutdown the transport stops reading, waits for entries in flight (up to `shutdownTimeout`), and removes the consumer from its groups unless it still has pending entries.
- Consumers that crashed or were renamed stay registered in the group. Each running instance periodically removes the consumers that have no pending entries and have been idle for more than an hour.

## RabbitMQ

```typescript
BusModule.forRoot({
  transport: RabbitmqTransport.withConfig({
    url: 'amqp://localhost',
    group: 'billing',
  }),
});
```

### Options

| Option          | Default    | Description                                                                                                                                                                       |
| --------------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`           | _required_ | AMQP connection URL.                                                                                                                                                              |
| `group`         | _required_ | Consumer group name, used as a prefix of queue names. Instances with the same `group` split the messages between them. A different `group` receives every new message on its own. |
| `retries.max`   | `5`        | Retries before a message is dropped.                                                                                                                                              |
| `retries.delay` | `5000`     | Milliseconds between retries.                                                                                                                                                     |

### Topology

- Events are published to the durable `events` direct exchange, with the event class name as the routing key. Messages are persistent and use publisher confirms.
- Each handler and event pair gets a queue named `<group>.<HandlerClass>.<EventClass>`.

### Delivery guarantees

Delivery is **at-least-once**: make handlers idempotent.

- Each handler has its own queue, so handlers of the same event succeed and retry independently.
- A message is acknowledged after its handler finishes. If the consumer dies first, the broker delivers it again.
- A failed message is nacked into `<queue>.delay` (through the `events.delay` exchange), waits `retries.delay` ms, and returns to the original queue. This repeats up to `retries.max` times.
- After that the message is logged as permanently failed and acknowledged, which drops it. A message is therefore tried at most `retries.max + 1` times.
- `publish` resolves once the broker confirms the message and rejects if the broker nacks it.
