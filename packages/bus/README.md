# @lsndr/bus

Event bus for [NestJS](https://nestjs.com/) with decorator-based event handlers, middleware, delayed retries, and RabbitMQ and Redis Streams transports.

## Installation

The package is published to GitHub Packages and requires Node.js >= 24.

```bash
npm install @lsndr/bus
```

Peer dependencies: `@nestjs/common`, `@nestjs/core`, `reflect-metadata`. For [telemetry](#telemetry), also install `@opentelemetry/api` (and `@opentelemetry/api-logs` to report exceptions). To use the built-in RabbitMQ transport, also install the optional peers:

```bash
npm install amqplib class-transformer
```

For the Redis Streams transport (requires **Redis 8.2+**), install `redis` instead of `amqplib`:

```bash
npm install redis class-transformer
```

## Usage

### Registering the module

```typescript
import { Module } from '@nestjs/common';
import { BusModule, RabbitmqTransport } from '@lsndr/bus';

@Module({
  imports: [
    BusModule.forRoot({
      transport: {
        provider: RabbitmqTransport,
        config: { url: 'amqp://localhost', group: 'billing' },
      },
      providers: [OrderPlacedHandler],
    }),
  ],
})
export class AppModule {}
```

`BusModule` is global and exports `Dispatcher` and `Health`.

### Defining events

Events are plain classes. The class name is used as the routing key, and payloads are serialized with `class-transformer`.

```typescript
export class OrderPlaced {
  constructor(public readonly orderId: string) {}
}
```

### Publishing events

```typescript
import { Inject, Injectable } from '@nestjs/common';
import { Dispatcher } from '@lsndr/bus';

@Injectable()
export class OrderService {
  constructor(@Inject(Dispatcher) private readonly dispatcher: Dispatcher) {}

  async place(orderId: string) {
    await this.dispatcher.publish(new OrderPlaced(orderId), {
      id: 'custom-id', // optional, a UUID v7 is generated if omitted
      publishedAt: new Date(), // optional, defaults to now
      meta: { correlationId: 'abc-123' }, // optional
    });
  }
}
```

To publish several events in one call, pass a list. Each item has its own options:

```typescript
await this.dispatcher.publish([
  { event: new OrderPlaced(orderId), options: { id: 'custom-id' } },
  { event: new InvoiceIssued(orderId) },
]);
```

With an `Outbox`, the whole list is stored by a single `save` call.

### Handling events

```typescript
import { EventHandler, type EventPayload } from '@lsndr/bus';

@EventHandler(OrderPlaced)
export class OrderPlacedHandler {
  handle({ event, id, meta, publishedAt }: EventPayload<OrderPlaced>) {
    // ...
  }
}
```

A handler can listen to several events by passing an array; `handle` then receives a union of payloads:

```typescript
@EventHandler([OrderPlaced, OrderCancelled])
export class OrderAuditHandler {
  handle(payload: EventPayload<OrderPlaced> | EventPayload<OrderCancelled>) {}
}
```

`@EventHandler` makes the class injectable. Register it as a provider in any module of the application, either in the `providers` option above or in a module of its own, and it is subscribed at bootstrap. Declaring it in a module of its own lets the handler inject that module's dependencies:

```typescript
@Module({
  imports: [OrdersModule], // provides what OrderPlacedHandler injects
  providers: [OrderPlacedHandler],
})
export class BillingModule {}

@Module({
  imports: [
    BillingModule,
    BusModule.forRoot({
      transport: {
        provider: RabbitmqTransport,
        config: { url: 'amqp://localhost', group: 'billing' },
      },
    }),
  ],
})
export class AppModule {}
```

Provide a handler in exactly one module: if the same class is provided in more than one (for example in both `BusModule`'s `providers` and a parent module), the application fails to start. Handlers provided with `useFactory` are discovered too, and a `useExisting` alias of a handler is ignored since the aliased provider is discovered itself. To keep a handler out of an application, don't import the module that provides it.

Handlers can be request-scoped or transient (the `scope` option), or depend on request-scoped providers. Such a handler is resolved anew for every event, and the event payload (`EventPayload`) is available as `REQUEST` (see [Middleware](#middleware) for how it combines with scoped middlewares):

```typescript
@EventHandler(OrderPlaced, { scope: Scope.REQUEST })
export class OrderPlacedHandler {
  constructor(
    @Inject(REQUEST) private readonly payload: EventPayload<OrderPlaced>,
  ) {}
}
```

### Middleware

```typescript
import { Injectable } from '@nestjs/common';
import { type EventHandlerMiddleware, type EventPayload } from '@lsndr/bus';

@Injectable()
export class TimingMiddleware implements EventHandlerMiddleware {
  async use(
    event: EventPayload<object>,
    next: (event: EventPayload<object>) => Promise<void>,
  ) {
    const start = Date.now();
    await next(event);
    console.log(`handled in ${Date.now() - start}ms`);
  }
}
```

A middleware must be provided in a module of the application, like a handler, so it is up to you how its dependencies are resolved. Then reference the class globally with the `middlewares` option, or per handler:

```typescript
@EventHandler(OrderPlaced, { middlewares: [TimingMiddleware] })
export class OrderPlacedHandler {}
```

Global middlewares run before handler-level ones. Bootstrap fails if a referenced middleware is not provided anywhere or is provided more than once. Like handlers, middlewares can be provided with `useValue` or `useFactory`, and a `useExisting` alias is ignored.

A middleware can be request-scoped or transient (`@Injectable({ scope: Scope.REQUEST })`), or depend on request-scoped providers. When a handler or any of its middlewares is scoped, they are all resolved anew for every event, in the same context, so they share request-scoped dependencies, and the event payload as received is available as `REQUEST`. Default-scoped middlewares are single shared instances.

### Health checks

```typescript
import { Health } from '@lsndr/bus';

await this.health.assertHealthy(); // throws if the broker is unreachable
```

### Lifecycle hooks

The bus calls three optional hooks on **every provider of the bus module**: the transport (and the one an `Outbox` relays to), the telemetry, and the classes you give in the `providers` option. A provider implements the ones it needs:

```typescript
import {
  Transport,
  type OnBusInit,
  type OnBusBootstrap,
  type OnBusShutdown,
} from '@lsndr/bus';

class MyTransport
  extends Transport
  implements OnBusInit, OnBusBootstrap, OnBusShutdown
{
  // Called when the application bootstraps, before the handlers are subscribed
  async onBusInit() {}

  // Called once all the handlers are subscribed
  onBusBootstrap() {}

  // Called when the bus shuts down
  async onBusShutdown() {}
}
```

They are called one provider after another, in the order the providers are registered (the transport and the telemetry first, then yours), and the bus waits for each. `onBusShutdown` goes in reverse, and a provider that fails to shut down is logged without keeping the others from it. Providers with a request or transient scope have no single instance, so they are skipped, and so are providers of other modules.

The Redis Streams transport starts consuming in `onBusBootstrap`, so calling `subscribe` yourself, without the bus, needs `onBusBootstrap()` afterwards to start reading. Its shutdown logic runs in `onBusShutdown`.

## Telemetry

The bus reports spans, metrics and exceptions that follow the OpenTelemetry [semantic conventions for messaging](https://opentelemetry.io/docs/specs/semconv/messaging/). `BusOtel` does it with OpenTelemetry, which it loads on startup, so the packages are only needed when it is used:

```bash
npm install @opentelemetry/api @opentelemetry/api-logs
```

```typescript
import { BusModule, BusOtel } from '@lsndr/bus';

BusModule.forRoot({
  transport: {/* ... */},
  telemetry: new BusOtel(),
});
```

Nothing is reported without the `telemetry` option. `BusOtel` takes the global tracer, meter and logger providers and propagator, so set up the OpenTelemetry SDK as usual. Pass `tracerProvider`, `meterProvider`, `loggerProvider` or `propagator` to use others. If the types of those options are not wanted, enable `skipLibCheck`.

### What is reported

The following is reported for every transport, the built-in ones and custom ones.

| What                                                                    | Where                                                                                                                                                                                                                              |
| ----------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `send {destination}` span                                               | `Dispatcher.publish`. `PRODUCER` for a single message, `CLIENT` for a batch, where every message also gets a `create` span, and for messages that come from an outbox.                                                             |
| `create {destination}` span                                             | Each message a `Outbox` stores. Its context is stored with the message.                                                                                                                                                            |
| `process {destination}` span                                            | Each call of a handler (`CONSUMER`), including its middlewares.                                                                                                                                                                    |
| `messaging.client.operation.duration`, `messaging.client.sent.messages` | Publishing.                                                                                                                                                                                                                        |
| `messaging.process.duration`, `messaging.client.consumed.messages`      | Handling.                                                                                                                                                                                                                          |
| `messaging.send.exception`, `messaging.process.exception` log events    | A failed publication (`WARN`) or handler (`ERROR`). Needs `@opentelemetry/api-logs`, otherwise a warning is logged once on startup and they are skipped. The span of a failure has the `ERROR` status and `error.type` either way. |

A message carries the context of the span that created it in `EventPayload.context`. A `process` span is **not** a child of the publisher's span: as the conventions recommend by default, it is a child of whatever span is active when the handler runs (none, usually), and has a link to the creation context. Follow the link to get from a handler to the publication.

The RabbitMQ and Redis Streams transports add what is specific to them:

- RabbitMQ: `messaging.system` is `rabbitmq`, the destination is `{exchange}:{routing key}` (and `:{queue}` when consuming), with `messaging.rabbitmq.destination.routing_key` and `messaging.rabbitmq.message.delivery_tag`. Acks and nacks are `settle` spans.
- Redis Streams: `messaging.system` is `redis` (it is not a well-known value), the destination is the stream. Acks are `settle` spans. `bus.redis_streams.inflight` (gauge), `bus.redis_streams.reclaimed` and `bus.redis_streams.stale_consumers.removed` (counters).
- Both: `bus.retries` and `bus.dropped` count messages scheduled for another attempt and messages given up on, per destination and handler. `bus.delivery.attempt` is an attribute of `process` spans.
- `Outbox`: `bus.outbox.relay.duration`, `bus.outbox.relayed` and `bus.outbox.relay.failures`. The relay sends with `CLIENT` `send` spans that link to the `create` spans.

The `bus.` prefix of the instruments that aren't part of the conventions is the `namespace` option of `BusOtel`.

### Outbox

An outbox has to store `OutboxRecord.context` and give it back in `relay`, which `dispatch` passes on, so that the trace continues after the relay. Records stored without it work, they just start a new trace.

### Custom telemetry

Implement `BusTelemetry` to report to something else. `publish` and `process` wrap the operations, and `transport` gives the transports what they report their own details through. `publish` and `process` are optional: what a telemetry leaves out is not instrumented.

```typescript
import { type BusTelemetry, type OnBusInit } from '@lsndr/bus';

class StatsdTelemetry implements BusTelemetry, OnBusInit {
  // Optional: called, and awaited, when the application bootstraps, before
  // the transports are instrumented and the handlers subscribed
  async onBusInit() {}

  async publish(operation, fn) {
    // operation.type is 'send' or 'create'. fn receives the context to send
    // along with each message, in order. Return it unchanged if you have none.
    const start = performance.now();

    try {
      return await fn(operation.messages.map((m) => m.context ?? {}));
    } finally {
      statsd.timing('publish', performance.now() - start);
    }
  }

  process(operation, fn) {
    return fn();
  }

  transport(descriptor) {
    return {
      span: (options, fn) => fn(),
      counter: (name) => ({
        add: (value, attributes) => statsd.increment(name, value),
      }),
      histogram: (name) => ({ record: (value) => statsd.timing(name, value) }),
      gauge: () => undefined,
    };
  }
}
```

### Telemetry of a custom transport

Without any change a transport is reported as `messaging.system = <class name>` with no destination. Override `getTelemetryDescriptor` to describe it, and pass what you know about a delivery as the second argument of the callback:

```typescript
class NatsTransport extends Transport<NatsConfig> {
  public override getTelemetryDescriptor(): TelemetryDescriptor {
    return {
      system: 'nats',
      destination: (eventName) => `events.${eventName}`,
      consumerGroup: this.config.group,
      server: { address: 'nats.local', port: 4222 },
    };
  }

  private async deliver(message, callback) {
    await callback(payload, {
      attempt: message.redeliveryCount + 1,
      attributes: { 'messaging.nats.sequence': message.seq },
    });
  }

  private async onRetry() {
    // Names of instruments are relative, BusOtel adds its namespace
    this.telemetry.counter('retries').add(1);
  }
}
```

`this.telemetry` also has `span`, `histogram` and `gauge`, and does nothing when the bus has no telemetry. `getTelemetryDescriptor` is called after the transport is created, so `this.config` is available.

The telemetry is set up, and the transports are instrumented, when the application bootstraps (`onApplicationBootstrap` of the bus), just before the handlers are subscribed. Events published earlier, for example from an `onModuleInit` of your own, are published but not reported.

Publishing and handling are reported by the transport itself: when the bus has telemetry, it calls `instrument(telemetry)` on every transport it provides. That replaces `publish` and `subscribe` with versions that call `telemetry.publish` and `telemetry.process`, so calling them directly is reported too. A transport you create yourself, outside the bus module, is not.

Override `instrument` to report a transport differently. `Outbox` does: it only reports `publish`, as the creation of the events, and leaves `subscribe` to the transport it relays to. The protected `instrumentPublish(telemetry, type)` and `instrumentSubscribe(telemetry)` do the replacing.

## Outbox

`Outbox` is a transport that stores events instead of publishing them, so the event can be written in the same database transaction as your data. A background loop then relays the stored events to a real transport (RabbitMQ, Redis Streams, or your own `Transport`). Subscriptions and health checks go straight to that transport.

Extend `Outbox` with your store and pass the real transport in `config.transport`, using the same `{ provider, config }` shape as `transport`:

```typescript
import { Outbox, type OutboxConfig, type OutboxRecord } from '@lsndr/bus';

interface PostgresOutboxConfig extends OutboxConfig {
  batchSize: number;
}

@Injectable()
class PostgresOutbox extends Outbox<PostgresOutboxConfig> {
  // Runs inside the caller's transaction: join it here (e.g. via AsyncLocalStorage)
  protected async save(records: OutboxRecord[]) {
    // INSERT INTO outbox (...) VALUES (...), (...)
  }

  protected async relay() {
    // BEGIN
    // SELECT ... FROM outbox ORDER BY ... LIMIT this.config.batchSize FOR UPDATE SKIP LOCKED
    // await this.dispatch(rows)
    // DELETE FROM outbox WHERE id = ANY(...)
    // COMMIT
    // return rows.length
  }
}

BusModule.forRoot({
  transport: {
    provider: PostgresOutbox,
    config: {
      transport: {
        provider: RabbitmqTransport,
        config: { url: 'amqp://localhost', group: 'billing' },
      },
      interval: 1000, // optional, ms to wait when idle or after a failure
      batchSize: 100, // your own option
    },
  },
});
```

- `publish` calls `save` once per call with a list of `OutboxRecord`s (id, event name, plain event data, meta, `publishedAt`), so a batch is stored together.
- `relay` is called in a loop. It locks pending records, calls `this.dispatch(records)`, and marks them dispatched only if that resolves. It returns the number of records dispatched: if above zero it runs again right away, otherwise it waits `interval`. Batching and limits are up to your implementation.
- `dispatch` publishes the records as one batch, in order, to the real transport with their original `id`, `name`, `meta` and `publishedAt`. It rejects on the first failure, so roll back and retry. Delivery is at-least-once.
- Locking with `SKIP LOCKED` lets several instances relay together, at the cost of strict ordering between batches.
- If you override `onApplicationBootstrap` or `onModuleDestroy`, call `super`.

## Configuration

`BusModule.forRoot()` options:

- `transport` — required. `{ provider, config }`, see the options below.
- `middlewares` — global middleware classes. They must be provided in a module of the application.
- `telemetry` — see [Telemetry](#telemetry). Nothing is reported by default.
- `imports`, `providers` — additional Nest imports and providers for the module.

Every transport takes these in its `config`:

- `group` — required. Consumer group name. RabbitMQ uses it as a prefix for queue names, Redis Streams as the name of the consumer group of every stream.
- `retries.max` — maximum retries before a message is dropped. Defaults to `5`.
- `retries.delay` — delay between retries in milliseconds. Defaults to `5000` for RabbitMQ and `300000` for Redis Streams, where it is also how long after a consumer crashed its entries are recovered.

RabbitMQ adds `url` (required). Redis Streams has more options, see [Redis Streams topology](#redis-streams-topology).

## RabbitMQ topology

- Events are published to the durable `events` direct exchange with the event class name as routing key. Messages are persistent and use publisher confirms.
- Each handler/event pair gets a queue named `<group>.<HandlerClass>.<EventClass>`.
- Failed messages are nacked into `<queue>.delay` (via the `events.delay` exchange), wait `retries.delay` ms, and are redelivered to the original queue.
- After `retries.max` failed retries, the message is logged as permanently failed and acknowledged (dropped).

## Redis Streams topology

```typescript
BusModule.forRoot({
  transport: {
    provider: RedisStreamsTransport,
    config: { url: 'redis://localhost:6379', group: 'billing' },
  },
});
```

Config options: `url` and `group` (required), `retries`, `consumer` (`<hostname>-<pid>`), `keyPrefix` (`events:`), `trim.maxLength` (`100`), `trim.onlyAcked` (`true`), `concurrency` (`10`), `reclaimInterval` (`max(retries.delay / 4, 1000)` ms), `shutdownTimeout` (not set: wait for all entries in flight).

- Each event class has its own stream, `<keyPrefix><EventClass>`.
- Each stream has one consumer group, named after `group` and created at the stream's tail (`$`). Instances of the group share it, so an entry goes to a single instance, which runs all of its handlers for the event in parallel. Every instance of a group must therefore register the same handlers.
- An instance reads all its streams with one blocking `XREADGROUP` (`COUNT = concurrency`). Each stream has `concurrency` slots, and a stream is part of the next read only while it has a free slot. Entries run as soon as they are read and free their slot on their own, so a stream runs at most `2 * concurrency - 1` entries at once; there is no global limit. When no stream has a free slot, Redis is not called until one frees.
- Consuming starts when `subscribe` is called, which the discovery does once after all handlers are found.
- Successful handlers of an entry are recorded in the hash `<keyPrefix>done:<group>:<EventClass>:<id>` (with a TTL), so a redelivery only runs the handlers that haven't succeeded. The entry is acknowledged, and the hash deleted, once every handler has succeeded or failed permanently.
- Shutdown stops reading and reclaiming, waits for the entries in flight (up to `shutdownTimeout`), then removes the consumer from the groups unless it still has pending entries.
- The consumer name defaults to the host name and the process id, so instances on the same host don't share pending entries. A stable name avoids leaving stale consumers behind after restarts; it doesn't change recovery, since pending entries are reclaimed by any consumer after `retries.delay`. A consumer is removed from its groups on graceful shutdown unless it still has pending entries.
- Consumers that crashed or were renamed stay registered in the group. Running consumers delete the ones that have no pending entries and have been idle for longer than an hour.
- Delivery is at-least-once. Handled entries are acknowledged with `XACK`. Failed entries stay pending and are claimed again (by any instance) after `retries.delay` ms, up to `retries.max` retries, then logged as permanently failed and acknowledged. Entries of crashed consumers are recovered the same way. While handlers are running their entry is kept from being reclaimed. Reclaimed entries take a slot of their stream while they run, but are not held back by the slots.
- Publishing uses `XADD … MAXLEN ~ <trim.maxLength> ACKED`: entries are trimmed only once every consumer group has acknowledged them, so `trim.maxLength` is a soft cap that unacknowledged entries can exceed. With `trim.onlyAcked: false` the policy is `KEEPREF` and the cap is hard: unhandled entries are trimmed too, and lost. Trimming is approximate, so it removes whole stream nodes (100 entries each by default).
