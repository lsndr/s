# @lsndr/bus

Event bus for [NestJS](https://nestjs.com/) with decorator-based event handlers, middleware, delayed retries, and RabbitMQ and Redis Streams transports.

## Installation

The package is published to GitHub Packages and requires Node.js >= 24.

```bash
npm install @lsndr/bus
```

Peer dependencies: `@nestjs/common`, `@nestjs/core`, `reflect-metadata`. To use the built-in RabbitMQ transport, also install the optional peers:

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

### Transport lifecycle

A custom `Transport` can implement two optional hooks, which the bus calls when they are present:

```typescript
import { Transport, type OnBusBootstrap, type OnBusShutdown } from '@lsndr/bus';

class MyTransport extends Transport implements OnBusBootstrap, OnBusShutdown {
  // Called once all the handlers are subscribed
  onBusBootstrap() {}

  // Called when the bus shuts down
  async onBusShutdown() {}
}
```

The Redis Streams transport starts consuming in `onBusBootstrap`, so calling `subscribe` yourself, without the bus, needs `onBusBootstrap()` afterwards to start reading. Its shutdown logic runs in `onBusShutdown`. `Outbox` forwards both hooks to its inner transport.

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
