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
      group: 'billing',
      transport: {
        provider: RabbitmqTransport,
        config: { url: 'amqp://localhost' },
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
      meta: { correlationId: 'abc-123' }, // optional
    });
  }
}
```

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
      group: 'billing',
      transport: {
        provider: RabbitmqTransport,
        config: { url: 'amqp://localhost' },
      },
    }),
  ],
})
export class AppModule {}
```

Provide a handler in exactly one module: if the same class is provided in more than one (for example in both `BusModule`'s `providers` and a parent module), the application fails to start. To keep a handler out of an application, don't import the module that provides it.

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

Global middlewares run before handler-level ones. A middleware is a single shared instance. Bootstrap fails if a referenced middleware is not provided anywhere, is provided more than once, or doesn't have the default scope.

### Health checks

```typescript
import { Health } from '@lsndr/bus';

await this.health.assertHealthy(); // throws if the broker is unreachable
```

## Configuration

`BusModule.forRoot()` options:

- `group` — required. Consumer group name, used as a prefix for queue names.
- `transport` — required. `{ provider, config }`; for RabbitMQ, `config` is `{ url }`, for Redis Streams see [Redis Streams topology](#redis-streams-topology).
- `middlewares` — global middleware classes. They must be provided in a module of the application.
- `imports`, `providers` — additional Nest imports and providers for the module.
- `retries.max` — maximum retries before a message is dropped. Defaults to `5`.
- `retries.delay` — delay between retries in milliseconds. Defaults to `5000`.

## RabbitMQ topology

- Events are published to the durable `events` direct exchange with the event class name as routing key. Messages are persistent and use publisher confirms.
- Each handler/event pair gets a queue named `<group>.<HandlerClass>.<EventClass>`.
- Failed messages are nacked into `<queue>.delay` (via the `events.delay` exchange), wait `retries.delay` ms, and are redelivered to the original queue.
- After `retries.max` failed retries, the message is logged as permanently failed and acknowledged (dropped).

## Redis Streams topology

```typescript
BusModule.forRoot({
  group: 'billing',
  transport: {
    provider: RedisStreamsTransport,
    config: { url: 'redis://localhost:6379' },
  },
});
```

The transport checks the server version on startup and fails if Redis is older than 8.2.

Config options: `url` (required), `consumer`, `streamPrefix` (`events:`), `maxLength` (`1000`), `batchSize` (`10`), `blockTimeout` (`5000` ms), `reclaimInterval` (`max(retries.delay / 4, 1000)` ms), `consumerTtl` (`3600000` ms).

- Each event class has its own stream, `<streamPrefix><EventClass>`.
- Each handler gets its own consumer group per stream, named `<group>.<HandlerClass>`, created at the stream's tail (`$`). Instances of the same handler share the group, so entries are load-balanced between them.
- The consumer name defaults to the host name. A stable name avoids leaving stale consumers behind after restarts; it doesn't change recovery, since pending entries are reclaimed by any consumer after `retries.delay`. Set `consumer` explicitly if several instances share a host, since instances with the same name share their pending entries. A consumer is removed from its groups on graceful shutdown unless it still has pending entries.
- Consumers that crashed or were renamed stay registered in the group. Running consumers delete the ones that have no pending entries and have been idle for longer than `consumerTtl`; keep it longer than `blockTimeout`.
- Delivery is at-least-once. Handled entries are acknowledged with `XACK`. Failed entries stay pending and are claimed again (by any instance) after `retries.delay` ms, up to `retries.max` retries, then logged as permanently failed and acknowledged. Entries of crashed consumers are recovered the same way. While a handler is running its entry is kept from being reclaimed.
- Publishing uses `XADD … MAXLEN ~ <maxLength> ACKED`: entries are trimmed only once every consumer group has acknowledged them, so `maxLength` is a soft cap that unacknowledged entries can exceed. Trimming is approximate, so it removes whole stream nodes (100 entries each by default).
