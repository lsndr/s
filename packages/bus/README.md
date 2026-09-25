# @lsndr/bus

Event bus for [NestJS](https://nestjs.com/) with decorator-based event handlers, middleware, delayed retries, and a RabbitMQ transport.

## Installation

The package is published to GitHub Packages and requires Node.js >= 24.

```bash
npm install @lsndr/bus
```

Peer dependencies: `@nestjs/common`, `@nestjs/core`, `reflect-metadata`. To use the built-in RabbitMQ transport, also install the optional peers:

```bash
npm install amqplib class-transformer
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
      handlers: [OrderPlacedHandler],
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

`@EventHandler` makes the class injectable, but it must also be registered as a provider **and** listed in the `handlers` option. Handlers missing from `handlers` are skipped with a warning at bootstrap.

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

Register globally with the `middlewares` option, or per handler:

```typescript
@EventHandler(OrderPlaced, { middlewares: [TimingMiddleware] })
export class OrderPlacedHandler {}
```

Global middlewares run before handler-level ones.

### Health checks

```typescript
import { Health } from '@lsndr/bus';

await this.health.assertHealthy(); // throws if the broker is unreachable
```

## Configuration

`BusModule.forRoot()` options:

- `group` — required. Consumer group name, used as a prefix for queue names.
- `transport` — required. `{ provider, config }`; for RabbitMQ, `config` is `{ url }`.
- `handlers` — event handler classes to subscribe.
- `middlewares` — global middleware classes.
- `imports`, `providers` — additional Nest imports and providers for the module.
- `retries.max` — maximum retries before a message is dropped. Defaults to `5`.
- `retries.delay` — delay between retries in milliseconds. Defaults to `5000`.

## RabbitMQ topology

- Events are published to the durable `events` direct exchange with the event class name as routing key. Messages are persistent and use publisher confirms.
- Each handler/event pair gets a queue named `<group>.<HandlerClass>.<EventClass>`.
- Failed messages are nacked into `<queue>.delay` (via the `events.delay` exchange), wait `retries.delay` ms, and are redelivered to the original queue.
- After `retries.max` failed retries, the message is logged as permanently failed and acknowledged (dropped).
