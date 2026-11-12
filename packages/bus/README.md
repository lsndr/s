# @lsndr/bus

Event bus for [NestJS](https://nestjs.com/): transport agnostic, with outbox support and high observability.

## Features

- Pluggable transports
- Middlewares
- Transactional outbox
- Pluggable serialization
- Logs, tracing and metrics, and ready-made Grafana dashboards

## Supported transports

- [Redis Streams](docs/transports.md#redis-streams) (`RedisStreamsTransport`)
- [RabbitMQ](docs/transports.md#rabbitmq) (`RabbitmqTransport`)
- Any [custom transport](docs/transports.md#providing-a-transport) implementing the `Transport` interface

More transports are coming.

## Requirements

- Node.js >= 24
- NestJS 12

## Installation

The package is published to GitHub Packages. Point the `@lsndr` scope at that registry in `.npmrc` (authenticating with a token that can read packages), then install:

```ini
@lsndr:registry=https://npm.pkg.github.com
```

```bash
npm install @lsndr/bus class-transformer
```

`class-transformer` is used by the default `JsonSerializer`; you can skip it if you provide your own [serializer](docs/serialization.md).

Install the optional peer dependencies for what you use:

| You use                              | Install              |
| ------------------------------------ | -------------------- |
| `RedisStreamsTransport` (Redis 8.2+) | `redis`              |
| `RabbitmqTransport`                  | `amqplib`            |
| `BusOtel`                            | `@opentelemetry/api` |

## Quick start

Define an event. It is a plain class, and its name is the routing key.

```typescript
export class OrderPlaced {
  constructor(public readonly orderId: string) {}
}
```

Register the module with a transport:

```typescript
import { Module } from '@nestjs/common';
import { BusModule, RabbitmqTransport } from '@lsndr/bus';

@Module({
  imports: [
    BusModule.forRoot({
      transport: RabbitmqTransport.withConfig({
        url: 'amqp://localhost',
        group: 'billing',
      }),
      providers: [OrderPlacedHandler],
    }),
  ],
})
export class AppModule {}
```

Publish an event:

```typescript
import { Inject, Injectable } from '@nestjs/common';
import { Dispatcher } from '@lsndr/bus';

@Injectable()
export class OrderService {
  constructor(@Inject(Dispatcher) private readonly dispatcher: Dispatcher) {}

  async place(orderId: string) {
    await this.dispatcher.publish(new OrderPlaced(orderId));
  }
}
```

Handle it:

```typescript
import { EventHandler, type EventPayload } from '@lsndr/bus';

@EventHandler(OrderPlaced)
export class OrderPlacedHandler {
  handle({ event, id, meta, publishedAt }: EventPayload<OrderPlaced>) {
    // ...
  }
}
```

## Guides

### Handling events

Pass an array to listen to several events. `handle` then receives a union of payloads:

```typescript
@EventHandler([OrderPlaced, OrderCancelled])
export class OrderAuditHandler {
  handle(payload: EventPayload<OrderPlaced> | EventPayload<OrderCancelled>) {}
}
```

`@EventHandler` makes the class injectable. Register it as a provider in **any module** of your application, not only `BusModule`. The bus discovers handlers wherever they are provided, and a handler is resolved against the module that provides it, so it can inject that module's dependencies:

```typescript
@Module({
  imports: [OrdersModule], // provides what OrderPlacedHandler injects
  providers: [OrderPlacedHandler],
})
export class BillingModule {}
```

Handlers can be request-scoped or transient (the `scope` option), or depend on request-scoped providers. Such a handler is resolved anew for every event, and the payload is available as `REQUEST`:

```typescript
import { Inject, Scope } from '@nestjs/common';
import { REQUEST } from '@nestjs/core';

@EventHandler(OrderPlaced, { scope: Scope.REQUEST })
export class OrderPlacedHandler {
  constructor(
    @Inject(REQUEST) private readonly payload: EventPayload<OrderPlaced>,
  ) {}
}
```

### Middleware

A middleware wraps the handler:

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

Provide it in a module of the application, under its own class as the token, then apply it to every handler or to one.

For every handler, list it in the `middlewares` option of `forRoot`:

```typescript
@Module({
  imports: [
    BusModule.forRoot({
      transport,
      middlewares: [TimingMiddleware],
      providers: [TimingMiddleware],
    }),
  ],
})
export class AppModule {}
```

For one handler, list it in the `middlewares` option of `@EventHandler`:

```typescript
@EventHandler(OrderPlaced, { middlewares: [TimingMiddleware] })
export class OrderPlacedHandler {}
```

Global middlewares run before handler-level ones. Bootstrap fails if a referenced middleware isn't provided. Middlewares can be request-scoped too; when a handler or any of its middlewares is scoped, they are all resolved anew for every event and share the same context.

### Health checks

```typescript
import { Inject, Injectable } from '@nestjs/common';
import { Health } from '@lsndr/bus';

@Injectable()
export class HealthService {
  constructor(@Inject(Health) private readonly health: Health) {}

  check() {
    return this.health.assertHealthy(); // rejects if the broker is unreachable
  }
}
```

## Configuration

### `BusModule.forRoot()`

| Option        | Description                                                                                                        |
| ------------- | ------------------------------------------------------------------------------------------------------------------ |
| `transport`   | **Required.** A [transport provider](docs/transports.md#providing-a-transport).                                    |
| `serializer`  | A provider of the [serializer](docs/serialization.md). Defaults to `JsonSerializer`.                               |
| `telemetry`   | A provider of [telemetry](docs/telemetry.md), such as `BusOtel.withConfig()`.                                      |
| `middlewares` | Global middleware classes. They must be provided in a module of the application.                                   |
| `imports`     | Additional Nest imports for the module.                                                                            |
| `providers`   | Additional providers for the module, such as your handlers. They receive the [lifecycle hooks](docs/lifecycle.md). |

`BusModule` is global and exports `Dispatcher` and `Health`.

## Further documentation

- [Transports](docs/transports.md): providing a transport, Redis Streams and RabbitMQ internals
- [Outbox](docs/outbox.md): publishing in the same transaction as your data
- [Serialization](docs/serialization.md): custom message formats
- [Lifecycle hooks](docs/lifecycle.md): `onBusInit`, `onBusBootstrap`, `onBusShutdown`
- [Telemetry](docs/telemetry.md): traces, metrics, Grafana dashboards, custom backends
