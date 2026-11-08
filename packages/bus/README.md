# @lsndr/bus

Event bus for [NestJS](https://nestjs.com/) with decorator-based event handlers, middleware, delayed retries, and RabbitMQ and Redis Streams transports.

## Installation

The package is published to GitHub Packages and requires Node.js >= 24.

```bash
npm install @lsndr/bus
```

Peer dependencies: `@nestjs/common`, `@nestjs/core`, `reflect-metadata`. To use the built-in RabbitMQ transport, also install the optional peers (`class-transformer` is used by the default [serializer](#serialization), and not needed with your own):

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

`BusModule` is global and exports `Dispatcher` and `Health`.

### Providing the transport

`transport` takes a Nest provider without its token, so it is resolved like any other provider:

```typescript
// A transport with its config: the built-in ones have `withConfig`
BusModule.forRoot({
  transport: RedisStreamsTransport.withConfig({ url, group }),
});

// A class, which injects its own dependencies (same as `{ useClass }`)
BusModule.forRoot({ transport: MyTransport, providers: [MyTransportDeps] });

// A factory, a value, or a transport already provided in another module
BusModule.forRoot({
  transport: {
    useFactory: (cfg: Config) => new MyTransport(cfg),
    inject: [Config],
  },
});
BusModule.forRoot({ transport: { useExisting: MyTransport } });
```

A provider in the object form can also carry `providers`, extra providers the transport needs, which are registered in the bus module next to it. For an outbox, the transport it relays to is registered the same way, so it gets the hooks too, ahead of the outbox on startup and after it on shutdown.

The transport is registered under the `Transport` token, and `Dispatcher`, `Subscriber` and `Health` are aliases of it.

### Defining events

Events are plain classes. The class name is used as the routing key, and payloads are serialized with `class-transformer` unless you provide another [serializer](#serialization).

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
      transport: RabbitmqTransport.withConfig({
        url: 'amqp://localhost',
        group: 'billing',
      }),
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

Global middlewares run before handler-level ones. A middleware is looked up by its class as an injection token, so it is provided under its own class: as a class, or with `provide: TimingMiddleware` and `useValue`, `useFactory` or `useExisting`. Bootstrap fails if a referenced middleware is not provided in any module.

A middleware can be request-scoped or transient (`@Injectable({ scope: Scope.REQUEST })`), or depend on request-scoped providers. When a handler or any of its middlewares is scoped, they are all resolved anew for every event, in the same context, so they share request-scoped dependencies, and the event payload as received is available as `REQUEST`. Default-scoped middlewares are single shared instances.

### Serialization

A `Serializer` turns an event and its envelope (id, name, meta, `publishedAt`) into the bytes a transport sends, and back. `serialize` receives the event and the publish options, fills in what they leave out (a UUID v7 `id`, the class name as `name`, empty `meta`, the current time) and returns the `id`, `name`, `publishedAt` and `headers` it used along with the `data`. `headers` are message headers that travel next to `data`, not inside it (telemetry puts the trace context there), and come from `options.headers`, empty by default. The default, `JsonSerializer`, writes JSON text and converts the event with `class-transformer`. To use another format, provide your own with the `serializer` option, in any of the forms accepted by `transport`:

```typescript
import { Injectable } from '@nestjs/common';
import { pack, unpack } from 'msgpackr';
import { randomUUID } from 'node:crypto';
import {
  type PublishOptions,
  type Serializer,
  type SerializedData,
  type SerializedMessage,
  type EventPayload,
} from '@lsndr/bus';

@Injectable()
class MsgpackSerializer implements Serializer {
  serialize(event: object, options?: PublishOptions): SerializedMessage {
    const id = options?.id ?? randomUUID();
    const name = options?.name ?? event.constructor.name;
    const publishedAt = options?.publishedAt ?? new Date();

    return {
      id,
      name,
      publishedAt,
      headers: { ...options?.headers },
      data: pack({ id, name, event, meta: options?.meta ?? {}, publishedAt }),
    };
  }

  deserialize<E extends object>(
    event: new (...args: any) => E,
    data: SerializedData,
  ): EventPayload<E> {
    const message = unpack(Buffer.from(data));

    return {
      ...message,
      event: Object.assign(Object.create(event.prototype), message.event),
    };
  }
}

BusModule.forRoot({ transport, serializer: MsgpackSerializer });
```

The serializer is initialized before the transport and shut down after it, and receives the [lifecycle hooks](#lifecycle-hooks) like any other provider of the bus module. Changing the serializer of a running system makes messages that are already in flight unreadable, so drain the queues first.

### Health checks

```typescript
import { Health } from '@lsndr/bus';

await this.health.assertHealthy(); // throws if the broker is unreachable
```

### Lifecycle hooks

The bus calls three optional hooks on **every provider of the bus module**: the transport (whichever module provides it), and the classes you give in the `providers` option. A provider implements the ones it needs:

```typescript
import {
  type Transport,
  type OnBusInit,
  type OnBusBootstrap,
  type OnBusShutdown,
} from '@lsndr/bus';

class MyTransport
  implements Transport, OnBusInit, OnBusBootstrap, OnBusShutdown
{
  // Called when the application bootstraps, before the handlers are subscribed
  async onBusInit() {}

  // Called once all the handlers are subscribed
  onBusBootstrap() {}

  // Called when the bus shuts down
  async onBusShutdown() {}
}
```

They are called one provider after another, in the order the providers are registered (the transport and its own `providers` first, then yours), and the bus waits for each. `onBusShutdown` goes in reverse, and a provider that fails to shut down is logged without keeping the others from it. Providers with a request or transient scope have no single instance, so they are skipped. Providers of other modules are skipped too, unless the bus module aliases them (like a transport given as `{ useExisting }`). An instance is called once, however many aliases point to it.

The built-in transports are plain classes created with `new RedisStreamsTransport(config, serializer, telemetry)`, and do their work in these hooks: they connect in `onBusInit`, the Redis Streams transport starts consuming in `onBusBootstrap`, and both clean up in `onBusShutdown`. Using one without the bus needs these calls yourself, e.g. `onBusInit()` before and `onBusBootstrap()` after `subscribe`.

## Outbox

`Outbox` is a transport that stores events instead of publishing them, so the event can be written in the same database transaction as your data. A background loop then relays the stored events to a real transport (RabbitMQ, Redis Streams, or your own `Transport`). Subscriptions and health checks go straight to that transport.

Extend `Outbox` with your store and provide it with `withConfig`. The real transport goes in `config.transport`, as any transport provider accepted by `transport` (`withConfig`, a class, `useFactory`, `useExisting`, …). The outbox is created by Nest, so it can inject its own dependencies, and its config is available as `this.config`:

```typescript
import { Outbox, type OutboxConfig, type OutboxRecord } from '@lsndr/bus';

interface PostgresOutboxConfig extends OutboxConfig {
  batchSize: number;
}

@Injectable()
class PostgresOutbox extends Outbox<PostgresOutboxConfig> {
  constructor(@Inject(Database) private readonly db: Database) {
    super();
  }

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
  transport: PostgresOutbox.withConfig({
    transport: RabbitmqTransport.withConfig({
      url: 'amqp://localhost',
      group: 'billing',
    }),
    interval: 1000, // optional, ms to wait when idle or after a failure
    batchSize: 100, // your own option
  }),
});
```

- `publish` calls `save` once per call with a list of `OutboxRecord`s (`id`, `name`, `publishedAt`, `headers` and `data`, the message as the [serializer](#serialization) produced it, a `string` or a `Buffer`, to store as it is, e.g. in a `text` column with the default serializer), so a batch is stored together. Store `headers` (a string map) too and return it as it was: it carries the trace context, and `dispatch` passes it on.
- `relay` is called in a loop. It locks pending records, calls `this.dispatch(records)`, and marks them dispatched only if that resolves. It returns the number of records dispatched: if above zero it runs again right away, otherwise it waits `interval`. Batching and limits are up to your implementation.
- `dispatch` publishes the records as one batch, in order, to the real transport with their original `id`, `name`, `meta` and `publishedAt`. It deserializes each record without an event class, since the outbox only knows the name, so the transport receives a plain object and serializes it again. A serializer used with an outbox therefore has to accept `Object` as the event class. It rejects on the first failure, so roll back and retry. Delivery is at-least-once.
- Locking with `SKIP LOCKED` lets several instances relay together, at the cost of strict ordering between batches.
- If you override `onBusBootstrap` or `onBusShutdown`, call `super`: they run the relay loop.

## Telemetry

Tracing is pluggable through the `telemetry` option. The default does nothing. `BusOtel` traces with [OpenTelemetry](https://opentelemetry.io) and follows the [messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/). It needs `@opentelemetry/api`, and spans are exported by whatever SDK you have set up.

```typescript
import { BusModule, BusOtel } from '@lsndr/bus';

BusModule.forRoot({
  transport: RabbitmqTransport.withConfig({
    url: 'amqp://localhost',
    group: 'billing',
  }),
  telemetry: BusOtel.withConfig(), // optional: { tracerProvider, propagator }
});
```

- Publishing creates a `send <destination>` producer span and injects the trace context (`traceparent`, `tracestate`) into the message headers: AMQP headers on RabbitMQ, a `headers` field of the entry on Redis Streams.
- Every handler that processes a message gets a `process <destination>` consumer span, a child of the sending span, and runs inside it, so your own spans nest under it. A failing handler marks its span as an error.
- With an [outbox](#outbox), storing an event creates a `create <event>` span (with `messaging.system.name` set to `outbox` and no destination) and its context is kept with the record. The `send` span made when the record is relayed is its child, so the relay and the consumers stay in the trace of the request.

Spans carry the attributes the conventions require or recommend, including the broker as `server.address` and `server.port`. On RabbitMQ the destination is `<exchange>:<routing key>` when sending and `<exchange>:<routing key>:<queue>` when processing.

Provide your own `Telemetry` (`create`, `send` and `process`, each wrapping a function) to plug in something else.

## Configuration

`BusModule.forRoot()` options:

- `transport` — required. A transport provider, see [Providing the transport](#providing-the-transport). The options below go in the config of `withConfig`.
- `serializer` — a provider of the [serializer](#serialization). Defaults to `JsonSerializer`.
- `middlewares` — global middleware classes. They must be provided in a module of the application.
- `imports`, `providers` — additional Nest imports and providers for the module.

Every built-in transport takes these in its config:

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
  transport: RedisStreamsTransport.withConfig({
    url: 'redis://localhost:6379',
    group: 'billing',
  }),
});
```

Config options: `url` and `group` (required), `retries`, `consumer` (`<hostname>-<pid>`), `keyPrefix` (`events:`), `trim.maxLength` (`100`), `trim.onlyAcked` (`true`), `concurrency` (`10`), `reclaimInterval` (`max(retries.delay / 4, 1000)` ms), `shutdownTimeout` (not set: wait for all entries in flight).

- Each event class has its own stream, `<keyPrefix><EventClass>`.
- Each stream has one consumer group, named after `group` and created at the stream's tail (`$`). Instances of the group share it, so an entry goes to a single instance, which runs all of its handlers for the event in parallel. Every instance of a group must therefore register the same handlers.
- An instance reads all its streams with one blocking `XREADGROUP` (`COUNT = concurrency`). Each stream has `concurrency` slots, and a stream is part of the next read only while it has a free slot. Entries run as soon as they are read and free their slot on their own, so a stream runs at most `2 * concurrency - 1` entries at once; there is no global limit. When no stream has a free slot, Redis is not called until one frees.
- Consuming starts in `onBusBootstrap`, once the discovery has subscribed all handlers.
- Successful handlers of an entry are recorded in the hash `<keyPrefix>done:<group>:<EventClass>:<id>` (with a TTL), so a redelivery only runs the handlers that haven't succeeded. The entry is acknowledged, and the hash deleted, once every handler has succeeded or failed permanently.
- Shutdown stops reading and reclaiming, waits for the entries in flight (up to `shutdownTimeout`), then removes the consumer from the groups unless it still has pending entries.
- The consumer name defaults to the host name and the process id, so instances on the same host don't share pending entries. A stable name avoids leaving stale consumers behind after restarts; it doesn't change recovery, since pending entries are reclaimed by any consumer after `retries.delay`. A consumer is removed from its groups on graceful shutdown unless it still has pending entries.
- Consumers that crashed or were renamed stay registered in the group. Running consumers delete the ones that have no pending entries and have been idle for longer than an hour.
- Delivery is at-least-once. Handled entries are acknowledged with `XACK`. Failed entries stay pending and are claimed again (by any instance) after `retries.delay` ms, up to `retries.max` retries, then logged as permanently failed and acknowledged. Entries of crashed consumers are recovered the same way. While handlers are running their entry is kept from being reclaimed. Reclaimed entries take a slot of their stream while they run, but are not held back by the slots.
- Publishing uses `XADD … MAXLEN ~ <trim.maxLength> ACKED`: entries are trimmed only once every consumer group has acknowledged them, so `trim.maxLength` is a soft cap that unacknowledged entries can exceed. With `trim.onlyAcked: false` the policy is `KEEPREF` and the cap is hard: unhandled entries are trimmed too, and lost. Trimming is approximate, so it removes whole stream nodes (100 entries each by default).
