# Outbox

`Outbox` is a transport that stores events instead of publishing them, so an event can be written in the same database transaction as your data. A background loop then relays the stored events to a real transport. Subscriptions and health checks go straight to that transport.

## What it solves

The outbox solves the double write problem. Saving your data and publishing an event are two writes to two systems, and they can't share a transaction. If the process crashes between them, you get a change without an event, or an event without a change.

The outbox saves the event in your database, in the same transaction as the change. A relay loop publishes it afterwards, so a failure only delays the event. Delivery is at-least-once.

## Usage

Extend `Outbox` with your store and provide it with `withConfig`. The real transport goes in `config.transport`, in the same form as the `transport` option of `forRoot`. The outbox is created by Nest, so it can inject its own dependencies, and its config is available as `this.config`.

```typescript
import { Inject, Injectable } from '@nestjs/common';
import {
  BusModule,
  Outbox,
  RabbitmqTransport,
  type OutboxConfig,
  type OutboxRecord,
} from '@lsndrs/bus';

interface PostgresOutboxConfig extends OutboxConfig {
  batchSize: number;
}

@Injectable()
class PostgresOutbox extends Outbox<PostgresOutboxConfig> {
  constructor(@Inject(Database) private readonly db: Database) {
    super();
  }

  // runs inside the caller's transaction: join it here (e.g. via AsyncLocalStorage)
  protected async save(record: OutboxRecord) {
    // INSERT INTO outbox (...) VALUES (...)
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

## How it works

- **`save`** is called once per `publish` with an `OutboxRecord`. Join the caller's transaction here to store the event together with your data. Store the record unchanged, and return it as it was in `relay`.
- **`relay`** runs in a loop. It reads pending records, calls `this.dispatch(records)`, and marks them dispatched only if that resolves. It returns the number of records dispatched: above zero it runs again right away, otherwise it waits `interval`. Batching and limits are up to you.
- **`dispatch`** publishes the records concurrently to the real transport, as they were originally published. It rejects if any publish fails (others may already have gone out), so roll back and retry.

## Things to know

- Delivery is at-least-once.
- Records are not relayed in order, since `dispatch` publishes them concurrently. If your store lets several instances relay at once, the order between batches is lost too.
- `dispatch` deserializes each record without an event class, since the outbox only knows the name, and the transport serializes it again. A serializer used with an outbox must therefore accept `Object` as the event class.
- The transport the outbox relays to is registered in the bus module too, so it receives the [lifecycle hooks](lifecycle.md): ahead of the outbox on startup, after it on shutdown.
