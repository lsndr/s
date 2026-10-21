import { Logger } from '@nestjs/common';
import { type Serializer } from '../../serialization/serializer.js';
import { type Clients } from './clients.js';
import { type Stream } from './streams.js';

export interface StreamEntry {
  id: string;
  message: Record<string, Buffer>;
}

export interface ProcessorOptions {
  group: string;
  keyPrefix: string;
  /** Maximum retries before an entry is dropped. */
  maxRetries: number;
  /** How long an entry has to be pending before it is claimed again. */
  retriesDelay: number;
  reclaimInterval: number;
}

/** Runs the handlers of stream entries. */
export class Processor {
  private readonly logger = new Logger(Processor.name);

  public constructor(
    private readonly clients: Clients<'command'>,
    private readonly serializer: Serializer,
    private readonly options: ProcessorOptions,
  ) {}

  /**
   * Runs the handlers of an entry that haven't succeeded yet. The entry is
   * acknowledged once every handler has succeeded or failed permanently,
   * otherwise it stays pending and is retried by the reclaim loop.
   *
   * Never rejects.
   */
  public async process(
    stream: Stream,
    entry: StreamEntry,
    attempt: number,
  ): Promise<void> {
    const { clients } = this;
    const { group, keyPrefix, maxRetries, retriesDelay, reclaimInterval } =
      this.options;
    const { event, handlers } = stream;
    const { id, message } = entry;
    const retryCount = attempt - 1;
    const eventName = event.name;
    const doneKey = `${keyPrefix}done:${group}:${eventName}:${id}`;
    // Long enough to outlive every retry of an entry
    const doneTtl = Math.max(
      2 * (maxRetries + 1) * (retriesDelay + reclaimInterval),
      3_600_000,
    );

    this.logger.log(`Received message for event ${eventName} (${id})`);

    try {
      const client = await clients.get('command');
      const done = await client.hGetAll(doneKey);
      const pending = handlers.filter(({ name }) => !(name in done));
      const rawPayload = message['payload'] ?? Buffer.alloc(0);
      const eventPayload = this.serializer.deserialize(event, rawPayload);

      this.logger.verbose(`Event ${eventName} (${id}) payload:`, eventPayload);

      const results = await Promise.all(
        pending.map(async ({ name, callback }) => {
          try {
            await callback(eventPayload);
          } catch (err) {
            return { name, error: err };
          }

          await client
            .multi()
            .hSet(doneKey, name, '1')
            .pExpire(doneKey, doneTtl)
            .exec()
            .catch((err) =>
              this.logger.warn(
                `Failed to record handler ${name} of ${id}: ${String(err)}`,
                err instanceof Error ? err.stack : undefined,
              ),
            );

          return { name, error: undefined };
        }),
      );
      const failed = results.filter(({ error }) => error !== undefined);

      for (const { name, error } of failed) {
        this.logger.error(
          `Error handling event ${eventName} (${id}) (handler: ${name}, retryCount: ${retryCount}): ${String(error)}`,
          error instanceof Error ? error.stack : undefined,
        );
      }

      if (failed.length > 0 && retryCount < maxRetries) {
        // stays pending and is retried by the reclaim loop
        return;
      }

      for (const { name, error } of failed) {
        this.logger.error(
          `Event ${eventName} (${id}) (handler: ${name}) failed permanently after ${retryCount} retries: ${String(error)}`,
          error instanceof Error ? error.stack : undefined,
        );
      }

      await client.multi().xAck(stream.name, group, id).del(doneKey).exec();

      if (failed.length === 0) {
        this.logger.log(`Event ${eventName} (${id}) handled successfully`);
      }
    } catch (err) {
      // Redis failure, the entry stays pending
      this.logger.error(
        `Failed to process entry ${id} of ${stream.name}: ${String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    }
  }
}
