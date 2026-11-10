import { Logger } from '@nestjs/common';
import { type Serializer } from '../../serialization/serializer.js';
import {
  type TelemetryBatch,
  type TelemetryDestination,
  type TelemetryServer,
} from '../../telemetry/telemetry.js';
import { type Instrumentation } from '../../telemetry/instrumentation.js';
import { type Clients } from './clients.js';
import { type Stream } from './streams.js';

export interface StreamEntry {
  id: string;
  message: Record<string, Buffer>;
}

export interface ProcessorOptions {
  group: string;
  /** Name of this consumer in the group. */
  consumer: string;
  keyPrefix: string;
  /** Maximum retries before an entry is dropped. */
  maxRetries: number;
  /** How long an entry has to be pending before it is claimed again. */
  retriesDelay: number;
  reclaimInterval: number;
  /** Redis server, which telemetry reports. */
  server?: TelemetryServer;
}

export class Processor {
  private readonly logger = new Logger(Processor.name);

  public constructor(
    private readonly clients: Clients<'command'>,
    private readonly serializer: Serializer,
    private readonly instrumentation: Instrumentation,
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
    batch?: TelemetryBatch,
  ): Promise<void> {
    const { clients } = this;
    const {
      group,
      consumer,
      keyPrefix,
      maxRetries,
      retriesDelay,
      reclaimInterval,
      server,
    } = this.options;
    const { event, handlers } = stream;
    const { id, message } = entry;
    const retryCount = attempt - 1;
    const eventName = event.name;
    const doneKey = `${keyPrefix}done:${group}:${eventName}:${id}`;
    const destination: TelemetryDestination = {
      system: 'redis',
      name: stream.name,
      consumerGroup: group,
      server,
    };
    // long enough to outlive every retry of an entry
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

      // a retry would only measure the delay between the retries
      if (attempt === 1) {
        this.instrumentation.deliver(destination, eventPayload.publishedAt);
      }

      const headers = this.parseHeaders(message['headers']);
      const results = await Promise.all(
        pending.map(async ({ name, callback }) => {
          try {
            await this.instrumentation.process(
              {
                system: 'redis',
                name,
                consumerGroup: group,
                subscription: name,
                clientId: consumer,
                server,
              },
              {
                id: eventPayload.id,
                name: eventName,
                headers,
                bodySize: rawPayload.length,
                attempt,
                batch,
              },
              async () => callback(eventPayload),
            );
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
        this.instrumentation.settle(destination, 'retried');

        return;
      }

      for (const { name, error } of failed) {
        this.logger.error(
          `Event ${eventName} (${id}) (handler: ${name}) failed permanently after ${retryCount} retries: ${String(error)}`,
          error instanceof Error ? error.stack : undefined,
        );
      }

      await client.multi().xAck(stream.name, group, id).del(doneKey).exec();
      this.instrumentation.settle(
        destination,
        failed.length === 0 ? 'acked' : 'dropped',
      );

      if (failed.length === 0) {
        this.logger.log(`Event ${eventName} (${id}) handled successfully`);
      }
    } catch (err) {
      // redis failure, the entry stays pending
      this.logger.error(
        `Failed to process entry ${id} of ${stream.name}: ${String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    }
  }

  private parseHeaders(raw: Buffer | undefined): Record<string, string> {
    if (!raw) {
      return {};
    }

    try {
      const parsed = JSON.parse(raw.toString());

      return typeof parsed === 'object' && parsed !== null ? parsed : {};
    } catch (err) {
      this.logger.error(
        `Failed to parse headers: ${String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );

      return {};
    }
  }
}
