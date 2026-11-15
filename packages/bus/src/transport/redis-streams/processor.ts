import { Logger } from '@nestjs/common';
import { type Serializer } from '../../serialization/serializer.js';
import {
  type TelemetryBatch,
  type TelemetryDestination,
  type TelemetryServer,
} from '../../telemetry/telemetry.js';
import { type Instrumentation } from '../../telemetry/instrumentation.js';
import { type EventPayload } from '../../types.js';
import { type Clients } from './clients.js';
import { type Stream } from './streams.js';

export interface StreamEntry {
  id: string;
  message: { id: Buffer; payload: Buffer; headers?: Buffer };
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

interface Delivery {
  stream: Stream;
  /** Id of the stream entry. */
  streamId: string;
  attempt: number;
  destination: TelemetryDestination;
  doneKey: string;
  headers: Record<string, string>;
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
   * otherwise it stays pending and is retried by the reclaim loop. An entry
   * that fails to deserialize is retried and acknowledged the same way.
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
    const { id: streamId, message } = entry;
    const eventName = event.name;
    const doneKey = `${keyPrefix}done:${group}:${eventName}:${streamId}`;
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

    this.logger.log(`Received message for event ${eventName} (${streamId})`);

    try {
      const client = await clients.get('command');
      const done = await client.hGetAll(doneKey);
      const pending = handlers.filter(({ name }) => !(name in done));
      const rawPayload = message.payload;
      const headers = this.parseHeaders(message.headers);
      const delivery: Delivery = {
        stream,
        streamId,
        attempt,
        destination,
        doneKey,
        headers,
      };
      let eventPayload: EventPayload<object>;

      try {
        eventPayload = this.serializer.deserialize(event, rawPayload);
      } catch (err) {
        // the payload, which has the id of the event, can't be read
        await this.settle(delivery, message.id.toString(), [err]);

        return;
      }

      this.logger.verbose(
        `Event ${eventName} (${streamId}) payload:`,
        eventPayload,
      );

      // a retry would only measure the delay between the retries
      if (attempt === 1) {
        this.instrumentation.deliver(destination, eventPayload.publishedAt);
      }

      const results = await Promise.all(
        pending.map(async ({ name, callback }) => {
          try {
            await this.instrumentation.process(
              {
                ...destination,
                subscription: name,
                clientId: consumer,
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
            return { error: err };
          }

          await client
            .multi()
            .hSet(doneKey, name, '1')
            .pExpire(doneKey, doneTtl)
            .exec()
            .catch((err) =>
              this.logger.warn(
                `Failed to record handler ${name} of ${streamId}: ${String(err)}`,
                err instanceof Error ? err.stack : undefined,
              ),
            );

          return undefined;
        }),
      );
      const failed = results
        .filter((result) => result !== undefined)
        .map(({ error }) => error);

      await this.settle(delivery, eventPayload.id, failed);

      if (failed.length === 0) {
        this.logger.log(
          `Event ${eventName} (${streamId}) handled successfully`,
        );
      }
    } catch (err) {
      // redis failure, the entry stays pending
      this.logger.error(
        `Failed to process entry ${streamId} of ${stream.name}: ${String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    }
  }

  private async settle(
    delivery: Delivery,
    messageId: string,
    errors: unknown[],
  ): Promise<void> {
    const { group, maxRetries } = this.options;
    const { stream, streamId, attempt, destination, doneKey, headers } =
      delivery;
    const eventName = stream.event.name;
    const retryCount = attempt - 1;

    for (const error of errors) {
      this.logger.error(
        `Error handling event ${eventName} (${streamId}) (retryCount: ${retryCount}): ${String(error)}`,
        error instanceof Error ? error.stack : undefined,
      );
    }

    if (errors.length > 0 && retryCount < maxRetries) {
      this.instrumentation.retry(destination);

      return;
    }

    for (const error of errors) {
      this.logger.error(
        `Event ${eventName} (${streamId}) failed permanently after ${retryCount} retries: ${String(error)}`,
        error instanceof Error ? error.stack : undefined,
      );
    }

    const client = await this.clients.get('command');

    await this.instrumentation.settle(
      { ...destination, operation: 'ack' },
      { id: messageId, name: eventName, headers, attempt },
      errors.length === 0 ? 'acked' : 'dropped',
      () =>
        client.multi().xAck(stream.name, group, streamId).del(doneKey).exec(),
    );
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
