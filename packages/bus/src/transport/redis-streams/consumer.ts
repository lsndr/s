import { setTimeout as sleep } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { type BusRetriesConfig, type EventConstructor } from '../../types.js';
import { parseEventPayload } from '../event-dto.js';
import { Clients } from './clients.js';
import { type Callback, type Stream, Streams } from './streams.js';

interface StreamEntry {
  id: string;
  message: Record<string, string>;
}

export interface StreamsConsumerOptions {
  group: string;
  consumer: string;
  /** Prefix of all keys, the streams included. */
  keyPrefix: string;
  /** Connection URL. */
  url: string;
  retries: BusRetriesConfig;
  batchSize: number;
  blockTimeout: number;
  reclaimInterval: number;
  consumerTtl: number;
  shutdownTimeout: number | undefined;
}

/**
 * Consumes all registered streams with a single consumer group.
 *
 * One blocking XREADGROUP covers every stream that has a free slot. Entries
 * run as soon as they are read, and each one frees its slot on its own.
 */
export class StreamsConsumer {
  private readonly streams: Streams;
  private readonly stopController = new AbortController();
  private readonly doneTtl: number;
  private readonly clients: Clients<'read' | 'command'>;
  private loops: Promise<void>[] = [];
  private started = false;
  private readonly logger = new Logger(StreamsConsumer.name);

  public constructor(private readonly options: StreamsConsumerOptions) {
    const { retries, reclaimInterval } = options;

    this.clients = new Clients(options.url);
    this.streams = new Streams(options.batchSize);

    // Long enough to outlive every retry of an entry
    this.doneTtl = Math.max(
      2 * (retries.max + 1) * (retries.delay + reclaimInterval),
      3_600_000,
    );
  }

  public async register(
    stream: string,
    event: EventConstructor<any>,
    handler: string,
    callback: Callback,
  ): Promise<void> {
    if (!this.streams.has(stream)) {
      await this.createGroup(stream);
    }

    this.streams.register(stream, event, handler, callback);
  }

  /**
   * Starts the loops. Does nothing when they are running already.
   */
  public start(): void {
    if (this.started) {
      return;
    }

    this.started = true;

    this.loops = [
      this.readLoop(),
      this.reclaimLoop(),
      this.refreshLoop(),
      this.cleanupLoop(),
    ];
  }

  /**
   * Stops reading and reclaiming, waits for the entries in flight and removes
   * the consumer from the groups unless it owns pending entries, then closes
   * its connections.
   */
  public async stop(): Promise<void> {
    this.stopController.abort();

    // Interrupts a blocked XREADGROUP
    await this.clients.destroy('read');

    await Promise.all(this.loops);
    await this.drain();

    for (const { name } of this.streams.all()) {
      await this.removeConsumerIfIdle(name).catch((err) =>
        this.logger.warn(
          `Failed to remove consumer from ${name}: ${String(err)}`,
        ),
      );
    }

    await this.clients.close();
  }

  private async drain(): Promise<void> {
    const { shutdownTimeout } = this.options;
    const pending = this.streams.inflight();

    if (pending.length === 0) {
      return;
    }

    const settled = Promise.all(pending).then(() => true);

    if (shutdownTimeout === undefined) {
      await settled;
      return;
    }

    const finished = await Promise.race([
      settled,
      sleep(shutdownTimeout).then(() => false),
    ]);

    if (!finished) {
      this.logger.warn(
        `Shutdown timeout reached with entries still in flight, they stay pending`,
      );
    }
  }

  private async readLoop(): Promise<void> {
    const { group, consumer, batchSize, blockTimeout } = this.options;
    const signal = this.stopController.signal;

    while (!signal.aborted) {
      // Waits for a free slot instead of calling Redis when all are full
      const readable = await this.streams.readable(signal);

      if (readable.length === 0) {
        continue;
      }

      try {
        const client = await this.clients.get('read');
        const reply =
          (await client.xReadGroup(
            group,
            consumer,
            readable.map((key) => ({ key, id: '>' })),
            { COUNT: batchSize, BLOCK: blockTimeout },
          )) ?? [];

        for (const { name, messages } of reply) {
          for (const entry of messages) {
            this.streams.run(String(name), entry.id, (stream) =>
              this.process(stream, entry, 1),
            );
          }
        }
      } catch (err) {
        if (signal.aborted) {
          return;
        }

        this.logger.error(
          `Failed to read from streams: ${String(err)}`,
          err instanceof Error ? err.stack : undefined,
        );
        await this.pause(1_000);
      }
    }
  }

  private async reclaimLoop(): Promise<void> {
    const { group, consumer, batchSize, reclaimInterval } = this.options;
    const { delay } = this.options.retries;

    while (!this.stopController.signal.aborted) {
      // Jitter keeps instances from polling at the same moment
      await this.pause(reclaimInterval * (0.9 + Math.random() * 0.2));

      if (this.stopController.signal.aborted) {
        return;
      }

      for (const state of this.streams.all()) {
        const { name: stream } = state;

        try {
          const client = await this.clients.get('command');
          const pending = await client.xPendingRange(
            stream,
            group,
            '-',
            '+',
            batchSize,
            { IDLE: delay },
          );
          const deliveries = new Map(
            pending
              .filter(({ id }) => !state.inflight.has(id))
              .map((entry) => [entry.id, entry.deliveriesCounter]),
          );

          if (deliveries.size === 0) {
            continue;
          }

          // min-idle makes sure only one of the competing consumers wins an
          // entry
          const claimed = await client.xClaim(stream, group, consumer, delay, [
            ...deliveries.keys(),
          ]);

          for (const entry of claimed) {
            if (entry && !this.stopController.signal.aborted) {
              const attempt = (deliveries.get(entry.id) ?? 0) + 1;

              this.streams.run(stream, entry.id, (current) =>
                this.process(current, entry, attempt),
              );
            }
          }
        } catch (err) {
          if (this.stopController.signal.aborted) {
            return;
          }

          this.logger.error(
            `Failed to reclaim entries of ${stream}: ${String(err)}`,
            err instanceof Error ? err.stack : undefined,
          );
        }
      }
    }
  }

  /**
   * Runs the handlers of an entry that haven't succeeded yet. The entry is
   * acknowledged once every handler has succeeded or failed permanently,
   * otherwise it stays pending and is retried by the reclaim loop.
   */
  private async process(
    stream: Stream,
    entry: StreamEntry,
    attempt: number,
  ): Promise<void> {
    const { group, keyPrefix } = this.options;
    const { event, handlers } = stream;
    const { id, message } = entry;
    const { max } = this.options.retries;
    const retryCount = attempt - 1;
    const eventName = event.name;
    const doneKey = `${keyPrefix}done:${group}:${eventName}:${id}`;

    this.logger.log(`Received message for event ${eventName}`);

    try {
      const client = await this.clients.get('command');
      const done = await client.hGetAll(doneKey);
      const pending = [...handlers].filter(([name]) => !(name in done));
      const payload = message['payload'] ?? '';

      this.logger.verbose(`Event ${eventName} payload: ${payload}`);

      const results = await Promise.all(
        pending.map(async ([name, callback]) => {
          try {
            await callback(parseEventPayload(event, payload));
          } catch (err) {
            return { name, error: err };
          }

          await client
            .multi()
            .hSet(doneKey, name, '1')
            .pExpire(doneKey, this.doneTtl)
            .exec()
            .catch((err) =>
              this.logger.warn(
                `Failed to record handler ${name} of ${id}: ${String(err)}`,
              ),
            );

          return { name, error: undefined };
        }),
      );
      const failed = results.filter(({ error }) => error !== undefined);

      for (const { name, error } of failed) {
        this.logger.error(
          `Error handling event ${eventName} (handler: ${name}, retryCount: ${retryCount}): ${String(error)}`,
          error instanceof Error ? error.stack : undefined,
        );
      }

      if (failed.length > 0 && retryCount < max) {
        // stays pending and is retried by the reclaim loop
        return;
      }

      for (const { name, error } of failed) {
        this.logger.error(
          `Event ${eventName} (handler: ${name}) failed permanently after ${retryCount} retries: ${String(error)}`,
          error instanceof Error ? error.stack : undefined,
        );
      }

      await client.multi().xAck(stream.name, group, id).del(doneKey).exec();

      if (failed.length === 0) {
        this.logger.log(`Event ${eventName} handled successfully`);
      }
    } catch (err) {
      // Redis failure, the entry stays pending
      this.logger.error(
        `Failed to process entry ${id} of ${stream.name}: ${String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    }
  }

  /** Keeps the entries in flight from looking abandoned. */
  private async refreshLoop(): Promise<void> {
    const { group, consumer } = this.options;
    const { delay } = this.options.retries;
    const { signal } = this.stopController;

    while (!signal.aborted) {
      await this.pause(Math.max(delay / 2, 10));

      for (const { name: stream, inflight } of this.streams.all()) {
        if (inflight.size === 0) {
          continue;
        }

        try {
          const client = await this.clients.get('command');

          await client.xClaimJustId(stream, group, consumer, 0, [
            ...inflight.keys(),
          ]);
        } catch (err) {
          this.logger.warn(
            `Failed to refresh entries of ${stream}: ${String(err)}`,
          );
        }
      }
    }
  }

  /**
   * Deletes the other consumers of the groups that have no pending entries and
   * have been idle for longer than `consumerTtl`. Runs once per half of
   * `consumerTtl`.
   */
  private async cleanupLoop(): Promise<void> {
    const { group, consumer, consumerTtl } = this.options;

    while (!this.stopController.signal.aborted) {
      await this.pause(consumerTtl / 2);

      if (this.stopController.signal.aborted) {
        return;
      }

      try {
        const client = await this.clients.get('command');

        for (const { name: stream } of this.streams.all()) {
          const consumers = await client.xInfoConsumers(stream, group);

          for (const { name, pending, idle } of consumers) {
            if (name === consumer || pending > 0 || idle <= consumerTtl) {
              continue;
            }

            await client.xGroupDelConsumer(stream, group, name);

            this.logger.log(`Removed stale consumer ${name} of group ${group}`);
          }
        }
      } catch (err) {
        this.logger.warn(`Failed to remove stale consumers: ${String(err)}`);
      }
    }
  }

  /**
   * Deletes the consumer from the group unless it owns pending entries,
   * which still have to be retried.
   */
  private async removeConsumerIfIdle(stream: string): Promise<void> {
    const { group, consumer } = this.options;
    const client = await this.clients.get('command');
    const owned = await client.xPendingRange(stream, group, '-', '+', 1, {
      consumer,
    });

    if (owned.length === 0) {
      await client.xGroupDelConsumer(stream, group, consumer);
    }
  }

  private async createGroup(stream: string): Promise<void> {
    try {
      await (
        await this.clients.get('read')
      ).xGroupCreate(stream, this.options.group, '$', {
        MKSTREAM: true,
      });
    } catch (err) {
      if (!String(err).includes('BUSYGROUP')) {
        throw err;
      }
    }
  }

  private async pause(ms: number): Promise<void> {
    try {
      await sleep(ms, undefined, { signal: this.stopController.signal });
    } catch {
      // Aborted on shutdown, which is how the loops are woken up
    }
  }
}
