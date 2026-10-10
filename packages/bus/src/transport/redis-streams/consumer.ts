import { setTimeout as sleep } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { type RedisClientType } from 'redis';
import { type BusRetriesConfig, type EventConstructor } from '../../types.js';
import { parseEventPayload } from '../event-dto.js';
import { type Callback, type Stream, Streams } from './streams.js';

interface StreamEntry {
  id: string;
  message: Record<string, string>;
}

export interface StreamsConsumerOptions {
  group: string;
  consumer: string;
  client: RedisClientType;
  /** Connection URL of the client used for reading streams. */
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
  private client: Promise<RedisClientType> | undefined;
  private loops: Promise<void>[] = [];
  private started = false;
  private readonly logger = new Logger(StreamsConsumer.name);

  public constructor(private readonly options: StreamsConsumerOptions) {
    const { retries, reclaimInterval } = options;

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
   * the consumer from the groups unless it owns pending entries.
   */
  public async stop(): Promise<void> {
    this.stopController.abort();
    this.streams.wake();

    await this.disconnect();

    await Promise.all(this.loops);
    await this.drain();

    for (const { name } of this.streams.all()) {
      await this.removeConsumerIfIdle(name).catch((err) =>
        this.logger.warn(
          `Failed to remove consumer from ${name}: ${String(err)}`,
        ),
      );
    }
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
      const readable = this.streams.readable();

      if (readable.length === 0) {
        // Nothing to read, wait for a slot instead of calling Redis
        await this.streams.wait();
        continue;
      }

      try {
        const reply = await (
          await this.getClient()
        ).xReadGroup(
          group,
          consumer,
          readable.map((key) => ({ key, id: '>' })),
          { COUNT: batchSize, BLOCK: blockTimeout },
        );

        for (const { name, messages } of reply ?? []) {
          const state = this.streams.get(String(name));

          for (const entry of messages as StreamEntry[]) {
            if (state) {
              void this.run(state, entry, 1);
            }
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
    const { client, group, consumer, batchSize, reclaimInterval } =
      this.options;
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
          const claimed = (await client.xClaim(stream, group, consumer, delay, [
            ...deliveries.keys(),
          ])) as (StreamEntry | null)[];

          for (const entry of claimed) {
            if (entry && !this.stopController.signal.aborted) {
              void this.run(state, entry, (deliveries.get(entry.id) ?? 0) + 1);
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

  /** Runs the entry in its stream, holding one of its slots. */
  private run(
    stream: Stream,
    entry: StreamEntry,
    attempt: number,
  ): Promise<void> {
    return this.streams.run(stream, entry.id, () =>
      this.process(stream, entry, attempt),
    );
  }

  /**
   * Runs the handlers of an entry that haven't succeeded yet. The entry is
   * acknowledged once every handler has succeeded or failed permanently,
   * otherwise it stays pending and is retried by the reclaim loop.
   */
  private async process(
    { name: stream, event, handlers }: Stream,
    { id, message }: StreamEntry,
    attempt: number,
  ): Promise<void> {
    const { client, group } = this.options;
    const { max } = this.options.retries;
    const retryCount = attempt - 1;
    const eventName = event.name;
    const doneKey = `bus:done:${group}:${stream}:${id}`;

    this.logger.log(`Received message for event ${eventName}`);

    try {
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

      await client.multi().xAck(stream, group, id).del(doneKey).exec();

      if (failed.length === 0) {
        this.logger.log(`Event ${eventName} handled successfully`);
      }
    } catch (err) {
      // Redis failure, the entry stays pending
      this.logger.error(
        `Failed to process entry ${id} of ${stream}: ${String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    }
  }

  /** Keeps the entries in flight from looking abandoned. */
  private async refreshLoop(): Promise<void> {
    const { client, group, consumer } = this.options;
    const { delay } = this.options.retries;
    const { signal } = this.stopController;

    while (!signal.aborted) {
      await this.pause(Math.max(delay / 2, 10));

      for (const { name: stream, inflight } of this.streams.all()) {
        if (inflight.size === 0) {
          continue;
        }

        await client
          .xClaimJustId(stream, group, consumer, 0, [...inflight.keys()])
          .catch((err) =>
            this.logger.warn(
              `Failed to refresh entries of ${stream}: ${String(err)}`,
            ),
          );
      }
    }
  }

  /**
   * Deletes the other consumers of the groups that have no pending entries and
   * have been idle for longer than `consumerTtl`. Runs once per half of
   * `consumerTtl`.
   */
  private async cleanupLoop(): Promise<void> {
    const { client, group, consumer, consumerTtl } = this.options;

    while (!this.stopController.signal.aborted) {
      await this.pause(consumerTtl / 2);

      if (this.stopController.signal.aborted) {
        return;
      }

      try {
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
    const { client, group, consumer } = this.options;
    const owned = await client.xPendingRange(stream, group, '-', '+', 1, {
      consumer,
    });

    if (owned.length === 0) {
      await client.xGroupDelConsumer(stream, group, consumer);
    }
  }

  /**
   * Connects the client used for reading. XREADGROUP BLOCK occupies the
   * connection, so it is separate from the one of the transport.
   */
  private async connect(): Promise<RedisClientType> {
    const { createClient } = await import('redis');
    const client = createClient({ url: this.options.url });

    client.on('error', (err: unknown) => {
      this.logger.error(
        `Redis connection error: ${err instanceof Error ? err.message : String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    });

    await client.connect();

    return client;
  }

  /** Interrupts a blocked XREADGROUP by destroying the client. */
  private async disconnect(): Promise<void> {
    await this.client?.then(
      (client) => client.destroy(),
      () => undefined,
    );
  }

  /** The client used for reading, connected when it is first requested. */
  private getClient(): Promise<RedisClientType> {
    this.client ??= this.connect().catch((err) => {
      // Let the next call connect again
      this.client = undefined;

      throw err;
    });

    return this.client;
  }

  private async createGroup(stream: string): Promise<void> {
    try {
      await (
        await this.getClient()
      ).xGroupCreate(stream, this.options.group, '$', {
        MKSTREAM: true,
      });
    } catch (err) {
      if (!String(err).includes('BUSYGROUP')) {
        throw err;
      }
    }
  }

  private pause(ms: number): Promise<void> {
    return sleep(ms, undefined, { signal: this.stopController.signal }).catch(
      () => undefined,
    );
  }
}
