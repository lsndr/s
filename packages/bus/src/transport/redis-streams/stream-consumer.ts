import { setTimeout as sleep } from 'node:timers/promises';
import { type Logger } from '@nestjs/common';
import { type BusRetriesConfig } from '../../types.js';
import { type RedisClientType } from 'redis';

interface StreamEntry {
  id: string;
  message: Record<string, string>;
}

export interface StreamConsumerOptions {
  stream: string;
  group: string;
  consumer: string;
  eventName: string;
  client: RedisClientType;
  blockingClient: RedisClientType;
  retries: BusRetriesConfig;
  batchSize: number;
  blockTimeout: number;
  reclaimInterval: number;
  consumerTtl: number;
  logger: Logger;
  handle: (payload: string) => void | Promise<void>;
}

export class StreamConsumer {
  private readonly stopController = new AbortController();
  private readonly inflight = new Set<string>();
  private loops: Promise<void>[] = [];
  private lastCleanup = Date.now();

  public constructor(private readonly options: StreamConsumerOptions) {}

  public start(): void {
    this.loops = [this.readLoop(), this.reclaimLoop()];
  }

  /**
   * Stops the loops and waits for the handlers that are still running.
   * Entries that were not acknowledged stay pending.
   */
  public async stop(): Promise<void> {
    this.stopController.abort();

    // Interrupts a blocked XREADGROUP
    this.options.blockingClient.destroy();

    await Promise.all(this.loops);
  }

  /**
   * Deletes the consumer from the group unless it owns pending entries,
   * which still have to be retried.
   */
  public async removeConsumerIfIdle(): Promise<void> {
    const { client, stream, group, consumer } = this.options;
    const owned = await client.xPendingRange(stream, group, '-', '+', 1, {
      consumer,
    });

    if (owned.length === 0) {
      await client.xGroupDelConsumer(stream, group, consumer);
    }
  }

  /** Reads new entries of the stream until the consumer is stopped. */
  private async readLoop(): Promise<void> {
    const { blockingClient, stream, group, consumer, logger } = this.options;

    while (!this.stopController.signal.aborted) {
      try {
        const reply = await blockingClient.xReadGroup(
          group,
          consumer,
          { key: stream, id: '>' },
          { COUNT: this.options.batchSize, BLOCK: this.options.blockTimeout },
        );
        const entries = (reply?.[0]?.messages ?? []) as StreamEntry[];

        await Promise.all(entries.map((entry) => this.process(entry, 1)));
      } catch (err) {
        if (this.stopController.signal.aborted) {
          return;
        }

        logger.error(
          `Failed to read from ${stream}: ${String(err)}`,
          err instanceof Error ? err.stack : undefined,
        );
        await this.pause(1_000);
      }
    }
  }

  private async reclaimLoop(): Promise<void> {
    const { client, stream, group, consumer, logger } = this.options;
    const { delay } = this.options.retries;
    const { reclaimInterval } = this.options;

    while (!this.stopController.signal.aborted) {
      // Jitter keeps instances from polling at the same moment
      await this.pause(reclaimInterval * (0.9 + Math.random() * 0.2));

      try {
        await this.removeStaleConsumers();

        const pending = await client.xPendingRange(
          stream,
          group,
          '-',
          '+',
          this.options.batchSize,
          { IDLE: delay },
        );
        const deliveries = new Map(
          pending
            .filter(({ id }) => !this.inflight.has(id))
            .map((entry) => [entry.id, entry.deliveriesCounter]),
        );

        if (deliveries.size === 0) {
          continue;
        }

        // min-idle makes sure only one of the competing consumers wins an entry
        const claimed = (await client.xClaim(stream, group, consumer, delay, [
          ...deliveries.keys(),
        ])) as (StreamEntry | null)[];

        await Promise.all(
          claimed
            .filter((entry): entry is StreamEntry => entry !== null)
            .map((entry) =>
              this.process(entry, (deliveries.get(entry.id) ?? 0) + 1),
            ),
        );
      } catch (err) {
        if (this.stopController.signal.aborted) {
          return;
        }

        logger.error(
          `Failed to reclaim entries of ${stream}: ${String(err)}`,
          err instanceof Error ? err.stack : undefined,
        );
      }
    }
  }

  /**
   * Deletes the other consumers of the group that have no pending entries and
   * have been idle for longer than `consumerTtl`. Runs at most once per half
   * of `consumerTtl`.
   */
  private async removeStaleConsumers(): Promise<void> {
    const { client, stream, group, consumer, consumerTtl, logger } =
      this.options;

    if (Date.now() - this.lastCleanup < consumerTtl / 2) {
      return;
    }

    this.lastCleanup = Date.now();

    const consumers = await client.xInfoConsumers(stream, group);

    for (const { name, pending, idle } of consumers) {
      if (name === consumer || pending > 0 || idle <= consumerTtl) {
        continue;
      }

      await client.xGroupDelConsumer(stream, group, name);

      logger.log(`Removed stale consumer ${name} of group ${group}`);
    }
  }

  /**
   * Handles a single entry and acknowledges it on success, or on failure once
   * `retries.max` is exceeded. Otherwise the entry stays pending.
   *
   * @param entry - Stream entry.
   * @param attempt - Delivery counter of the entry, starting from `1`.
   */
  private async process(
    { id, message }: StreamEntry,
    attempt: number,
  ): Promise<void> {
    const { client, stream, group, consumer, eventName, logger } = this.options;
    const { max, delay } = this.options.retries;
    const retryCount = attempt - 1;

    this.inflight.add(id);

    // Keeps the entry from looking abandoned while the handler is running
    const heartbeat = setInterval(
      () => {
        client
          .xClaimJustId(stream, group, consumer, 0, id)
          .catch((err) =>
            logger.warn(`Failed to refresh ${id}: ${String(err)}`),
          );
      },
      Math.max(delay / 2, 10),
    );

    logger.log(`Received message for event ${eventName}`);

    try {
      await this.options.handle(message['payload'] ?? '');
      await client.xAck(stream, group, id);

      logger.log(`Event ${eventName} handled successfully`);
    } catch (err) {
      logger.error(
        `Error handling event ${eventName} (retryCount: ${retryCount}): ${String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );

      if (retryCount >= max) {
        logger.error(
          `Event ${eventName} failed permanently after ${retryCount} retries: ${String(err)}`,
          err instanceof Error ? err.stack : undefined,
        );
        await client.xAck(stream, group, id).catch(() => undefined);
      }
      // otherwise the entry stays pending and is retried by the reclaim loop
    } finally {
      clearInterval(heartbeat);
      this.inflight.delete(id);
    }
  }

  private pause(ms: number): Promise<void> {
    return sleep(ms, undefined, { signal: this.stopController.signal }).catch(
      () => undefined,
    );
  }
}
