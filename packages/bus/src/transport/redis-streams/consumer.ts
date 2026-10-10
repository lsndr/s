import { setTimeout as sleep } from 'node:timers/promises';
import { type Logger } from '@nestjs/common';
import { type RedisClientType } from 'redis';
import {
  type BusRetriesConfig,
  type EventConstructor,
  type EventPayload,
} from '../../types.js';
import { parseEventPayload } from '../event-dto.js';

type Callback = (event: EventPayload<any>) => void | Promise<void>;

interface StreamEntry {
  id: string;
  message: Record<string, string>;
}

interface StreamState {
  stream: string;
  event: EventConstructor<any>;
  handlers: Map<string, Callback>;
  inflight: Map<string, Promise<void>>;
  /** Free slots. Can drop to zero or below, since overflow is accepted. */
  free: number;
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
  logger: Logger;
}

/**
 * Consumes all registered streams with a single consumer group.
 *
 * One blocking XREADGROUP covers every stream that has a free slot. Entries
 * run as soon as they are read, and each one frees its slot on its own.
 */
export class StreamsConsumer {
  private readonly streams = new Map<string, StreamState>();
  private readonly stopController = new AbortController();
  private readonly doneTtl: number;
  private client: RedisClientType | undefined;
  private connecting: Promise<RedisClientType> | undefined;
  private loops: Promise<void>[] = [];
  private heartbeat: NodeJS.Timeout | undefined;
  private started = false;
  private lastCleanup = Date.now();
  private wakeUp: (() => void) | undefined;

  public constructor(private readonly options: StreamsConsumerOptions) {
    const { retries, reclaimInterval } = options;

    // Long enough to outlive every retry of an entry
    this.doneTtl = Math.max(
      2 * (retries.max + 1) * (retries.delay + reclaimInterval),
      3_600_000,
    );
  }

  /**
   * Adds a handler and creates the consumer group of its stream. A stream
   * registered while the consumer is running joins the next read.
   *
   * The group is created on the reading connection, so a running read can
   * delay it by up to `blockTimeout`.
   */
  public async register(
    stream: string,
    event: EventConstructor<any>,
    handler: string,
    callback: Callback,
  ): Promise<void> {
    let state = this.streams.get(stream);

    if (state?.handlers.has(handler)) {
      throw new Error(`Handler ${handler} is already subscribed to ${stream}`);
    }

    if (!state) {
      await this.createGroup(stream);

      // Registered meanwhile by a concurrent call
      state = this.streams.get(stream) ?? {
        stream,
        event,
        handlers: new Map(),
        inflight: new Map(),
        free: this.options.batchSize,
      };
      this.streams.set(stream, state);
    }

    state.handlers.set(handler, callback);
    this.signal();
  }

  /** Starts the loops. Does nothing when they are running already. */
  public async start(): Promise<void> {
    if (this.started) {
      return;
    }

    this.started = true;
    await this.connect();

    const { delay } = this.options.retries;

    this.heartbeat = setInterval(
      () => void this.refreshInflight(),
      Math.max(delay / 2, 10),
    );
    this.loops = [this.readLoop(), this.reclaimLoop()];
  }

  /**
   * Stops reading and reclaiming, waits for the entries in flight and removes
   * the consumer from the groups unless it owns pending entries.
   */
  public async stop(): Promise<void> {
    this.stopController.abort();
    this.signal();

    // Interrupts a blocked XREADGROUP
    this.client?.destroy();

    await Promise.all(this.loops);
    await this.drain();

    if (this.heartbeat) {
      clearInterval(this.heartbeat);
    }

    for (const { stream } of this.streams.values()) {
      await this.removeConsumerIfIdle(stream).catch((err) =>
        this.options.logger.warn(
          `Failed to remove consumer from ${stream}: ${String(err)}`,
        ),
      );
    }
  }

  private async drain(): Promise<void> {
    const { shutdownTimeout, logger } = this.options;
    const pending = [...this.streams.values()].flatMap((state) => [
      ...state.inflight.values(),
    ]);

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
      logger.warn(
        `Shutdown timeout reached with entries still in flight, they stay pending`,
      );
    }
  }

  private async readLoop(): Promise<void> {
    const { group, consumer, batchSize, blockTimeout, logger } = this.options;
    const signal = this.stopController.signal;

    while (!signal.aborted) {
      const readable = [...this.streams.values()].filter(
        (state) => state.free > 0,
      );

      if (readable.length === 0) {
        // Nothing to read, wait for a slot instead of calling Redis
        await this.waitForSignal();
        continue;
      }

      try {
        const reply = await this.client!.xReadGroup(
          group,
          consumer,
          readable.map(({ stream }) => ({ key: stream, id: '>' })),
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

        logger.error(
          `Failed to read from streams: ${String(err)}`,
          err instanceof Error ? err.stack : undefined,
        );
        await this.pause(1_000);
      }
    }
  }

  private async reclaimLoop(): Promise<void> {
    const { client, group, consumer, batchSize, reclaimInterval, logger } =
      this.options;
    const { delay } = this.options.retries;

    while (!this.stopController.signal.aborted) {
      // Jitter keeps instances from polling at the same moment
      await this.pause(reclaimInterval * (0.9 + Math.random() * 0.2));

      if (this.stopController.signal.aborted) {
        return;
      }

      try {
        await this.removeStaleConsumers();
      } catch (err) {
        logger.warn(`Failed to remove stale consumers: ${String(err)}`);
      }

      for (const state of this.streams.values()) {
        const { stream } = state;

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

          logger.error(
            `Failed to reclaim entries of ${stream}: ${String(err)}`,
            err instanceof Error ? err.stack : undefined,
          );
        }
      }
    }
  }

  /** Takes a slot, runs the entry and frees the slot when it is finished. */
  private run(
    state: StreamState,
    entry: StreamEntry,
    attempt: number,
  ): Promise<void> {
    state.free -= 1;

    const promise = this.process(state, entry, attempt).finally(() => {
      state.inflight.delete(entry.id);
      state.free += 1;
      this.signal();
    });

    state.inflight.set(entry.id, promise);

    return promise;
  }

  /**
   * Runs the handlers of an entry that haven't succeeded yet. The entry is
   * acknowledged once every handler has succeeded or failed permanently,
   * otherwise it stays pending and is retried by the reclaim loop.
   */
  private async process(
    { stream, event, handlers }: StreamState,
    { id, message }: StreamEntry,
    attempt: number,
  ): Promise<void> {
    const { client, group, logger } = this.options;
    const { max } = this.options.retries;
    const retryCount = attempt - 1;
    const eventName = event.name;
    const doneKey = `bus:done:${group}:${stream}:${id}`;

    logger.log(`Received message for event ${eventName}`);

    try {
      const done = await client.hGetAll(doneKey);
      const pending = [...handlers].filter(([name]) => !(name in done));
      const payload = message['payload'] ?? '';

      logger.verbose(`Event ${eventName} payload: ${payload}`);

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
              logger.warn(
                `Failed to record handler ${name} of ${id}: ${String(err)}`,
              ),
            );

          return { name, error: undefined };
        }),
      );
      const failed = results.filter(({ error }) => error !== undefined);

      for (const { name, error } of failed) {
        logger.error(
          `Error handling event ${eventName} (handler: ${name}, retryCount: ${retryCount}): ${String(error)}`,
          error instanceof Error ? error.stack : undefined,
        );
      }

      if (failed.length > 0 && retryCount < max) {
        // stays pending and is retried by the reclaim loop
        return;
      }

      for (const { name, error } of failed) {
        logger.error(
          `Event ${eventName} (handler: ${name}) failed permanently after ${retryCount} retries: ${String(error)}`,
          error instanceof Error ? error.stack : undefined,
        );
      }

      await client.multi().xAck(stream, group, id).del(doneKey).exec();

      if (failed.length === 0) {
        logger.log(`Event ${eventName} handled successfully`);
      }
    } catch (err) {
      // Redis failure, the entry stays pending
      logger.error(
        `Failed to process entry ${id} of ${stream}: ${String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    }
  }

  /** Keeps the entries in flight from looking abandoned. */
  private async refreshInflight(): Promise<void> {
    const { client, group, consumer, logger } = this.options;

    for (const { stream, inflight } of this.streams.values()) {
      if (inflight.size === 0) {
        continue;
      }

      await client
        .xClaimJustId(stream, group, consumer, 0, [...inflight.keys()])
        .catch((err) =>
          logger.warn(`Failed to refresh entries of ${stream}: ${String(err)}`),
        );
    }
  }

  /**
   * Deletes the other consumers of the groups that have no pending entries and
   * have been idle for longer than `consumerTtl`. Runs at most once per half
   * of `consumerTtl`.
   */
  private async removeStaleConsumers(): Promise<void> {
    const { client, group, consumer, consumerTtl, logger } = this.options;

    if (Date.now() - this.lastCleanup < consumerTtl / 2) {
      return;
    }

    this.lastCleanup = Date.now();

    for (const stream of this.streams.keys()) {
      const consumers = await client.xInfoConsumers(stream, group);

      for (const { name, pending, idle } of consumers) {
        if (name === consumer || pending > 0 || idle <= consumerTtl) {
          continue;
        }

        await client.xGroupDelConsumer(stream, group, name);

        logger.log(`Removed stale consumer ${name} of group ${group}`);
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
  private connect(): Promise<RedisClientType> {
    this.connecting ??= this.createClient().then((client) => {
      this.client = client;

      return client;
    });

    return this.connecting;
  }

  private async createGroup(stream: string): Promise<void> {
    const client = await this.connect();

    try {
      await client.xGroupCreate(stream, this.options.group, '$', {
        MKSTREAM: true,
      });
    } catch (err) {
      if (!String(err).includes('BUSYGROUP')) {
        throw err;
      }
    }
  }

  private async createClient(): Promise<RedisClientType> {
    const { createClient } = await import('redis');
    const client = createClient({ url: this.options.url });

    client.on('error', (err: unknown) => {
      this.options.logger.error(
        `Redis connection error: ${err instanceof Error ? err.message : String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    });

    await client.connect();

    return client;
  }

  private waitForSignal(): Promise<void> {
    return new Promise((resolve) => {
      this.wakeUp = resolve;
    });
  }

  private signal(): void {
    const wakeUp = this.wakeUp;

    this.wakeUp = undefined;
    wakeUp?.();
  }

  private pause(ms: number): Promise<void> {
    return sleep(ms, undefined, { signal: this.stopController.signal }).catch(
      () => undefined,
    );
  }
}
