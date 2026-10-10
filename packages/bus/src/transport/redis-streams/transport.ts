import { hostname } from 'node:os';
import {
  BeforeApplicationShutdown,
  Inject,
  Injectable,
  Logger,
  type OnModuleInit,
} from '@nestjs/common';
import { type EventConstructor } from '../../types.js';
import {
  Transport,
  TransportConfig,
  type TransportPublishOptions,
} from '../transport.js';
import { type EventCallback, type Subscription } from '../subscriber.js';
import { type PublishEvent } from '../dispatcher.js';
import { createEventDto, parseEventPayload } from '../event-dto.js';
import { Clients } from './clients.js';
import { Loops } from './loops.js';
import { type Stream, Streams } from './streams.js';

export interface RedisStreamsTransportConfig {
  /**
   * Redis connection URL.
   *
   * @example 'redis://localhost:6379'
   */
  url: string;
  /**
   * Consumer group name. Every stream has one group with this name, so
   * instances with the same group share the entries, and every instance of a
   * group must register the same handlers.
   */
  group: string;
  retries?: {
    /**
     * Maximum retries before an entry is dropped.
     *
     * @default 5
     */
    max?: number;
    /**
     * How long an entry has to be pending before it is claimed again, in
     * milliseconds.
     *
     * @default 5000
     */
    delay?: number;
  };
  /**
   * Consumer name used in every consumer group.
   *
   * Keeping the name stable across restarts avoids leaving stale consumers in
   * the groups. Pending entries are recovered after `retries.delay` whatever
   * the name is. Set it explicitly when several instances run on the same
   * host.
   *
   * @default os.hostname()
   */
  consumer?: string;
  /**
   * Prefix of all keys the transport uses: the streams, and the hashes that
   * record the handlers that have succeeded (`<keyPrefix>done:…`).
   *
   * @default 'events:'
   */
  keyPrefix?: string;
  /**
   * Approximate stream length above which acknowledged entries are trimmed.
   * Unacknowledged entries are never trimmed, so it is a soft cap.
   *
   * @default 1000
   */
  maxLength?: number;
  /**
   * Number of slots of each stream. A stream is read while it has a free slot,
   * and a read returns at most this many entries of a stream, so a stream runs
   * up to `2 * batchSize - 1` entries at once.
   *
   * @default 10
   */
  batchSize?: number;
  /**
   * How long a stream read blocks waiting for new entries, in milliseconds.
   *
   * @default 5000
   */
  blockTimeout?: number;
  /**
   * How often pending entries are checked for retries and abandoned entries,
   * in milliseconds. A retry happens between `retries.delay` and
   * `retries.delay` plus this interval after a failure. Each check is jittered
   * by up to 10% so instances don't poll at the same moment.
   *
   * @default Math.max(retries.delay / 4, 1000)
   */
  reclaimInterval?: number;
  /**
   * How long a consumer of a group can stay idle before the other consumers
   * delete it, in milliseconds. Only consumers without pending entries are
   * deleted. Must be longer than `blockTimeout`, which is how often a live
   * consumer shows activity.
   *
   * @default 3600000
   */
  consumerTtl?: number;
  /**
   * How long shutdown waits for the entries in flight, in milliseconds. Entries
   * that are not finished by then stay pending and are retried later. Waits
   * for all of them when not set.
   */
  shutdownTimeout?: number;
}

/** Values of the config with the defaults applied. */
interface Settings {
  group: string;
  consumer: string;
  keyPrefix: string;
  retries: { max: number; delay: number };
  batchSize: number;
  blockTimeout: number;
  reclaimInterval: number;
  consumerTtl: number;
  shutdownTimeout: number | undefined;
}

interface StreamEntry {
  id: string;
  message: Record<string, string>;
}

@Injectable()
export class RedisStreamsTransport
  extends Transport<RedisStreamsTransportConfig>
  implements OnModuleInit, BeforeApplicationShutdown
{
  private readonly clients: Clients<'read' | 'command'>;
  private readonly streams: Streams;
  private readonly loops = new Loops();
  private readonly settings: Settings;
  private readonly logger = new Logger(RedisStreamsTransport.name);

  public constructor(
    @Inject(TransportConfig) config: RedisStreamsTransportConfig,
  ) {
    super();

    const retries = {
      max: config.retries?.max ?? 5,
      delay: config.retries?.delay ?? 5_000,
    };

    this.settings = {
      group: config.group,
      consumer: config.consumer ?? hostname(),
      keyPrefix: config.keyPrefix ?? 'events:',
      retries,
      batchSize: config.batchSize ?? 10,
      blockTimeout: config.blockTimeout ?? 5_000,
      reclaimInterval:
        config.reclaimInterval ?? Math.max(retries.delay / 4, 1_000),
      consumerTtl: config.consumerTtl ?? 3_600_000,
      shutdownTimeout: config.shutdownTimeout,
    };
    this.clients = new Clients(config.url);
    this.streams = new Streams(this.settings.batchSize);
  }

  public async onModuleInit(): Promise<void> {
    await this.clients.get('command');
  }

  /**
   * Stops reading and reclaiming, waits for the entries in flight and removes
   * the consumer from the groups unless it owns pending entries, then closes
   * the connections.
   */
  public async beforeApplicationShutdown(): Promise<void> {
    this.loops.abort();

    // Interrupts a blocked XREADGROUP
    await this.clients.destroy('read');

    await this.loops.wait();

    const drained = await this.streams.drain(this.settings.shutdownTimeout);

    if (!drained) {
      this.logger.warn(
        `Shutdown timeout reached with entries still in flight, they stay pending`,
      );
    }

    for (const { name } of this.streams.all()) {
      await this.removeConsumerIfIdle(name).catch((err) =>
        this.logger.warn(
          `Failed to remove consumer from ${name}: ${String(err)}`,
        ),
      );
    }

    await this.clients.close();
  }

  public async assertHealthy(): Promise<void> {
    await (await this.clients.get('command')).ping();
  }

  public publish(
    events: PublishEvent<TransportPublishOptions>[],
  ): Promise<void>;
  public publish(
    event: object,
    options?: TransportPublishOptions,
  ): Promise<void>;
  public async publish(
    eventOrEvents: object | PublishEvent<TransportPublishOptions>[],
    options?: TransportPublishOptions,
  ): Promise<void> {
    const events = Array.isArray(eventOrEvents)
      ? eventOrEvents
      : [{ event: eventOrEvents, options }];

    for (const { event, options: eventOptions } of events) {
      await this.publishEvent(event, eventOptions);
    }
  }

  public subscribe(subscriptions: Subscription[]): Promise<void>;
  public subscribe<
    C extends EventConstructor<any>,
    E extends InstanceType<C> = InstanceType<C>,
  >(event: C, handler: string, callback: EventCallback<E>): Promise<void>;
  public async subscribe(
    eventOrSubscriptions: EventConstructor<any> | Subscription[],
    handler?: string,
    callback?: EventCallback<any>,
  ): Promise<void> {
    const subscriptions = Array.isArray(eventOrSubscriptions)
      ? eventOrSubscriptions
      : [
          {
            event: eventOrSubscriptions,
            handler: handler!,
            callback: callback!,
          },
        ];

    for (const { event, handler: handlerName, callback: cb } of subscriptions) {
      const stream = this.getStream(event.name);

      if (!this.streams.has(stream)) {
        await this.createGroup(stream);
      }

      this.streams.register(stream, event, handlerName, cb);

      this.logger.log(
        `Subscribed to event ${event.name} (handler: ${handlerName})`,
      );
    }

    // Consuming starts once the handlers are registered
    this.start();
  }

  private async publishEvent(
    event: object,
    options?: TransportPublishOptions,
  ): Promise<void> {
    const payload = createEventDto(event, options);
    const json = JSON.stringify(payload);

    this.logger.log(`Publishing event ${payload.name}`);
    this.logger.verbose(`Event ${payload.name} payload:`, payload);

    const client = await this.clients.get('command');

    await client.xAdd(
      this.getStream(payload.name),
      '*',
      { id: payload.id, payload: json },
      {
        TRIM: {
          strategy: 'MAXLEN',
          strategyModifier: '~',
          threshold: this.config.maxLength ?? 1_000,
          policy: 'ACKED',
        },
      },
    );

    this.logger.debug(`Event ${payload.name} added to the stream`);
  }

  /**
   * Starts the loops that consume the streams. Does nothing when they are
   * running already.
   */
  private start(): void {
    if (this.loops.running) {
      return;
    }

    const { reclaimInterval, consumerTtl } = this.settings;
    const { delay } = this.settings.retries;

    this.loops.start('read from streams', () => this.read(), {
      retryDelay: 1_000,
    });
    this.loops.start('reclaim entries', () => this.reclaim(), {
      // Jitter keeps instances from polling at the same moment
      interval: () => reclaimInterval * (0.9 + Math.random() * 0.2),
    });
    this.loops.start('refresh entries', () => this.refresh(), {
      interval: () => Math.max(delay / 2, 10),
    });
    this.loops.start('remove stale consumers', () => this.removeStale(), {
      interval: () => consumerTtl / 2,
    });
  }

  /** Reads the streams that have a free slot, and starts the entries. */
  private async read(): Promise<void> {
    const { group, consumer, batchSize, blockTimeout } = this.settings;

    // Waits for a free slot instead of calling Redis when all are full
    const readable = await this.streams.readable(this.loops.signal);

    if (readable.length === 0) {
      return;
    }

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
  }

  /** Claims the entries that have been pending for too long, and runs them. */
  private async reclaim(): Promise<void> {
    const { group, consumer, batchSize } = this.settings;
    const { delay } = this.settings.retries;

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
          if (entry && !this.loops.signal.aborted) {
            const attempt = (deliveries.get(entry.id) ?? 0) + 1;

            this.streams.run(stream, entry.id, (current) =>
              this.process(current, entry, attempt),
            );
          }
        }
      } catch (err) {
        if (this.loops.signal.aborted) {
          return;
        }

        this.logger.error(
          `Failed to reclaim entries of ${stream}: ${String(err)}`,
          err instanceof Error ? err.stack : undefined,
        );
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
    const { group, keyPrefix, reclaimInterval } = this.settings;
    const { event, handlers } = stream;
    const { id, message } = entry;
    const { max, delay } = this.settings.retries;
    const retryCount = attempt - 1;
    const eventName = event.name;
    const doneKey = `${keyPrefix}done:${group}:${eventName}:${id}`;
    // Long enough to outlive every retry of an entry
    const doneTtl = Math.max(
      2 * (max + 1) * (delay + reclaimInterval),
      3_600_000,
    );

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
            .pExpire(doneKey, doneTtl)
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
  private async refresh(): Promise<void> {
    const { group, consumer } = this.settings;

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

  /**
   * Deletes the other consumers of the groups that have no pending entries and
   * have been idle for longer than `consumerTtl`.
   */
  private async removeStale(): Promise<void> {
    const { group, consumer, consumerTtl } = this.settings;
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
  }

  /**
   * Deletes the consumer from the group unless it owns pending entries,
   * which still have to be retried.
   */
  private async removeConsumerIfIdle(stream: string): Promise<void> {
    const { group, consumer } = this.settings;
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
      ).xGroupCreate(stream, this.settings.group, '$', {
        MKSTREAM: true,
      });
    } catch (err) {
      if (!String(err).includes('BUSYGROUP')) {
        throw err;
      }
    }
  }

  private getStream(eventName: string): string {
    return `${this.settings.keyPrefix}${eventName}`;
  }
}
