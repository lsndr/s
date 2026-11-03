import { hostname } from 'node:os';
import { Inject, Logger } from '@nestjs/common';
import { type EventConstructor } from '../../types.js';
import {
  type OnBusBootstrap,
  type OnBusInit,
  type OnBusShutdown,
} from '../../lifecycle.js';
import { type Transport } from '../transport.js';
import { type BusProvider } from '../../provider.js';
import { type EventCallback, type Subscription } from '../subscriber.js';
import { type PublishEvent, type PublishOptions } from '../dispatcher.js';
import { Serializer } from '../../serialization/serializer.js';
import { Clients } from './clients.js';
import { Handler } from './handler.js';
import { Loops } from './loops/loops.js';
import { ReadLoop } from './loops/read.js';
import { ReclaimLoop } from './loops/reclaim.js';
import { RefreshLoop } from './loops/refresh.js';
import { StaleConsumersLoop } from './loops/stale-consumers.js';
import { Processor } from './processor.js';
import { Streams } from './streams.js';

export interface RedisStreamsTransportConfig {
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
     * milliseconds. Also how long after a consumer crashed its entries are
     * recovered.
     *
     * @default 300000
     */
    delay?: number;
  };
  /**
   * Consumer name used in every consumer group.
   *
   * Instances with the same name share their pending entries, so the default
   * includes the process id to keep instances on the same host apart. Set a
   * name that is stable across restarts to avoid leaving stale consumers in
   * the groups. Pending entries are recovered after `retries.delay` whatever
   * the name is.
   *
   * @default `${os.hostname()}-${process.pid}`
   */
  consumer?: string;
  /**
   * Prefix of all keys the transport uses: the streams, and the hashes that
   * record the handlers that have succeeded (`<keyPrefix>done:…`).
   *
   * @default 'events:'
   */
  keyPrefix?: string;
  trim?: {
    /**
     * Approximate stream length above which entries are trimmed.
     *
     * @default 100
     */
    maxLength?: number;
    /**
     * Whether only the entries that every consumer group has acknowledged are
     * trimmed, which makes `maxLength` a soft cap that unacknowledged entries
     * can exceed. When `false`, `maxLength` is a hard cap: entries are trimmed
     * even if they were not handled yet, and a trimmed entry is lost.
     *
     * @default true
     */
    onlyAcked?: boolean;
  };
  /**
   * Number of slots of each stream. A stream is read while it has a free slot,
   * and a read returns at most this many entries of a stream, so a stream runs
   * up to `2 * concurrency - 1` read entries at once. Reclaimed entries are
   * not limited by the slots, so they come on top of that.
   *
   * @default 10
   */
  concurrency?: number;
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
   * How long shutdown waits for the entries in flight, in milliseconds. Entries
   * that are not finished by then stay pending and are retried later. Waits
   * for all of them when not set.
   */
  shutdownTimeout?: number;
}

interface Settings {
  url: string;
  group: string;
  consumer: string;
  keyPrefix: string;
  retries: { max: number; delay: number };
  concurrency: number;
  reclaimInterval: number;
  /** How long a read blocks waiting for new entries, in milliseconds. */
  blockTimeout: number;
  /**
   * How long a consumer of a group can stay idle before the other consumers
   * delete it, in milliseconds.
   */
  consumerTtl: number;
  trim: { maxLength: number; onlyAcked: boolean };
  shutdownTimeout: number | undefined;
}

/** @throws If a value is out of range. */
function resolveSettings(config: RedisStreamsTransportConfig): Settings {
  const fail = (option: string, requirement: string): never => {
    throw new Error(`Invalid Redis Streams option ${option}: ${requirement}`);
  };
  const atLeast = (
    option: string,
    value: number | undefined,
    fallback: number,
    min: number,
  ): number => {
    const result = value ?? fallback;

    if (result < min) {
      fail(option, `must be at least ${min}`);
    }

    return result;
  };
  const positive = <T extends number | undefined>(
    option: string,
    value: number | undefined,
    fallback: T,
  ): number | T => {
    const result = value ?? fallback;

    if (result !== undefined && result <= 0) {
      fail(option, 'must be greater than 0');
    }

    return result as number | T;
  };

  // the entries in flight are refreshed every half of the delay
  const delay = atLeast('retries.delay', config.retries?.delay, 300_000, 20);
  return {
    url: config.url,
    group: config.group,
    consumer: config.consumer ?? `${hostname()}-${process.pid}`,
    keyPrefix: config.keyPrefix ?? 'events:',
    retries: {
      max: atLeast('retries.max', config.retries?.max, 5, 0),
      delay,
    },
    concurrency: atLeast('concurrency', config.concurrency, 10, 1),
    reclaimInterval: positive(
      'reclaimInterval',
      config.reclaimInterval,
      Math.max(delay / 4, 1_000),
    ),
    // not configurable: the TTL has to stay longer than the block timeout,
    // which is how often a live consumer shows activity
    blockTimeout: 5_000,
    consumerTtl: 3_600_000,
    trim: {
      maxLength: atLeast('trim.maxLength', config.trim?.maxLength, 100, 1),
      onlyAcked: config.trim?.onlyAcked ?? true,
    },
    shutdownTimeout: positive(
      'shutdownTimeout',
      config.shutdownTimeout,
      undefined,
    ),
  };
}

const RedisStreamsTransportConfigToken = Symbol('RedisStreamsTransportConfig');

/** Transport that publishes and consumes events through Redis Streams. */
export class RedisStreamsTransport
  implements Transport, OnBusInit, OnBusBootstrap, OnBusShutdown
{
  private readonly clients: Clients<'read' | 'command'>;
  private readonly streams: Streams;
  private readonly loops: Loops;
  private readonly settings: Settings;
  private readonly logger = new Logger(RedisStreamsTransport.name);

  public constructor(
    @Inject(RedisStreamsTransportConfigToken)
    config: RedisStreamsTransportConfig,
    @Inject(Serializer)
    private readonly serializer: Serializer,
  ) {
    this.settings = resolveSettings(config);
    this.clients = new Clients(this.settings.url);
    this.streams = new Streams(this.settings.concurrency);

    const {
      group,
      consumer,
      keyPrefix,
      concurrency,
      reclaimInterval,
      blockTimeout,
      consumerTtl,
    } = this.settings;
    const { clients, streams } = this;
    const { max: maxRetries, delay: retriesDelay } = this.settings.retries;
    const processor = new Processor(clients, serializer, {
      group,
      keyPrefix,
      maxRetries,
      retriesDelay,
      reclaimInterval,
    });

    this.loops = new Loops([
      new ReadLoop(clients, streams, processor, {
        group,
        consumer,
        batchSize: concurrency,
        blockTimeout,
      }),
      new ReclaimLoop(clients, streams, processor, {
        group,
        consumer,
        batchSize: concurrency,
        retriesDelay,
        reclaimInterval,
      }),
      new RefreshLoop(clients, streams, { group, consumer, retriesDelay }),
      new StaleConsumersLoop(clients, streams, {
        group,
        consumer,
        consumerTtl,
      }),
    ]);
  }

  public static withConfig(
    config: RedisStreamsTransportConfig,
  ): BusProvider<RedisStreamsTransport> {
    return {
      useClass: RedisStreamsTransport,
      providers: [
        { provide: RedisStreamsTransportConfigToken, useValue: config },
      ],
    };
  }

  public async onBusInit(): Promise<void> {
    await this.clients.get('command');
  }

  public onBusBootstrap(): void {
    this.loops.start();
  }

  /**
   * Stops the loops, waits for the entries in flight and removes the consumer
   * from the groups unless it owns pending entries, then closes the
   * connections.
   */
  public async onBusShutdown(): Promise<void> {
    await this.loops.onTransportShutdown();

    const drained = await this.streams.drain(this.settings.shutdownTimeout);

    if (!drained) {
      this.logger.warn(
        `Shutdown timeout reached with entries still in flight, they stay pending`,
      );
    }

    // after the drain, so that the consumer doesn't own what was in flight
    for (const { name } of this.streams.all()) {
      await this.removeConsumerIfIdle(name).catch((err) =>
        this.logger.warn(
          `Failed to remove consumer from ${name}: ${String(err)}`,
        ),
      );
    }

    await this.clients.destroy();
  }

  public async assertHealthy(): Promise<void> {
    await (await this.clients.get('command')).ping();
  }

  public publish(events: PublishEvent[]): Promise<void>;
  public publish(event: object, options?: PublishOptions): Promise<void>;
  public async publish(
    eventOrEvents: object | PublishEvent[],
    options?: PublishOptions,
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

      this.streams.register(stream, event, new Handler(handlerName, cb));

      this.logger.log(
        `Subscribed to event ${event.name} (handler: ${handlerName})`,
      );
    }
  }

  private async publishEvent(
    event: object,
    options?: PublishOptions,
  ): Promise<void> {
    const message = this.serializer.serialize(event, options);
    const { data } = message;

    this.logger.log(`Publishing event ${message.name}`);
    this.logger.verbose(`Event ${message.name} payload:`, event);

    const client = await this.clients.get('command');

    await client.xAdd(
      this.getStream(message.name),
      '*',
      { id: message.id, payload: data },
      {
        TRIM: {
          strategy: 'MAXLEN',
          strategyModifier: '~',
          threshold: this.settings.trim.maxLength,
          policy: this.settings.trim.onlyAcked ? 'ACKED' : 'KEEPREF',
        },
      },
    );

    this.logger.debug(`Event ${message.name} added to the stream`);
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
        await this.clients.get('command')
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
