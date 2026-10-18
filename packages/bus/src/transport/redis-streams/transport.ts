import { hostname } from 'node:os';
import { Inject, Injectable, Logger, type OnModuleInit } from '@nestjs/common';
import { type EventConstructor } from '../../types.js';
import { type OnBusBootstrap, type OnBusShutdown } from '../../lifecycle.js';
import {
  Transport,
  TransportConfig,
  type TransportPublishOptions,
} from '../transport.js';
import { type EventCallback, type Subscription } from '../subscriber.js';
import { type PublishEvent } from '../dispatcher.js';
import { createEventDto } from '../event-dto.js';
import { type TelemetryDescriptor } from '../../telemetry/telemetry.js';
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

/** Values of the config with the defaults applied. */
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

/**
 * Applies the defaults to the config and checks the values.
 *
 * @throws If a value is out of range.
 */
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

  // The entries in flight are refreshed every half of the delay
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
    // Not configurable. The TTL has to stay longer than the block timeout,
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

/** The address of the server in a Redis URL, if it is a valid one. */
function parseServer(url: string): TelemetryDescriptor['server'] {
  try {
    const { hostname, port } = new URL(url);

    return hostname
      ? { address: hostname, port: port ? Number(port) : 6379 }
      : undefined;
  } catch {
    return undefined;
  }
}

@Injectable()
export class RedisStreamsTransport
  extends Transport<RedisStreamsTransportConfig>
  implements OnModuleInit, OnBusBootstrap, OnBusShutdown
{
  private readonly clients: Clients<'read' | 'command'>;
  private readonly streams: Streams;
  private readonly loops: Loops;
  private readonly settings: Settings;
  private readonly logger = new Logger(RedisStreamsTransport.name);

  public constructor(
    @Inject(TransportConfig) config: RedisStreamsTransportConfig,
  ) {
    super();

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
    const telemetry = () => this.telemetry;
    const processor = new Processor(
      clients,
      {
        group,
        keyPrefix,
        maxRetries,
        retriesDelay,
        reclaimInterval,
      },
      telemetry,
    );

    this.loops = new Loops([
      new ReadLoop(clients, streams, processor, {
        group,
        consumer,
        batchSize: concurrency,
        blockTimeout,
      }),
      new ReclaimLoop(
        clients,
        streams,
        processor,
        {
          group,
          consumer,
          batchSize: concurrency,
          retriesDelay,
          reclaimInterval,
        },
        telemetry,
      ),
      new RefreshLoop(clients, streams, { group, consumer, retriesDelay }),
      new StaleConsumersLoop(
        clients,
        streams,
        {
          group,
          consumer,
          consumerTtl,
        },
        telemetry,
      ),
    ]);
  }

  public async onModuleInit(): Promise<void> {
    await this.clients.get('command');
  }

  public override getTelemetryDescriptor(): TelemetryDescriptor {
    const { url, group, keyPrefix } = this.settings;
    const server = parseServer(url);

    return {
      // Redis is not one of the well-known messaging systems
      system: 'redis',
      destination: (eventName) => `${keyPrefix}${eventName}`,
      consumerGroup: group,
      server,
    };
  }

  /** Starts consuming, once all the handlers are registered. */
  public onBusBootstrap(): void {
    this.telemetry.gauge(
      'redis_streams.inflight',
      {
        description: 'Entries being handled by this instance',
        unit: '{entry}',
      },
      () =>
        this.streams.all().map(({ name }) => ({
          value: this.streams.inflight(name).length,
          attributes: { 'messaging.destination.name': name },
        })),
    );

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

    // After the drain, so that the consumer doesn't own what was in flight
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

      this.streams.register(stream, event, new Handler(handlerName, cb));

      this.logger.log(
        `Subscribed to event ${event.name} (handler: ${handlerName})`,
      );
    }
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
          threshold: this.settings.trim.maxLength,
          policy: this.settings.trim.onlyAcked ? 'ACKED' : 'KEEPREF',
        },
      },
    );

    this.logger.debug(`Event ${payload.name} added to the stream`);
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
