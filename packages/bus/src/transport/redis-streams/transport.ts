import { hostname } from 'node:os';
import {
  BeforeApplicationShutdown,
  Inject,
  Injectable,
  Logger,
  type OnModuleInit,
} from '@nestjs/common';
import { type RedisClientType } from 'redis';
import { type EventConstructor, BusConfig } from '../../types.js';
import { Transport, type TransportPublishOptions } from '../transport.js';
import { type EventCallback, type Subscription } from '../subscriber.js';
import { type PublishEvent } from '../dispatcher.js';
import { createEventDto } from '../event-dto.js';
import { StreamsConsumer } from './consumer.js';

export interface RedisStreamsTransportConfig {
  /**
   * Redis connection URL.
   *
   * @example 'redis://localhost:6379'
   */
  url: string;
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
   * Prefix of the stream keys.
   *
   * @default 'events:'
   */
  streamPrefix?: string;
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

const MIN_REDIS_VERSION = [8, 2, 0] as const;

@Injectable()
export class RedisStreamsTransport
  extends Transport<RedisStreamsTransportConfig>
  implements OnModuleInit, BeforeApplicationShutdown
{
  private client: RedisClientType | undefined;
  private consumer: StreamsConsumer | undefined;
  private readonly logger = new Logger(RedisStreamsTransport.name);

  public constructor(@Inject(BusConfig) private readonly busConfig: BusConfig) {
    super();
  }

  public async onModuleInit(): Promise<void> {
    this.client = await this.createClient(this.config);
    await this.client.connect();

    await this.assertVersion();
  }

  public async beforeApplicationShutdown(): Promise<void> {
    await this.consumer?.stop();
    await this.getClient().close();
  }

  public async assertHealthy(): Promise<void> {
    await this.getClient().ping();
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

    const consumer = this.getConsumer();

    for (const { event, handler: handlerName, callback: cb } of subscriptions) {
      await consumer.register(
        this.getStream(event.name),
        event,
        handlerName,
        cb,
      );

      this.logger.log(
        `Subscribed to event ${event.name} (handler: ${handlerName})`,
      );
    }

    // Consuming starts once the handlers are registered
    await consumer.start();
  }

  private async publishEvent(
    event: object,
    options?: TransportPublishOptions,
  ): Promise<void> {
    const payload = createEventDto(event, options);
    const json = JSON.stringify(payload);

    this.logger.log(`Publishing event ${payload.name}`);
    this.logger.verbose(`Event ${payload.name} payload:`, payload);

    await this.getClient().xAdd(
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

  private getConsumer(): StreamsConsumer {
    this.consumer ??= new StreamsConsumer({
      group: this.busConfig.group,
      consumer: this.getConsumerName(),
      client: this.getClient(),
      url: this.config.url,
      retries: this.busConfig.retries,
      batchSize: this.config.batchSize ?? 10,
      blockTimeout: this.config.blockTimeout ?? 5_000,
      reclaimInterval:
        this.config.reclaimInterval ??
        Math.max(this.busConfig.retries.delay / 4, 1_000),
      consumerTtl: this.config.consumerTtl ?? 3_600_000,
      shutdownTimeout: this.config.shutdownTimeout,
      logger: this.logger,
    });

    return this.consumer;
  }

  private async assertVersion(): Promise<void> {
    const info = await this.getClient().info('server');
    const match = /redis_version:(\d+)\.(\d+)\.(\d+)/.exec(info);
    const version = match?.slice(1).map(Number) ?? [];
    const supported = MIN_REDIS_VERSION.reduce<boolean | undefined>(
      (result, min, i) =>
        result ?? ((version[i] ?? 0) === min ? undefined : version[i]! > min),
      undefined,
    );

    if (!match || supported === false) {
      await this.getClient().close();

      throw new Error(
        `${this.constructor.name} requires Redis >= ${MIN_REDIS_VERSION.join('.')}, got ${version.join('.') || 'unknown version'}`,
      );
    }
  }

  private getClient(): RedisClientType {
    if (!this.client) {
      throw new Error('Redis client is not initialized');
    }

    return this.client;
  }

  private async createClient(
    config: RedisStreamsTransportConfig,
  ): Promise<RedisClientType> {
    const { createClient } = await import('redis');
    const client = createClient({ url: config.url });

    client.on('error', (err: unknown) => {
      this.logger.error(
        `Redis connection error: ${err instanceof Error ? err.message : String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    });

    return client;
  }

  private getStream(eventName: string): string {
    return `${this.config.streamPrefix ?? 'events:'}${eventName}`;
  }

  private getConsumerName(): string {
    return this.config.consumer ?? hostname();
  }
}
