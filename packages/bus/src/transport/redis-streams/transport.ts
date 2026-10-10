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
import { createEventDto } from '../event-dto.js';
import { Clients } from './clients.js';
import { StreamsConsumer } from './consumer.js';

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

@Injectable()
export class RedisStreamsTransport
  extends Transport<RedisStreamsTransportConfig>
  implements OnModuleInit, BeforeApplicationShutdown
{
  private readonly clients: Clients;
  private readonly consumer: StreamsConsumer;
  private readonly keyPrefix: string;
  private readonly logger = new Logger(RedisStreamsTransport.name);

  public constructor(
    @Inject(TransportConfig) config: RedisStreamsTransportConfig,
  ) {
    super();

    const retries = {
      max: config.retries?.max ?? 5,
      delay: config.retries?.delay ?? 5_000,
    };

    this.clients = new Clients(config.url);
    this.keyPrefix = config.keyPrefix ?? 'events:';
    this.consumer = new StreamsConsumer({
      group: config.group,
      consumer: config.consumer ?? hostname(),
      keyPrefix: this.keyPrefix,
      url: config.url,
      retries,
      batchSize: config.batchSize ?? 10,
      blockTimeout: config.blockTimeout ?? 5_000,
      reclaimInterval:
        config.reclaimInterval ?? Math.max(retries.delay / 4, 1_000),
      consumerTtl: config.consumerTtl ?? 3_600_000,
      shutdownTimeout: config.shutdownTimeout,
    });
  }

  public async onModuleInit(): Promise<void> {
    await this.clients.get();
  }

  public async beforeApplicationShutdown(): Promise<void> {
    await this.consumer.stop();
    await this.clients.close();
  }

  public async assertHealthy(): Promise<void> {
    await (await this.clients.get()).ping();
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
      await this.consumer.register(
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
    this.consumer.start();
  }

  private async publishEvent(
    event: object,
    options?: TransportPublishOptions,
  ): Promise<void> {
    const payload = createEventDto(event, options);
    const json = JSON.stringify(payload);

    this.logger.log(`Publishing event ${payload.name}`);
    this.logger.verbose(`Event ${payload.name} payload:`, payload);

    const client = await this.clients.get();

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

  private getStream(eventName: string): string {
    return `${this.keyPrefix}${eventName}`;
  }
}
