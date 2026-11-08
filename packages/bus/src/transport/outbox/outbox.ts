import { Inject, Logger, type Type } from '@nestjs/common';
import { setTimeout } from 'node:timers/promises';
import {
  type SerializedMessage,
  Serializer,
} from '../../serialization/serializer.js';
import { type PublishEvent, type PublishOptions } from '../dispatcher.js';
import { type EventCallback, type Subscription } from '../subscriber.js';
import { type EventConstructor } from '../../types.js';
import { type OnBusBootstrap, type OnBusShutdown } from '../../lifecycle.js';
import { type Transport } from '../transport.js';
import { type BusProvider, toProviders } from '../../provider.js';
import { Telemetry } from '../../telemetry/telemetry.js';

export type OutboxRecord = SerializedMessage;

export interface OutboxConfig<T extends Transport = Transport> {
  /**
   * The transport the stored events are relayed to, provided the same way as
   * the `transport` option of the bus module.
   */
  transport: BusProvider<T>;
  /**
   * How long the relay waits when there is nothing to relay, or after a
   * failure, in milliseconds.
   *
   * @default 1000
   */
  interval?: number;
}

type InferConfig<O> = O extends Outbox<infer C> ? C : never;

type InferTransport<C> = C extends OutboxConfig<infer T> ? T : Transport;

const OutboxTransport = Symbol('OutboxTransport');

const OutboxConfigToken = Symbol('OutboxConfig');

/**
 * Base class of transactional outboxes: `publish` only stores the event with
 * {@link Outbox.save}, and a background loop forwards the stored events to the
 * transport with {@link Outbox.relay}. Subscriptions and health checks go
 * straight to that transport.
 *
 * Subclasses that override `onBusBootstrap` or `onBusShutdown` must call
 * `super`.
 */
export abstract class Outbox<C extends OutboxConfig = OutboxConfig>
  implements Transport, OnBusBootstrap, OnBusShutdown
{
  @Inject(OutboxConfigToken)
  protected config!: C;

  @Inject(OutboxTransport)
  protected transport!: InferTransport<C>;

  @Inject(Serializer)
  protected serializer!: Serializer;

  @Inject(Telemetry)
  protected telemetry!: Telemetry;

  private readonly logger = new Logger(Outbox.name);
  private abortController?: AbortController;
  private loop?: Promise<void>;

  /**
   * The transport is registered in the bus module next to the outbox, so the
   * bus calls its lifecycle hooks itself, before the ones of the outbox and
   * after them on shutdown.
   */
  public static withConfig<O extends Outbox<any>>(
    this: Type<O>,
    config: InferConfig<O>,
  ): BusProvider<O> {
    return {
      useClass: this,
      providers: [
        { provide: OutboxConfigToken, useValue: config },
        ...toProviders(OutboxTransport, config.transport),
      ],
    };
  }

  /**
   * Stores the records of one `publish` call, in order. Store them together,
   * so that a batch is either saved entirely or not at all. To make the events
   * part of the caller's transaction, join it here, e.g. through
   * `AsyncLocalStorage`.
   */
  protected abstract save(records: OutboxRecord[]): Promise<void>;

  /**
   * Relays pending records. Implementations lock the pending records they
   * take (e.g. `SELECT … FOR UPDATE SKIP LOCKED`) so several instances can run
   * together, `await this.dispatch(records)`, and mark the records dispatched
   * only if that resolves, all in one transaction.
   *
   * @returns The number of records dispatched. The relay runs again right away
   * if it is above zero, otherwise it waits for `interval`.
   */
  protected abstract relay(): Promise<number>;

  public onBusBootstrap(): void {
    const abortController = new AbortController();

    this.abortController = abortController;
    this.loop = this.runLoop(abortController.signal);
  }

  public async onBusShutdown(): Promise<void> {
    this.abortController?.abort();
    await this.loop;
  }

  public assertHealthy(): Promise<void> {
    return this.transport.assertHealthy();
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

    if (events.length === 0) {
      return;
    }

    const records = events.map(({ event, options }) =>
      this.serializer.serialize(event, options),
    );

    await this.telemetry.create(
      { system: 'outbox' },
      records.map(({ id, name, headers }) => ({ id, name, headers })),
      () => this.save(records),
    );
  }

  public subscribe(subscriptions: Subscription[]): Promise<void>;
  public subscribe<
    C extends EventConstructor<any>,
    E extends InstanceType<C> = InstanceType<C>,
  >(event: C, handler: string, callback: EventCallback<E>): Promise<void>;
  public subscribe(
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

    return this.transport.subscribe(subscriptions);
  }

  protected async dispatch(records: OutboxRecord[]): Promise<void> {
    await this.transport.publish(
      records.map((record) => {
        const { event, meta, publishedAt } = this.serializer.deserialize(
          Object,
          record.data,
        );

        return {
          event,
          options: {
            id: record.id,
            name: record.name,
            meta,
            publishedAt,
            headers: record.headers,
          },
        };
      }),
    );
  }

  private async runLoop(signal: AbortSignal): Promise<void> {
    const interval = this.config.interval ?? 1_000;

    while (!signal.aborted) {
      let relayed = 0;

      try {
        relayed = await this.relay();
      } catch (err) {
        this.logger.error(
          `Failed to relay outbox records: ${String(err)}`,
          err instanceof Error ? err.stack : undefined,
        );
      }

      if (relayed === 0) {
        await setTimeout(interval, undefined, { signal }).catch(
          () => undefined,
        );
      }
    }
  }
}
