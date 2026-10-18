import {
  Inject,
  Logger,
  type OnApplicationBootstrap,
  type OnModuleDestroy,
} from '@nestjs/common';
import { setTimeout } from 'node:timers/promises';
import { createEventDto, type EventDto } from '../event-dto.js';
import { type PublishEvent } from '../dispatcher.js';
import { type EventCallback, type Subscription } from '../subscriber.js';
import { type EventConstructor } from '../../types.js';
import {
  type BusTransport,
  Transport,
  type TransportPublishOptions,
} from '../transport.js';

export type OutboxRecord = EventDto;

export interface OutboxConfig<T extends Transport = Transport> {
  /** The transport the stored events are relayed to. */
  transport: BusTransport<T>;
  /**
   * How long the relay waits when there is nothing to relay, or after a
   * failure, in milliseconds.
   *
   * @default 1000
   */
  interval?: number;
}

type InferTransport<C> = C extends OutboxConfig<infer T> ? T : unknown;

export const OutboxTransport = Symbol('OutboxTransport');

/**
 * Where an outbox gets its config from. Not `TransportConfig`, which the
 * transport it relays to reads its config from.
 */
export const OutboxConfigToken = Symbol('OutboxConfig');

/**
 * Base class of transactional outboxes.
 *
 * `publish` only stores the event with {@link Outbox.save}. A background loop
 * calls {@link Outbox.relay}, which forwards the stored events to the
 * configured transport. Subscriptions and health checks go straight to that
 * transport.
 *
 * The transport is a provider of the bus module, so the bus calls its
 * lifecycle hooks itself.
 *
 * Subclasses that override `onApplicationBootstrap` or `onModuleDestroy` must
 * call `super`.
 */
export abstract class Outbox<C extends OutboxConfig = OutboxConfig>
  extends Transport<C>
  implements OnApplicationBootstrap, OnModuleDestroy
{
  @Inject(OutboxConfigToken)
  declare public config: C;

  @Inject(OutboxTransport)
  protected transport!: InferTransport<C>;

  private readonly logger = new Logger(Outbox.name);
  private abortController?: AbortController;
  private loop?: Promise<void>;

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

  public onApplicationBootstrap(): void {
    const abortController = new AbortController();

    this.abortController = abortController;
    this.loop = this.runLoop(abortController.signal);
  }

  public async onModuleDestroy(): Promise<void> {
    this.abortController?.abort();
    await this.loop;
  }

  public assertHealthy(): Promise<void> {
    return this.transport.assertHealthy();
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

    if (events.length === 0) {
      return;
    }

    await this.save(
      events.map(({ event, options }) => createEventDto(event, options)),
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

  /**
   * Publishes records to the configured transport in order, with their
   * original id, name, meta and publication time. Rejects on the first
   * failure, so the records are expected to stay pending and be retried
   * (at-least-once delivery).
   */
  protected async dispatch(records: OutboxRecord[]): Promise<void> {
    await this.transport.publish(
      records.map((record) => ({
        event: record.event,
        options: {
          id: record.id,
          name: record.name,
          meta: record.meta,
          publishedAt: new Date(record.publishedAt),
        },
      })),
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
