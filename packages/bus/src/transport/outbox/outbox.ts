import {
  Inject,
  Logger,
  type OnApplicationBootstrap,
  type OnModuleDestroy,
} from '@nestjs/common';
import { setTimeout } from 'node:timers/promises';
import { type EventConstructor } from '../../types.js';
import { createEventDto, type EventDto } from '../event-dto.js';
import { type EventCallback } from '../subscriber.js';
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
 * Base class of transactional outboxes.
 *
 * `publish` only stores the event with {@link Outbox.save}. A background loop
 * calls {@link Outbox.relay}, which forwards the stored events to the
 * configured transport. Subscriptions and health checks go straight to that
 * transport.
 *
 * Subclasses that override `onApplicationBootstrap` or `onModuleDestroy` must
 * call `super`.
 */
export abstract class Outbox<C extends OutboxConfig = OutboxConfig>
  extends Transport<C>
  implements OnApplicationBootstrap, OnModuleDestroy
{
  @Inject(OutboxTransport)
  protected transport!: InferTransport<C>;

  private readonly logger = new Logger(Outbox.name);
  private abortController?: AbortController;
  private loop?: Promise<void>;

  /**
   * Stores a record. To make the event part of the caller's transaction, join
   * it here, e.g. through `AsyncLocalStorage`.
   */
  protected abstract save(record: OutboxRecord): Promise<void>;

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

  public async publish<E extends object>(
    event: E,
    options?: TransportPublishOptions,
  ): Promise<void> {
    await this.save(createEventDto(event, options));
  }

  public subscribe<
    C extends EventConstructor<any>,
    E extends InstanceType<C> = InstanceType<C>,
  >(event: C, handler: string, callback: EventCallback<E>): Promise<void> {
    return this.transport.subscribe(event, handler, callback);
  }

  public assertHealthy(): Promise<void> {
    return this.transport.assertHealthy();
  }

  /**
   * Publishes records to the configured transport one by one, in order, with
   * their original id, name, meta and publication time. Rejects on the first
   * failure, so the records are expected to stay pending and be retried
   * (at-least-once delivery).
   */
  protected async dispatch(records: OutboxRecord[]): Promise<void> {
    for (const record of records) {
      await this.transport.publish(record.event, {
        id: record.id,
        name: record.name,
        meta: record.meta,
        publishedAt: new Date(record.publishedAt),
      });
    }
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
