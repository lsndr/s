import { Inject, Logger, type Type } from '@nestjs/common';
import { setTimeout } from 'node:timers/promises';
import {
  type SerializedMessage,
  Serializer,
} from '../../serialization/serializer.js';
import { type PublishOptions } from '../dispatcher.js';
import { type EventCallback } from '../subscriber.js';
import { type EventConstructor } from '../../types.js';
import { type OnBusBootstrap, type OnBusShutdown } from '../../lifecycle.js';
import { type Transport } from '../transport.js';
import { type BusProvider, toProviders } from '../../provider.js';
import { Telemetry } from '../../telemetry/telemetry.js';
import { Instrumentation } from '../../telemetry/instrumentation.js';

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
  private instrumentation?: Instrumentation;
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
   * Stores the record of one `publish` call. To make the event part of the
   * caller's transaction, join it here, e.g. through `AsyncLocalStorage`.
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

  public async publish(event: object, options?: PublishOptions): Promise<void> {
    const record = this.serializer.serialize(event, options);

    await this.getInstrumentation().create(
      { system: 'outbox' },
      {
        id: record.id,
        name: record.name,
        headers: record.headers,
        bodySize: Buffer.byteLength(record.data),
      },
      () => this.save(record),
    );
  }

  public subscribe<
    C extends EventConstructor<any>,
    E extends InstanceType<C> = InstanceType<C>,
  >(event: C, handler: string, callback: EventCallback<E>): Promise<void> {
    return this.transport.subscribe(event, handler, callback);
  }

  protected async dispatch(records: OutboxRecord[]): Promise<void> {
    await Promise.all(
      records.map((record) => {
        const { event, meta, publishedAt } = this.serializer.deserialize(
          Object,
          record.data,
        );

        return this.transport.publish(event, {
          id: record.id,
          name: record.name,
          meta,
          publishedAt,
          headers: record.headers,
        });
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

  private getInstrumentation(): Instrumentation {
    // not made in the constructor: the telemetry is injected as a property
    this.instrumentation ??= new Instrumentation(this.telemetry);

    return this.instrumentation;
  }
}
