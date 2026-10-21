import { Inject, Logger, Optional, type Type } from '@nestjs/common';
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
import {
  instrumentPublish,
  instrumentTransport,
  describeTransport,
} from '../../telemetry/instrument.js';
import {
  type BusTelemetry,
  createTransportTelemetry,
  type TelemetryDescribed,
  type TelemetryDescriptor,
  type TelemetryInstrumented,
  Telemetry,
  type TransportTelemetry,
} from '../../telemetry/telemetry.js';

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

/** Where the transport an outbox relays to is registered. */
const OutboxTransport = Symbol('OutboxTransport');

/** Where the config of an outbox is registered. */
const OutboxConfigToken = Symbol('OutboxConfig');

/**
 * Base class of transactional outboxes.
 *
 * `publish` only stores the event with {@link Outbox.save}. A background loop
 * calls {@link Outbox.relay}, which forwards the stored events to the
 * configured transport. Subscriptions and health checks go straight to that
 * transport.
 *
 * Provide an outbox with {@link Outbox.withConfig}. The transport it relays to
 * is registered in the bus module next to it, so the bus calls its lifecycle
 * hooks itself, before the ones of the outbox and after them on shutdown.
 *
 * Subclasses that override `onBusBootstrap` or `onBusShutdown` must call
 * `super`.
 */
export abstract class Outbox<C extends OutboxConfig = OutboxConfig>
  implements
    Transport,
    OnBusBootstrap,
    OnBusShutdown,
    TelemetryDescribed,
    TelemetryInstrumented
{
  @Inject(OutboxConfigToken)
  protected config!: C;

  @Inject(OutboxTransport)
  protected transport!: InferTransport<C>;

  @Inject(Serializer)
  protected serializer!: Serializer;

  @Optional()
  @Inject(Telemetry)
  private readonly busTelemetry?: BusTelemetry;

  private readonly logger = new Logger(Outbox.name);
  private transportTelemetry?: TransportTelemetry;
  private abortController?: AbortController;
  private loop?: Promise<void>;

  /**
   * Provides the outbox with the given config, available as `this.config`,
   * and registers the transport it relays to. The outbox is created by Nest,
   * so it can inject its own dependencies.
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

  /** Resolved on first use, as the dependencies are injected after creation. */
  private get telemetry(): TransportTelemetry {
    return (this.transportTelemetry ??= createTransportTelemetry(
      this.busTelemetry,
      () => this.getTelemetryDescriptor(),
    ));
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

  /**
   * Only publishing is reported, as the creation of the events, which the
   * transport the outbox relays to sends later. That transport is reported
   * like any other, and subscribing is left to it, as the outbox hands the
   * subscriptions over.
   */
  public instrument(telemetry: BusTelemetry): void {
    instrumentPublish(this, telemetry, 'create');
    instrumentTransport(this.transport, telemetry);
  }

  /** Described as the transport the events are relayed to. */
  public getTelemetryDescriptor(): TelemetryDescriptor {
    return describeTransport(this.transport);
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

    await this.save(
      events.map(({ event, options }) =>
        this.serializer.serialize(event, options),
      ),
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
          options: { id: record.id, name: record.name, meta, publishedAt },
        };
      }),
    );
  }

  private async runLoop(signal: AbortSignal): Promise<void> {
    const interval = this.config.interval ?? 1_000;
    const duration = this.telemetry.histogram('outbox.relay.duration', {
      description: 'Duration of a relay of outbox records',
      unit: 's',
    });
    const relayedRecords = this.telemetry.counter('outbox.relayed', {
      description: 'Outbox records relayed to the transport',
      unit: '{record}',
    });
    const failures = this.telemetry.counter('outbox.relay.failures', {
      description: 'Relays of outbox records that failed',
      unit: '{relay}',
    });

    while (!signal.aborted) {
      let relayed = 0;
      const start = performance.now();

      try {
        relayed = await this.relay();
        duration.record((performance.now() - start) / 1_000);
        relayedRecords.add(relayed);
      } catch (err) {
        duration.record((performance.now() - start) / 1_000);
        failures.add(1);
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
