import {
  type PublishEvent,
  type PublishOptions,
  type Dispatcher,
} from './dispatcher.js';
import { type EventConstructor } from '../types.js';
import {
  type EventCallback,
  type Subscriber,
  type Subscription,
} from './subscriber.js';
import { type Health } from './health.js';
import { Inject, Optional, type Type } from '@nestjs/common';
import { v7 as uuidv7 } from 'uuid';
import {
  type BusTelemetry,
  noopTransportTelemetry,
  type PublishOperation,
  Telemetry,
  type TelemetryDescriptor,
  type TransportTelemetry,
} from '../telemetry/telemetry.js';

export const TransportConfig = Symbol('TransportConfig');

export interface TransportPublishOptions extends PublishOptions {
  /** Event name. Defaults to the class name of the event. */
  name?: string;
  /** Creation context of the event, set by the telemetry. */
  context?: Record<string, string>;
}

export abstract class Transport<TransportConfig = unknown>
  implements Dispatcher, Subscriber, Health
{
  @Inject(TransportConfig)
  public config!: TransportConfig;

  @Optional()
  @Inject(Telemetry)
  private readonly busTelemetry?: BusTelemetry;

  private transportTelemetry?: TransportTelemetry;
  private publishInstrumented = false;
  private subscribeInstrumented = false;

  /**
   * What the transport reports its own details through. Does nothing when the
   * bus has no telemetry.
   */
  protected get telemetry(): TransportTelemetry {
    return (this.transportTelemetry ??=
      this.busTelemetry?.transport(this.getTelemetryDescriptor()) ??
      noopTransportTelemetry);
  }

  public abstract assertHealthy(): Promise<void>;

  public abstract publish(
    events: PublishEvent<TransportPublishOptions>[],
  ): Promise<void>;
  public abstract publish(
    event: object,
    options?: TransportPublishOptions,
  ): Promise<void>;

  public abstract subscribe(subscriptions: Subscription[]): Promise<void>;
  public abstract subscribe<
    C extends EventConstructor<any>,
    E extends InstanceType<C> = InstanceType<C>,
  >(event: C, handler: string, callback: EventCallback<E>): Promise<void>;

  /**
   * Describes the transport to the telemetry. Override it to name the
   * messaging system and the destinations of messages, the default only names
   * the system after the class. Called after the transport is created, so
   * `config` is available.
   */
  public getTelemetryDescriptor(): TelemetryDescriptor {
    return { system: this.constructor.name };
  }

  /**
   * Makes the transport report publishing and handling events to the
   * telemetry, by replacing its `publish` and `subscribe` with versions that
   * call `telemetry.publish` and `telemetry.process`. Called by the bus once
   * all of its transports are created. Does what the telemetry has hooks for,
   * and nothing twice.
   *
   * Override it for a transport that is not reported like a broker is. An
   * `Outbox`, for example, only reports publishing, as the creation of the
   * events.
   */
  public instrument(telemetry: BusTelemetry): void {
    this.instrumentPublish(telemetry, 'send');
    this.instrumentSubscribe(telemetry);
  }

  /**
   * Replaces `publish` with a version that reports publishing as the given
   * operation, if the telemetry reports publishing.
   */
  protected instrumentPublish(
    telemetry: BusTelemetry,
    type: PublishOperation['type'],
  ): void {
    if (this.publishInstrumented || !telemetry.publish) {
      return;
    }

    this.publishInstrumented = true;

    const original: (
      events: PublishEvent<TransportPublishOptions>[],
    ) => Promise<void> = this.publish.bind(this);

    this.publish = async (
      eventOrEvents: object | PublishEvent<TransportPublishOptions>[],
      options?: TransportPublishOptions,
    ): Promise<void> => {
      const events = Array.isArray(eventOrEvents)
        ? eventOrEvents
        : [{ event: eventOrEvents, options }];

      if (events.length === 0) {
        return original(events);
      }

      // The id and the name are settled here, so that the telemetry knows them
      const messages = events.map(({ event, options }) => ({
        event,
        options: {
          ...options,
          id: options?.id ?? uuidv7(),
          name: options?.name ?? event.constructor.name,
        },
      }));

      return telemetry.publish!(
        {
          type,
          descriptor: this.getTelemetryDescriptor(),
          messages: messages.map(({ options }) => ({
            id: options.id,
            name: options.name,
            context: options.context,
          })),
        },
        (contexts) =>
          original(
            messages.map(({ event, options }, index) => ({
              event,
              options: { ...options, context: contexts[index] },
            })),
          ),
      );
    };
  }

  /**
   * Replaces `subscribe` with a version that reports each call of a handler as
   * processing a message, if the telemetry reports processing.
   */
  protected instrumentSubscribe(telemetry: BusTelemetry): void {
    if (this.subscribeInstrumented || !telemetry.process) {
      return;
    }

    this.subscribeInstrumented = true;

    const original: (subscriptions: Subscription[]) => Promise<void> =
      this.subscribe.bind(this);

    this.subscribe = ((
      eventOrSubscriptions: EventConstructor<any> | Subscription[],
      handler?: string,
      callback?: EventCallback<any>,
    ): Promise<void> => {
      const subscriptions = Array.isArray(eventOrSubscriptions)
        ? eventOrSubscriptions
        : [
            {
              event: eventOrSubscriptions,
              handler: handler!,
              callback: callback!,
            },
          ];

      return original(
        subscriptions.map((subscription) => ({
          ...subscription,
          callback: (payload, delivery) =>
            telemetry.process!(
              {
                descriptor: this.getTelemetryDescriptor(),
                eventName: subscription.event.name,
                handler: subscription.handler,
                messageId: payload.id,
                context: payload.context,
                delivery,
              },
              async () => subscription.callback(payload, delivery),
            ),
        })),
      );
    }) as Transport['subscribe'];
  }
}

export type InferTransportConfig<T> =
  T extends Transport<infer C> ? C : unknown;

export interface BusTransport<T extends Transport> {
  provider: Type<T>;
  config: InferTransportConfig<T>;
}
