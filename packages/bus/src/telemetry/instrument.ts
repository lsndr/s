import { v7 as uuidv7 } from 'uuid';
import {
  type PublishEvent,
  type PublishOptions,
} from '../transport/dispatcher.js';
import {
  type EventCallback,
  type Subscription,
} from '../transport/subscriber.js';
import { type Transport } from '../transport/transport.js';
import { type EventConstructor } from '../types.js';
import { readContext, withContext } from './context.js';
import {
  type BusTelemetry,
  type PublishOperation,
  type TelemetryDescribed,
  type TelemetryDescriptor,
  type TelemetryInstrumented,
} from './telemetry.js';

const publishInstrumented = new WeakSet<object>();
const subscribeInstrumented = new WeakSet<object>();

/** How the transport describes itself, or the name of its class. */
export function describeTransport(transport: object): TelemetryDescriptor {
  const { getTelemetryDescriptor } = transport as Partial<TelemetryDescribed>;

  return typeof getTelemetryDescriptor === 'function'
    ? getTelemetryDescriptor.call(transport)
    : { system: transport.constructor.name };
}

/**
 * Makes the transport report publishing and handling events to the telemetry,
 * by replacing its `publish` and `subscribe` with versions that call
 * `telemetry.publish` and `telemetry.process`. Does what the telemetry has
 * hooks for, and nothing twice.
 *
 * A transport that implements {@link TelemetryInstrumented} instruments itself
 * instead.
 */
export function instrumentTransport(
  transport: Transport,
  telemetry: BusTelemetry,
): void {
  const { instrument } = transport as Partial<TelemetryInstrumented>;

  if (typeof instrument === 'function') {
    instrument.call(transport, telemetry);

    return;
  }

  instrumentPublish(transport, telemetry, 'send');
  instrumentSubscribe(transport, telemetry);
}

/**
 * Replaces `publish` with a version that reports publishing as the given
 * operation, if the telemetry reports publishing. The creation context of an
 * event travels in its `meta`.
 */
export function instrumentPublish(
  transport: Transport,
  telemetry: BusTelemetry,
  type: PublishOperation['type'],
): void {
  if (publishInstrumented.has(transport) || !telemetry.publish) {
    return;
  }

  publishInstrumented.add(transport);

  const original: (events: PublishEvent[]) => Promise<void> =
    transport.publish.bind(transport);

  transport.publish = async (
    eventOrEvents: object | PublishEvent[],
    options?: PublishOptions,
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
        descriptor: describeTransport(transport),
        messages: messages.map(({ options }) => ({
          id: options.id,
          name: options.name,
          context: readContext(options.meta),
        })),
      },
      (contexts) =>
        original(
          messages.map(({ event, options }, index) => ({
            event,
            options: {
              ...options,
              meta: withContext(options.meta, contexts[index]),
            },
          })),
        ),
    );
  };
}

/**
 * Replaces `subscribe` with a version that reports each call of a handler as
 * processing a message, if the telemetry reports processing.
 */
export function instrumentSubscribe(
  transport: Transport,
  telemetry: BusTelemetry,
): void {
  if (subscribeInstrumented.has(transport) || !telemetry.process) {
    return;
  }

  subscribeInstrumented.add(transport);

  const original: (subscriptions: Subscription[]) => Promise<void> =
    transport.subscribe.bind(transport);

  transport.subscribe = ((
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
              descriptor: describeTransport(transport),
              eventName: subscription.event.name,
              handler: subscription.handler,
              messageId: payload.id,
              context: readContext(payload.meta),
              delivery,
            },
            async () => subscription.callback(payload, delivery),
          ),
      })),
    );
  }) as Transport['subscribe'];
}
