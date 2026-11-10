import {
  type TelemetryAttributes,
  type TelemetryDestination,
} from './telemetry.js';

/**
 * Attributes of a destination that are safe on a metric. What differs per
 * message or per client is left out, since every value makes a new series.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/
 */
export function toMetricAttributes(
  destination: TelemetryDestination,
): TelemetryAttributes {
  const attributes: TelemetryAttributes = {};

  if (destination.system) {
    attributes['messaging.system.name'] = destination.system;
  }

  if (destination.name) {
    attributes['messaging.destination.name'] = destination.name;
  }

  if (destination.consumerGroup) {
    attributes['messaging.consumer.group.name'] = destination.consumerGroup;
  }

  if (destination.subscription) {
    attributes['messaging.destination.subscription.name'] =
      destination.subscription;
  }

  if (destination.server) {
    attributes['server.address'] = destination.server.address;

    if (destination.server.port !== undefined) {
      attributes['server.port'] = destination.server.port;
    }
  }

  return attributes;
}
