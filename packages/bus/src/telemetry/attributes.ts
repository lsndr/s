import {
  type TelemetryAttribute,
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
): TelemetryAttribute[] {
  const attributes: TelemetryAttribute[] = [];

  if (destination.system) {
    attributes.push({
      name: ['system'],
      value: destination.system,
    });
  }

  if (destination.name) {
    attributes.push({
      name: ['destination', 'name'],
      value: destination.name,
    });
  }

  if (destination.consumerGroup) {
    attributes.push({
      name: ['consumer', 'group', 'name'],
      value: destination.consumerGroup,
    });
  }

  if (destination.subscription) {
    attributes.push({
      name: ['subscription', 'name'],
      value: destination.subscription,
    });
  }

  if (destination.server) {
    attributes.push({
      name: ['server', 'address'],
      value: destination.server.address,
    });

    if (destination.server.port !== undefined) {
      attributes.push({
        name: ['server', 'port'],
        value: destination.server.port,
      });
    }
  }

  return attributes;
}
