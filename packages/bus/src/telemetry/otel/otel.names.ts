import type * as OtelApi from '@opentelemetry/api';
import { type TelemetryAttributes, type TelemetryName } from '../telemetry.js';

/**
 * Names of the bus that the semantic conventions have another name for.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/
 */
const CONVENTIONS = new Map([
  // metrics
  ['operation.duration', 'messaging.client.operation.duration'],
  ['process.duration', 'messaging.process.duration'],
  ['sent.messages', 'messaging.client.sent.messages'],
  ['consumed.messages', 'messaging.client.consumed.messages'],
  // attributes
  ['operation.name', 'messaging.operation.name'],
  ['operation.type', 'messaging.operation.type'],
  ['system', 'messaging.system.name'],
  ['destination.name', 'messaging.destination.name'],
  ['consumer.group.name', 'messaging.consumer.group.name'],
  ['subscription.name', 'messaging.destination.subscription.name'],
]);

/**
 * Namespaces that are exported as they are: the general ones of the
 * conventions, and the ones of a broker, which the conventions put at the
 * root as well (Redis has none, and is written the way RabbitMQ is).
 *
 * @see https://opentelemetry.io/docs/specs/semconv/messaging/rabbitmq/
 */
const ROOT_NAMESPACES = new Set(['server', 'error', 'rabbitmq', 'redis']);

/**
 * Turns the names of the bus into the ones OpenTelemetry exports: the name of
 * the conventions where they have one, and the name of the bus under the
 * prefix where they don't, unless it is in a namespace of its own.
 */
export class OtelNames {
  public constructor(private readonly prefix: string) {}

  public toName(name: TelemetryName): string {
    const joined = name.join('.');
    const convention = CONVENTIONS.get(joined);

    if (convention) {
      return convention;
    }

    if (!this.prefix || ROOT_NAMESPACES.has(name[0] ?? '')) {
      return joined;
    }

    return `${this.prefix}.${joined}`;
  }

  public toAttributes(
    attributes: TelemetryAttributes = [],
  ): OtelApi.Attributes {
    return Object.fromEntries(
      attributes.map(({ name, value }) => [this.toName(name), value]),
    );
  }
}
