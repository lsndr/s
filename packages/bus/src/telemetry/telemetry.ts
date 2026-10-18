export type AttributeValue =
  | string
  | number
  | boolean
  | readonly string[]
  | readonly number[]
  | readonly boolean[];

export type Attributes = Record<string, AttributeValue | undefined>;

/**
 * Fields that carry the creation context of a message, e.g. `traceparent`, as
 * produced by the telemetry. Opaque to the bus, which only stores and forwards
 * it with the message.
 */
export type ContextCarrier = Record<string, string>;

/** What a transport tells the telemetry about itself. */
export interface TelemetryDescriptor {
  /** Messaging system, e.g. `rabbitmq`. */
  system: string;
  /**
   * Destination of a message. Called with the event name and, when the
   * message is consumed, the name of the handler.
   */
  destination?(eventName: string, handler?: string): string;
  /**
   * Transport-specific attributes of a message, for example the routing key.
   * Called like {@link TelemetryDescriptor.destination}.
   */
  attributes?(eventName: string, handler?: string): Attributes;
  /** Consumer group the transport consumes as. */
  consumerGroup?: string;
  /** Address of the broker. */
  server?: { address: string; port?: number };
  clientId?: string;
}

/**
 * What a transport knows about a single delivery, passed to the callback
 * along with the payload.
 */
export interface Delivery {
  /** 1 for the first delivery of a message, 2 for the first retry, … */
  attempt?: number;
  /** Transport-specific attributes of the delivery, e.g. a delivery tag. */
  attributes?: Attributes;
}

export interface PublishMessage {
  id: string;
  name: string;
  /**
   * Creation context the message already has, e.g. when it is relayed from an
   * outbox.
   */
  context?: ContextCarrier;
}

export interface PublishOperation {
  /**
   * `send` for a message handed to the broker. `create` for a message that is
   * only stored to be sent later, as an outbox does.
   */
  type: 'send' | 'create';
  descriptor: TelemetryDescriptor;
  messages: PublishMessage[];
}

export interface ProcessOperation {
  descriptor: TelemetryDescriptor;
  eventName: string;
  /** Name of the handler class. */
  handler: string;
  messageId: string;
  /** Creation context of the message. */
  context?: ContextCarrier;
  delivery?: Delivery;
}

export interface InstrumentOptions {
  description?: string;
  unit?: string;
}

export interface Counter {
  add(value: number, attributes?: Attributes): void;
}

export interface Histogram {
  record(value: number, attributes?: Attributes): void;
}

export interface Observation {
  value: number;
  attributes?: Attributes;
}

export interface TransportSpanOptions {
  /**
   * Name of the span. Ignored when `operation` is set, the span is then named
   * after the operation and the destination.
   */
  name: string;
  /**
   * Makes the span a messaging operation, e.g. `{ name: 'ack', type:
   * 'settle' }`, which is also timed.
   */
  operation?: { name: string; type?: string };
  destination?: string;
  attributes?: Attributes;
}

/**
 * What a transport uses to report its own details. Names of instruments are
 * relative: the telemetry decides on the prefix.
 */
export interface TransportTelemetry {
  span<T>(options: TransportSpanOptions, fn: () => Promise<T>): Promise<T>;
  counter(name: string, options?: InstrumentOptions): Counter;
  histogram(name: string, options?: InstrumentOptions): Histogram;
  /** Reports the observations whenever the telemetry collects them. */
  gauge(
    name: string,
    options: InstrumentOptions,
    observe: () => Observation[],
  ): void;
}

/**
 * Telemetry of the bus. The bus reports what is common to all transports
 * through `publish` and `process`, and transports report what is specific to
 * them through {@link BusTelemetry.transport}.
 *
 * Both `publish` and `process` are optional: what a telemetry leaves out is
 * not reported. Implement `OnBusInit` to set it up when the application
 * bootstraps, before the transports are instrumented. Until then the bus does
 * not report anything to it, except through {@link BusTelemetry.transport},
 * which must not fail.
 */
export interface BusTelemetry {
  /**
   * Wraps the publication of messages.
   *
   * @param fn - Publishes the messages. Receives the creation context of each
   * message, in order, to send along.
   */
  publish?<T>(
    operation: PublishOperation,
    fn: (contexts: ContextCarrier[]) => Promise<T>,
  ): Promise<T>;
  /** Wraps the handling of a message. */
  process?<T>(operation: ProcessOperation, fn: () => Promise<T>): Promise<T>;
  /** Returns what the given transport reports its own details through. */
  transport(descriptor: TelemetryDescriptor): TransportTelemetry;
}

export const Telemetry = Symbol('Telemetry');

const noopInstrument = { add: () => undefined, record: () => undefined };

export const noopTransportTelemetry: TransportTelemetry = {
  span: (_options, fn) => fn(),
  counter: () => noopInstrument,
  histogram: () => noopInstrument,
  gauge: () => undefined,
};
