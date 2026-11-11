/** A batch of messages pulled from the broker, which process spans link to. */
export interface TelemetryBatch {
  /** Records how many messages the broker returned. */
  setSize(size: number): void;
}

export interface TelemetryMessage {
  id: string;
  /** Event name. */
  name: string;
  /**
   * Message headers. `create` and `send` inject the trace context into them,
   * `process` extracts it.
   */
  headers: Record<string, string>;
  /** Size of the serialized event in bytes. */
  bodySize?: number;
  /** Number of the delivery, 1 for the first try and 2 for the first retry. */
  attempt?: number;
  /** Attributes specific to the transport that differ per message. */
  attributes?: TelemetryAttributes;
  /** Batch the message was received in. */
  batch?: TelemetryBatch;
}

export interface TelemetryServer {
  address: string;
  port?: number;
}

export interface TelemetryDestination {
  /** Messaging system, e.g. `rabbitmq` or `redis`. */
  system?: string;
  /**
   * What the operation is called, when it isn't the plain `create`, `send`,
   * `receive`, `process` or `settle`, e.g. `reclaim` for a receive that takes
   * over the messages of other consumers, or `ack` for a settle. Its type
   * stays the generic one.
   */
  operation?: string;
  /**
   * Exchange, queue or stream the message goes to or comes from. Left out
   * when there is none, e.g. while an outbox stores the message.
   */
  name?: string;
  consumerGroup?: string;
  /** Name of the handler that consumes the message. */
  subscription?: string;
  /** Identifies the client that sends or consumes the message. */
  clientId?: string;
  /** Broker the message goes through. */
  server?: TelemetryServer;
  /** Attributes specific to the transport. */
  attributes?: TelemetryAttributes;
}

/**
 * Traces how messages are created, sent, received, processed and settled,
 * each operation wrapped around the function that does it.
 */
export interface TelemetryTracer {
  /** Wraps storing a message for a later send, e.g. in an outbox. */
  create<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T>;

  /** Wraps handing a message over to the broker. */
  send<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T>;

  /**
   * Wraps pulling a batch of messages from the broker. `fn` gets the batch
   * before the pull, so it can record the size and hand the batch on to the
   * messages it received.
   */
  receive<T>(
    destination: TelemetryDestination,
    fn: (batch: TelemetryBatch) => Promise<T>,
  ): Promise<T>;

  /** Wraps one handler processing a message. */
  process<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T>;

  /** Wraps telling the broker how a delivery ended, e.g. an ack. */
  settle<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T>;
}

/**
 * Name of a metric or an attribute as its parts, e.g.
 * `['delivery', 'latency']`, for the tracer and the meter to join the
 * way their backend writes names.
 */
export type TelemetryName = readonly string[];

export interface TelemetryAttribute {
  name: TelemetryName;
  value: string | number;
}

export type TelemetryAttributes = readonly TelemetryAttribute[];

export interface TelemetryInstrumentOptions {
  description?: string;
  /** Unit of the values, e.g. `s` or `{message}`. */
  unit?: string;
}

export interface TelemetryHistogramOptions extends TelemetryInstrumentOptions {
  /** Upper bounds of the buckets the values are sorted into. */
  boundaries?: number[];
}

export interface TelemetryCounter {
  add(value: number, attributes?: TelemetryAttributes): void;
}

export interface TelemetryHistogram {
  record(value: number, attributes?: TelemetryAttributes): void;
}

export interface TelemetryObservation {
  value: number;
  attributes?: TelemetryAttributes;
}

export interface TelemetryGauge {
  /** Stops observing. */
  close(): void;
}

/**
 * Makes the instruments the bus records its metrics with. Which metrics there
 * are is up to the bus, how they are named and stored is up to the meter.
 */
export interface TelemetryMeter {
  counter(
    name: TelemetryName,
    options?: TelemetryInstrumentOptions,
  ): TelemetryCounter;

  histogram(
    name: TelemetryName,
    options?: TelemetryHistogramOptions,
  ): TelemetryHistogram;

  /** Calls `observe` for the current values whenever metrics are collected. */
  gauge(
    name: TelemetryName,
    options: TelemetryInstrumentOptions,
    observe: () => TelemetryObservation[] | Promise<TelemetryObservation[]>,
  ): TelemetryGauge;
}

/** Observes the bus with traces and metrics. */
export interface Telemetry {
  getTracer(): TelemetryTracer;
  getMeter(): TelemetryMeter;
}

export const Telemetry = Symbol('Telemetry');
