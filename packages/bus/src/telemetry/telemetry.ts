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
  /** Attributes specific to the transport that differ per message. */
  attributes?: Record<string, string | number>;
}

export interface TelemetryServer {
  address: string;
  port?: number;
}

export interface TelemetryDestination {
  /** Messaging system, e.g. `rabbitmq` or `redis`. */
  system?: string;
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
  attributes?: Record<string, string | number>;
}

/** Observes how messages are created, sent and processed. */
export interface Telemetry {
  /** Wraps storing a message for a later send, e.g. in an outbox. */
  create<T>(
    destination: TelemetryDestination,
    messages: TelemetryMessage[],
    fn: () => Promise<T>,
  ): Promise<T>;

  /** Wraps handing messages over to the broker. */
  send<T>(
    destination: TelemetryDestination,
    messages: TelemetryMessage[],
    fn: () => Promise<T>,
  ): Promise<T>;

  /** Wraps one handler processing a message. */
  process<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T>;
}

export const Telemetry = Symbol('Telemetry');
