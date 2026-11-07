export interface PublishOptions {
  id?: string;
  /** Event name. Defaults to the class name of the event. */
  name?: string;
  meta?: Record<string, string | number | undefined | null>;
  /** Defaults to the current time. */
  publishedAt?: Date;
  /**
   * Message headers, which travel next to the serialized message. Telemetry
   * uses them to carry the trace context.
   */
  headers?: Record<string, string>;
}

export interface PublishEvent<O extends PublishOptions = PublishOptions> {
  event: object;
  options?: O;
}

export interface Dispatcher {
  publish(events: PublishEvent[]): Promise<void>;
  publish(event: object, options?: PublishOptions): Promise<void>;
}

export const Dispatcher = Symbol('Dispatcher');
