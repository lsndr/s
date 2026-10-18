export interface EventPayload<E extends object> {
  id: string;
  event: E;
  meta: Record<string, string | number | undefined | null>;
  publishedAt: Date;
  /**
   * Creation context of the event, set by the telemetry. Empty when
   * telemetry is off, or for events published before it was.
   */
  context?: Record<string, string>;
}

export type EventConstructor<E extends object> = new (...args: any) => E;

export interface EventHandler<E extends object> {
  handle(event: EventPayload<E>): Promise<void> | void;
}
