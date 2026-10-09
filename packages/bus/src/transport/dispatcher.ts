export interface PublishOptions {
  id?: string;
  meta?: Record<string, string | number | undefined | null>;
  /** Defaults to the current time. */
  publishedAt?: Date;
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
