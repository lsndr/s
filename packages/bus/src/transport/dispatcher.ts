export interface PublishOptions {
  id?: string;
  /** Event name. Defaults to the class name of the event. */
  name?: string;
  meta?: Record<string, string | number | undefined | null>;
  /** Defaults to the current time. */
  publishedAt?: Date;
  headers?: Record<string, string>;
}

export interface Dispatcher {
  publish(event: object, options?: PublishOptions): Promise<void>;
}

export const Dispatcher = Symbol('Dispatcher');
