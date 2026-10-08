export interface PublishOptions {
  id?: string;
  meta?: Record<string, string | number | undefined | null>;
  /** Defaults to the current time. */
  publishedAt?: Date;
}

export interface Dispatcher {
  publish<E extends object>(event: E, options?: PublishOptions): Promise<void>;
}

export const Dispatcher = Symbol('Dispatcher');
