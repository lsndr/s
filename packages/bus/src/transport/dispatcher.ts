export interface PublishOptions {
  id?: string;
  meta?: Record<string, string | number | undefined | null>;
}

export interface Dispatcher {
  publish<E extends object>(event: E, options?: PublishOptions): Promise<void>;
}

export const Dispatcher = Symbol('Dispatcher');
