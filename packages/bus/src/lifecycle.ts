export interface OnBusBootstrap {
  /** Called once every handler found by the bus is subscribed. */
  onBusBootstrap(): Promise<void> | void;
}

export interface OnBusShutdown {
  /** Called when the bus shuts down. */
  onBusShutdown(): Promise<void> | void;
}

export function hasOnBusBootstrap(value: unknown): value is OnBusBootstrap {
  return (
    typeof (value as Partial<OnBusBootstrap> | null)?.onBusBootstrap ===
    'function'
  );
}

export function hasOnBusShutdown(value: unknown): value is OnBusShutdown {
  return (
    typeof (value as Partial<OnBusShutdown> | null)?.onBusShutdown ===
    'function'
  );
}
