/**
 * The hooks below are called by the bus on every provider of the bus module,
 * the transports, the telemetry and the `providers` given to `forRoot`
 * included, in the order they are registered, and in reverse for shutdown. A
 * provider with a request or transient scope is skipped.
 */
export interface OnBusInit {
  /**
   * Called once when the application bootstraps, before the transports are
   * instrumented and the handlers are subscribed. The bus waits for it.
   */
  onBusInit(): Promise<void> | void;
}

export interface OnBusBootstrap {
  /** Called once every handler found by the bus is subscribed. */
  onBusBootstrap(): Promise<void> | void;
}

export interface OnBusShutdown {
  /** Called when the bus shuts down. */
  onBusShutdown(): Promise<void> | void;
}

export function hasOnBusInit(value: unknown): value is OnBusInit {
  return typeof (value as Partial<OnBusInit> | null)?.onBusInit === 'function';
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
