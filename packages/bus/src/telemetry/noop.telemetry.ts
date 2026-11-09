import {
  type Telemetry,
  type TelemetryBatch,
  type TelemetryDestination,
  type TelemetryMessage,
} from './telemetry.js';

/** Telemetry that observes nothing. It is the default. */
export class NoopTelemetry implements Telemetry {
  public create<T>(
    _destination: TelemetryDestination,
    _message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    return fn();
  }

  public send<T>(
    _destination: TelemetryDestination,
    _message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    return fn();
  }

  public receive<T>(
    _destination: TelemetryDestination,
    fn: (batch: TelemetryBatch) => Promise<T>,
  ): Promise<T> {
    return fn({ setSize: () => undefined });
  }

  public process<T>(
    _destination: TelemetryDestination,
    _message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    return fn();
  }
}
