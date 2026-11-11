import {
  type Telemetry,
  type TelemetryBatch,
  type TelemetryCounter,
  type TelemetryDestination,
  type TelemetryGauge,
  type TelemetryHistogram,
  type TelemetryMessage,
  type TelemetryMeter,
  type TelemetryTracer,
} from './telemetry.js';

class NoopTracer implements TelemetryTracer {
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

class NoopMeter implements TelemetryMeter {
  public counter(): TelemetryCounter {
    return { add: () => undefined };
  }

  public histogram(): TelemetryHistogram {
    return { record: () => undefined };
  }

  public gauge(): TelemetryGauge {
    return { close: () => undefined };
  }
}

/** Telemetry that observes nothing. It is the default. */
export class NoopTelemetry implements Telemetry {
  private readonly tracer = new NoopTracer();
  private readonly meter = new NoopMeter();

  public getTracer(): TelemetryTracer {
    return this.tracer;
  }

  public getMeter(): TelemetryMeter {
    return this.meter;
  }
}
