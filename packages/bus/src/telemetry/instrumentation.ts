import { Metrics, type Settlement } from './metrics.js';
import {
  type Telemetry,
  type TelemetryBatch,
  type TelemetryDestination,
  type TelemetryGauge,
  type TelemetryInstrumentOptions,
  type TelemetryMessage,
  type TelemetryName,
  type TelemetryObservableCounter,
  type TelemetryObservation,
} from './telemetry.js';

/** One batch for the transport, which the tracer and the metrics each see as theirs. */
class InstrumentedBatch implements TelemetryBatch {
  public constructor(
    public readonly traced: TelemetryBatch,
    public readonly metered: TelemetryBatch,
  ) {}

  public setSize(size: number): void {
    this.traced.setSize(size);
    this.metered.setSize(size);
  }
}

/**
 * Traces and measures the operations of a transport together, so that it
 * wraps an operation once. The tracer is asked for on every call, because a
 * telemetry may only have it once the bus is initialized.
 */
export class Instrumentation {
  private readonly metrics: Metrics;

  public constructor(private readonly telemetry: Telemetry) {
    this.metrics = new Metrics(telemetry);
  }

  public create<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    const tracer = this.telemetry.getTracer();

    return this.metrics.create(destination, () =>
      tracer.create(destination, message, fn),
    );
  }

  public send<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    const tracer = this.telemetry.getTracer();

    return this.metrics.send(destination, () =>
      tracer.send(destination, message, fn),
    );
  }

  public receive<T>(
    destination: TelemetryDestination,
    fn: (batch: TelemetryBatch) => Promise<T>,
  ): Promise<T> {
    const tracer = this.telemetry.getTracer();

    return this.metrics.receive(destination, (metered) =>
      tracer.receive(destination, (traced) =>
        fn(new InstrumentedBatch(traced, metered)),
      ),
    );
  }

  public process<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    const tracer = this.telemetry.getTracer();
    const { batch } = message;
    const [traced, metered] =
      batch instanceof InstrumentedBatch
        ? [batch.traced, batch.metered]
        : [batch, batch];

    return this.metrics.process(
      destination,
      { ...message, batch: metered },
      () => tracer.process(destination, { ...message, batch: traced }, fn),
    );
  }

  public settle<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    outcome: Settlement,
    fn: () => Promise<T>,
  ): Promise<T> {
    const tracer = this.telemetry.getTracer();

    return this.metrics.settle(destination, outcome, () =>
      tracer.settle(destination, message, fn),
    );
  }

  /** Counts a delivery left for a retry, which sends nothing to the broker. */
  public retry(destination: TelemetryDestination): void {
    this.metrics.retry(destination);
  }

  /** Records how long the message took from its publish to now. */
  public deliver(destination: TelemetryDestination, publishedAt: Date): void {
    this.metrics.deliver(destination, publishedAt);
  }

  /** Wraps one round of an outbox relay, whether it found records or not. */
  public relay<T>(fn: () => Promise<T>): Promise<T> {
    return this.metrics.relay(fn);
  }

  /**
   * Wraps an outbox handing records over to its transport. `publishedAt` is
   * when each record was stored.
   */
  public dispatch<T>(publishedAt: Date[], fn: () => Promise<T>): Promise<T> {
    return this.metrics.dispatch(publishedAt, fn);
  }

  public gauge(
    name: TelemetryName,
    options: TelemetryInstrumentOptions,
    observe: () => TelemetryObservation[] | Promise<TelemetryObservation[]>,
  ): TelemetryGauge {
    return this.metrics.gauge(name, options, observe);
  }

  public observableCounter(
    name: TelemetryName,
    options: TelemetryInstrumentOptions,
    observe: () => TelemetryObservation[] | Promise<TelemetryObservation[]>,
  ): TelemetryObservableCounter {
    return this.metrics.observableCounter(name, options, observe);
  }
}
