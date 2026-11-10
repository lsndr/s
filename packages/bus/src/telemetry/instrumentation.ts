import { Logger } from '@nestjs/common';
import { toMetricAttributes } from './attributes.js';
import {
  type Telemetry,
  type TelemetryBatch,
  type TelemetryCounter,
  type TelemetryDestination,
  type TelemetryGauge,
  type TelemetryHistogram,
  type TelemetryInstrumentOptions,
  type TelemetryMessage,
  type TelemetryMeter,
  type TelemetryObservation,
} from './telemetry.js';

/** How a delivery ended: handled, left for a retry, or given up on. */
export type Settlement = 'acked' | 'retried' | 'dropped';

/** Wider than the ones for durations: a backlog delays messages for long. */
const LATENCY_BOUNDARIES = [
  0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 600,
  1800, 3600,
];

/** One batch for the transport, which the tracer and the meter each see as theirs. */
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

interface Instruments {
  meter: TelemetryMeter;
  settled: TelemetryCounter;
  latency: TelemetryHistogram;
}

/**
 * Drives the tracer and the meter of a telemetry together, so a transport
 * wraps an operation once. They are asked for on every call, because a
 * telemetry may only have them once the bus is initialized.
 */
export class Instrumentation {
  private readonly logger = new Logger(Instrumentation.name);
  private instruments?: Instruments;

  public constructor(private readonly telemetry: Telemetry) {}

  public create<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    const tracer = this.telemetry.getTracer();

    return this.telemetry
      .getMeter()
      .create(destination, message, () =>
        tracer.create(destination, message, fn),
      );
  }

  public send<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    const tracer = this.telemetry.getTracer();

    return this.telemetry
      .getMeter()
      .send(destination, message, () => tracer.send(destination, message, fn));
  }

  public receive<T>(
    destination: TelemetryDestination,
    fn: (batch: TelemetryBatch) => Promise<T>,
  ): Promise<T> {
    const tracer = this.telemetry.getTracer();

    return this.telemetry
      .getMeter()
      .receive(destination, (metered) =>
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

    return this.telemetry
      .getMeter()
      .process(destination, { ...message, batch: metered }, () =>
        tracer.process(destination, { ...message, batch: traced }, fn),
      );
  }

  public settle(destination: TelemetryDestination, outcome: Settlement): void {
    this.record(({ settled }) =>
      settled.add(1, {
        ...toMetricAttributes(destination),
        'bus.settlement.outcome': outcome,
      }),
    );
  }

  /** Records how long the message took from its publish to now. */
  public deliver(destination: TelemetryDestination, publishedAt: Date): void {
    // the clock of the publisher can be ahead of this one
    const latency = Math.max(0, Date.now() - publishedAt.getTime()) / 1000;

    this.record((instruments) =>
      instruments.latency.record(latency, toMetricAttributes(destination)),
    );
  }

  public gauge(
    name: string,
    options: TelemetryInstrumentOptions,
    observe: () => TelemetryObservation[] | Promise<TelemetryObservation[]>,
  ): TelemetryGauge {
    return this.telemetry.getMeter().gauge(name, options, observe);
  }

  /**
   * Never throws: these are recorded between the handler and the ack of a
   * message, where a failing meter would change how the message is settled.
   */
  private record(fn: (instruments: Instruments) => void): void {
    try {
      fn(this.getInstruments());
    } catch (err) {
      this.logger.warn(`Failed to record a metric: ${String(err)}`);
    }
  }

  private getInstruments(): Instruments {
    const meter = this.telemetry.getMeter();

    if (this.instruments?.meter !== meter) {
      this.instruments = {
        meter,
        settled: meter.counter('bus.client.settled.messages', {
          description: 'Number of deliveries by how they ended.',
          unit: '{message}',
        }),
        latency: meter.histogram('bus.delivery.latency', {
          description:
            'Time from publishing a message to the start of its processing.',
          unit: 's',
          boundaries: LATENCY_BOUNDARIES,
        }),
      };
    }

    return this.instruments;
  }
}
