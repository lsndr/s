import { Logger } from '@nestjs/common';
import { toMetricAttributes } from './attributes.js';
import { getErrorType } from './error-type.js';
import {
  type Telemetry,
  type TelemetryAttribute,
  type TelemetryAttributes,
  type TelemetryBatch,
  type TelemetryCounter,
  type TelemetryDestination,
  type TelemetryGauge,
  type TelemetryHistogram,
  type TelemetryInstrumentOptions,
  type TelemetryMessage,
  type TelemetryMeter,
  type TelemetryName,
  type TelemetryObservation,
} from './telemetry.js';

/** How a delivery ended: handled, left for a retry, or given up on. */
export type Settlement = 'acked' | 'retried' | 'dropped';

type Operation = 'create' | 'send' | 'receive' | 'process';

/** Boundaries the messaging conventions advise for durations, in seconds. */
const DURATION_BOUNDARIES = [
  0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
];

/** Wider than the ones for durations: a backlog delays messages for long. */
const LATENCY_BOUNDARIES = [
  0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 600,
  1800, 3600,
];

const OUTBOX: TelemetryAttributes = [{ name: ['system'], value: 'outbox' }];

function errorOf(error: unknown): TelemetryAttribute {
  return { name: ['error', 'type'], value: getErrorType(error) };
}

class MeteredBatch implements TelemetryBatch {
  public size = 0;

  public setSize(size: number): void {
    this.size = size;
  }
}

interface Instruments {
  meter: TelemetryMeter;
  operationDuration: TelemetryHistogram;
  processDuration: TelemetryHistogram;
  sent: TelemetryCounter;
  consumed: TelemetryCounter;
  settled: TelemetryCounter;
  latency: TelemetryHistogram;
  relayDuration: TelemetryHistogram;
  relayed: TelemetryCounter;
  relayLatency: TelemetryHistogram;
}

/**
 * Every metric of the bus, named in its own vocabulary: a telemetry maps the
 * names to the ones its backend has a convention for. The meter is asked for
 * on every call, because a telemetry may only have it once the bus is
 * initialized.
 */
export class Metrics {
  private readonly logger = new Logger(Metrics.name);
  private instruments?: Instruments;

  public constructor(private readonly telemetry: Telemetry) {}

  public create<T>(
    destination: TelemetryDestination,
    fn: () => Promise<T>,
  ): Promise<T> {
    // not counted as sent: the message hasn't gone to the broker yet
    return this.measure('create', destination, fn, (attributes, duration) => {
      this.recordOperation('create', attributes, duration);
    });
  }

  public send<T>(
    destination: TelemetryDestination,
    fn: () => Promise<T>,
  ): Promise<T> {
    return this.measure('send', destination, fn, (attributes, duration) => {
      this.recordOperation('send', attributes, duration);
      this.record(({ sent }) => sent.add(1, attributes));
    });
  }

  public receive<T>(
    destination: TelemetryDestination,
    fn: (batch: TelemetryBatch) => Promise<T>,
  ): Promise<T> {
    const batch = new MeteredBatch();

    return this.measure(
      'receive',
      destination,
      () => fn(batch),
      (attributes, duration) => {
        this.recordOperation('receive', attributes, duration);

        if (batch.size > 0) {
          this.record(({ consumed }) => consumed.add(batch.size, attributes));
        }
      },
    );
  }

  public process<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    return this.measure('process', destination, fn, (attributes, duration) => {
      this.record(({ processDuration, consumed }) => {
        processDuration.record(duration, attributes);

        // a message of a batch was counted when the batch was received
        if (!message.batch) {
          consumed.add(1, attributes);
        }
      });
    });
  }

  public settle(destination: TelemetryDestination, outcome: Settlement): void {
    this.record(({ settled }) =>
      settled.add(1, [
        ...toMetricAttributes(destination),
        { name: ['settlement', 'outcome'], value: outcome },
      ]),
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

  /** Wraps one round of an outbox relay, whether it found records or not. */
  public async relay<T>(fn: () => Promise<T>): Promise<T> {
    const start = performance.now();
    const attributes = [...OUTBOX];

    try {
      return await fn();
    } catch (error) {
      attributes.push(errorOf(error));

      throw error;
    } finally {
      const duration = (performance.now() - start) / 1000;

      this.record(({ relayDuration }) =>
        relayDuration.record(duration, attributes),
      );
    }
  }

  /**
   * Wraps an outbox handing records over to its transport. `publishedAt` is
   * when each record was stored.
   */
  public async dispatch<T>(
    publishedAt: Date[],
    fn: () => Promise<T>,
  ): Promise<T> {
    const attributes = [...OUTBOX];

    try {
      const result = await fn();
      const now = Date.now();

      this.record(({ relayLatency }) => {
        for (const date of publishedAt) {
          // the clock that stored the record can be ahead of this one
          relayLatency.record(Math.max(0, now - date.getTime()) / 1000, OUTBOX);
        }
      });

      return result;
    } catch (error) {
      attributes.push(errorOf(error));

      throw error;
    } finally {
      this.record(({ relayed }) => relayed.add(publishedAt.length, attributes));
    }
  }

  public gauge(
    name: TelemetryName,
    options: TelemetryInstrumentOptions,
    observe: () => TelemetryObservation[] | Promise<TelemetryObservation[]>,
  ): TelemetryGauge {
    return this.telemetry.getMeter().gauge(name, options, observe);
  }

  private async measure<T>(
    operation: Operation,
    destination: TelemetryDestination,
    fn: () => Promise<T>,
    record: (attributes: TelemetryAttributes, duration: number) => void,
  ): Promise<T> {
    const attributes: TelemetryAttribute[] = [
      {
        name: ['operation', 'name'],
        // a custom name of the operation replaces the generic one
        value: destination.operation ?? operation,
      },
      ...toMetricAttributes(destination),
    ];
    const start = performance.now();

    try {
      return await fn();
    } catch (error) {
      attributes.push(errorOf(error));

      throw error;
    } finally {
      record(attributes, (performance.now() - start) / 1000);
    }
  }

  /** Only the operation duration has the type among its attributes. */
  private recordOperation(
    operation: Operation,
    attributes: TelemetryAttributes,
    duration: number,
  ): void {
    this.record(({ operationDuration }) =>
      operationDuration.record(duration, [
        ...attributes,
        { name: ['operation', 'type'], value: operation },
      ]),
    );
  }

  /**
   * Never throws: metrics are recorded around the work of the bus, where a
   * failing meter would change its result or how a message is settled.
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
        operationDuration: meter.histogram(['operation', 'duration'], {
          description: 'Duration of messaging operations the client started.',
          unit: 's',
          boundaries: DURATION_BOUNDARIES,
        }),
        processDuration: meter.histogram(['process', 'duration'], {
          description: 'Duration of processing a message.',
          unit: 's',
          boundaries: DURATION_BOUNDARIES,
        }),
        sent: meter.counter(['sent', 'messages'], {
          description: 'Number of messages the producer tried to send.',
          unit: '{message}',
        }),
        consumed: meter.counter(['consumed', 'messages'], {
          description: 'Number of messages delivered to the application.',
          unit: '{message}',
        }),
        settled: meter.counter(['settled', 'messages'], {
          description: 'Number of deliveries by how they ended.',
          unit: '{message}',
        }),
        latency: meter.histogram(['delivery', 'latency'], {
          description:
            'Time from publishing a message to the start of its processing.',
          unit: 's',
          boundaries: LATENCY_BOUNDARIES,
        }),
        relayDuration: meter.histogram(['outbox', 'relay', 'duration'], {
          description: 'Duration of one round of an outbox relay.',
          unit: 's',
          boundaries: DURATION_BOUNDARIES,
        }),
        relayed: meter.counter(['outbox', 'relayed', 'messages'], {
          description:
            'Number of records an outbox tried to hand to its transport.',
          unit: '{message}',
        }),
        relayLatency: meter.histogram(['outbox', 'relay', 'latency'], {
          description:
            'Time a record waited in an outbox before it was relayed.',
          unit: 's',
          boundaries: LATENCY_BOUNDARIES,
        }),
      };
    }

    return this.instruments;
  }
}
