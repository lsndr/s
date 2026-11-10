import type * as OtelApi from '@opentelemetry/api';
import packageJson from '../../../package.json' with { type: 'json' };
import { toMetricAttributes } from '../attributes.js';
import {
  type TelemetryAttributes,
  type TelemetryBatch,
  type TelemetryCounter,
  type TelemetryDestination,
  type TelemetryGauge,
  type TelemetryHistogram,
  type TelemetryHistogramOptions,
  type TelemetryInstrumentOptions,
  type TelemetryMessage,
  type TelemetryMeter,
  type TelemetryObservation,
} from '../telemetry.js';
import { getErrorType } from '../error-type.js';

export interface OtelMeterConfig {
  meterProvider?: OtelApi.MeterProvider;
}

type Operation = 'create' | 'send' | 'receive' | 'process';

/** Boundaries the conventions advise for durations, in seconds. */
const DURATION_BOUNDARIES = [
  0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
];

class OtelBatch implements TelemetryBatch {
  public size = 0;

  public setSize(size: number): void {
    this.size = size;
  }
}

// https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/
export class OtelMeter implements TelemetryMeter {
  private readonly meter: OtelApi.Meter;
  private readonly operationDuration: OtelApi.Histogram;
  private readonly processDuration: OtelApi.Histogram;
  private readonly sentMessages: OtelApi.Counter;
  private readonly consumedMessages: OtelApi.Counter;

  public constructor(api: typeof OtelApi, config: OtelMeterConfig) {
    const meter = (config.meterProvider ?? api.metrics).getMeter(
      packageJson.name,
      packageJson.version,
    );

    this.meter = meter;
    this.operationDuration = meter.createHistogram(
      'messaging.client.operation.duration',
      {
        description: 'Duration of messaging operations the client started.',
        unit: 's',
        advice: { explicitBucketBoundaries: DURATION_BOUNDARIES },
      },
    );
    this.processDuration = meter.createHistogram('messaging.process.duration', {
      description: 'Duration of processing a message.',
      unit: 's',
      advice: { explicitBucketBoundaries: DURATION_BOUNDARIES },
    });
    this.sentMessages = meter.createCounter('messaging.client.sent.messages', {
      description: 'Number of messages the producer tried to send.',
      unit: '{message}',
    });
    this.consumedMessages = meter.createCounter(
      'messaging.client.consumed.messages',
      {
        description: 'Number of messages delivered to the application.',
        unit: '{message}',
      },
    );
  }

  public create<T>(
    destination: TelemetryDestination,
    _message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    // not counted as sent: the message hasn't gone to the broker yet
    return this.measure('create', destination, fn, (attributes, duration) => {
      this.recordOperation('create', attributes, duration);
    });
  }

  public send<T>(
    destination: TelemetryDestination,
    _message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    return this.measure('send', destination, fn, (attributes, duration) => {
      this.recordOperation('send', attributes, duration);
      this.sentMessages.add(1, attributes);
    });
  }

  public receive<T>(
    destination: TelemetryDestination,
    fn: (batch: TelemetryBatch) => Promise<T>,
  ): Promise<T> {
    const batch = new OtelBatch();

    return this.measure(
      'receive',
      destination,
      () => fn(batch),
      (attributes, duration) => {
        this.recordOperation('receive', attributes, duration);

        if (batch.size > 0) {
          this.consumedMessages.add(batch.size, attributes);
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
      this.processDuration.record(duration, attributes);

      // a message of a batch was counted when the batch was received
      if (!message.batch) {
        this.consumedMessages.add(1, attributes);
      }
    });
  }

  public counter(
    name: string,
    options?: TelemetryInstrumentOptions,
  ): TelemetryCounter {
    return this.meter.createCounter(name, options);
  }

  public histogram(
    name: string,
    options: TelemetryHistogramOptions = {},
  ): TelemetryHistogram {
    const { boundaries, ...rest } = options;

    return this.meter.createHistogram(name, {
      ...rest,
      ...(boundaries && { advice: { explicitBucketBoundaries: boundaries } }),
    });
  }

  public gauge(
    name: string,
    options: TelemetryInstrumentOptions,
    observe: () => TelemetryObservation[] | Promise<TelemetryObservation[]>,
  ): TelemetryGauge {
    const gauge = this.meter.createObservableGauge(name, options);
    const callback = async (result: OtelApi.ObservableResult) => {
      for (const { value, attributes } of await observe()) {
        result.observe(value, attributes);
      }
    };

    gauge.addCallback(callback);

    return { close: () => gauge.removeCallback(callback) };
  }

  private async measure<T>(
    operation: Operation,
    destination: TelemetryDestination,
    fn: () => Promise<T>,
    record: (attributes: TelemetryAttributes, duration: number) => void,
  ): Promise<T> {
    const attributes: TelemetryAttributes = {
      // the type stays the generic one when the operation has a custom name
      'messaging.operation.name': destination.operation ?? operation,
      ...toMetricAttributes(destination),
    };
    const start = performance.now();

    try {
      return await fn();
    } catch (error) {
      attributes['error.type'] = getErrorType(error);

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
    this.operationDuration.record(duration, {
      ...attributes,
      'messaging.operation.type': operation,
    });
  }
}
