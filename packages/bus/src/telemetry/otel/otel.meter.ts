import type * as OtelApi from '@opentelemetry/api';
import packageJson from '../../../package.json' with { type: 'json' };
import {
  type TelemetryAttributes,
  type TelemetryCounter,
  type TelemetryGauge,
  type TelemetryHistogram,
  type TelemetryHistogramOptions,
  type TelemetryInstrumentOptions,
  type TelemetryMeter,
  type TelemetryName,
  type TelemetryObservableCounter,
  type TelemetryObservation,
} from '../telemetry.js';
import { type OtelNames } from './otel.names.js';

export interface OtelMeterConfig {
  names: OtelNames;
  meterProvider?: OtelApi.MeterProvider;
}

export class OtelMeter implements TelemetryMeter {
  private readonly meter: OtelApi.Meter;
  private readonly names: OtelNames;

  public constructor(api: typeof OtelApi, config: OtelMeterConfig) {
    this.names = config.names;
    this.meter = (config.meterProvider ?? api.metrics).getMeter(
      packageJson.name,
      packageJson.version,
    );
  }

  public counter(
    name: TelemetryName,
    options?: TelemetryInstrumentOptions,
  ): TelemetryCounter {
    const counter = this.meter.createCounter(this.names.toName(name), options);

    return {
      add: (value: number, attributes?: TelemetryAttributes) =>
        counter.add(value, this.names.toAttributes(attributes)),
    };
  }

  public histogram(
    name: TelemetryName,
    options: TelemetryHistogramOptions = {},
  ): TelemetryHistogram {
    const { boundaries, ...rest } = options;

    const histogram = this.meter.createHistogram(this.names.toName(name), {
      ...rest,
      ...(boundaries && { advice: { explicitBucketBoundaries: boundaries } }),
    });

    return {
      record: (value: number, attributes?: TelemetryAttributes) =>
        histogram.record(value, this.names.toAttributes(attributes)),
    };
  }

  public gauge(
    name: TelemetryName,
    options: TelemetryInstrumentOptions,
    observe: () => TelemetryObservation[] | Promise<TelemetryObservation[]>,
  ): TelemetryGauge {
    const gauge = this.meter.createObservableGauge(
      this.names.toName(name),
      options,
    );
    const callback = async (result: OtelApi.ObservableResult) => {
      for (const { value, attributes } of await observe()) {
        result.observe(value, this.names.toAttributes(attributes));
      }
    };

    gauge.addCallback(callback);

    return { close: () => gauge.removeCallback(callback) };
  }

  public observableCounter(
    name: TelemetryName,
    options: TelemetryInstrumentOptions,
    observe: () => TelemetryObservation[] | Promise<TelemetryObservation[]>,
  ): TelemetryObservableCounter {
    const counter = this.meter.createObservableCounter(
      this.names.toName(name),
      options,
    );
    const callback = async (result: OtelApi.ObservableResult) => {
      for (const { value, attributes } of await observe()) {
        result.observe(value, this.names.toAttributes(attributes));
      }
    };

    counter.addCallback(callback);

    return { close: () => counter.removeCallback(callback) };
  }
}
