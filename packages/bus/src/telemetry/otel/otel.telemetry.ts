import type * as OtelApi from '@opentelemetry/api';
import { Inject } from '@nestjs/common';
import packageJson from '../../../package.json' with { type: 'json' };
import { type BusProvider } from '../../provider.js';
import { type OnBusInit } from '../../lifecycle.js';
import {
  type Telemetry,
  type TelemetryMeter,
  type TelemetryTracer,
} from '../telemetry.js';
import { OtelMeter } from './otel.meter.js';
import { OtelTracer } from './otel.tracer.js';

export interface BusOtelConfig {
  /** Defaults to the global tracer provider. */
  tracerProvider?: OtelApi.TracerProvider;
  /** Defaults to the global meter provider. */
  meterProvider?: OtelApi.MeterProvider;
  /** Defaults to the global propagator. */
  propagator?: OtelApi.TextMapPropagator;
}

const BusOtelConfigToken = Symbol('BusOtelConfig');

/**
 * Telemetry that traces and measures the bus with OpenTelemetry, following the
 * messaging semantic conventions. Needs `@opentelemetry/api` to be installed.
 */
export class BusOtel implements Telemetry, OnBusInit {
  private tracer?: OtelTracer;
  private meter?: OtelMeter;

  public constructor(
    @Inject(BusOtelConfigToken) private readonly config: BusOtelConfig,
  ) {}

  public static withConfig(config: BusOtelConfig = {}): BusProvider<BusOtel> {
    return {
      useClass: BusOtel,
      providers: [{ provide: BusOtelConfigToken, useValue: config }],
    };
  }

  public async onBusInit(): Promise<void> {
    const api = await import('@opentelemetry/api');
    const { tracerProvider, meterProvider, propagator } = this.config;

    this.tracer = new OtelTracer(api, { tracerProvider, propagator });
    this.meter = new OtelMeter(
      (meterProvider ?? api.metrics).getMeter(
        packageJson.name,
        packageJson.version,
      ),
    );
  }

  public getTracer(): TelemetryTracer {
    if (!this.tracer) {
      throw new Error('BusOtel is not initialized, call onBusInit first');
    }

    return this.tracer;
  }

  public getMeter(): TelemetryMeter {
    if (!this.meter) {
      throw new Error('BusOtel is not initialized, call onBusInit first');
    }

    return this.meter;
  }
}
