import type * as OtelApi from '@opentelemetry/api';
import { Inject } from '@nestjs/common';
import packageJson from '../../../package.json' with { type: 'json' };
import { type BusProvider } from '../../provider.js';
import { type OnBusInit } from '../../lifecycle.js';
import {
  type Telemetry,
  type TelemetryDestination,
  type TelemetryMessage,
} from '../telemetry.js';

export interface BusOtelConfig {
  /** Defaults to the global tracer provider. */
  tracerProvider?: OtelApi.TracerProvider;
  /** Defaults to the global propagator. */
  propagator?: OtelApi.TextMapPropagator;
}

const BusOtelConfigToken = Symbol('BusOtelConfig');

type Operation = 'create' | 'send' | 'process';

/**
 * Telemetry that traces the bus with OpenTelemetry, following the messaging
 * semantic conventions. Needs `@opentelemetry/api` to be installed.
 */
export class BusOtel implements Telemetry, OnBusInit {
  private api?: typeof OtelApi;

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
    this.api = await import('@opentelemetry/api');
  }

  public create<T>(
    destination: TelemetryDestination,
    messages: TelemetryMessage[],
    fn: () => Promise<T>,
  ): Promise<T> {
    return this.produce('create', destination, messages, fn);
  }

  public send<T>(
    destination: TelemetryDestination,
    messages: TelemetryMessage[],
    fn: () => Promise<T>,
  ): Promise<T> {
    return this.produce('send', destination, messages, fn);
  }

  public process<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    const { context, propagation, defaultTextMapGetter } = this.getApi();
    const parent = this.getPropagator(propagation).extract(
      context.active(),
      message.headers,
      defaultTextMapGetter,
    );
    const span = this.startSpan('process', destination, message, parent);

    return this.run(span, fn, this.getApi().trace.setSpan(parent, span));
  }

  private async produce<T>(
    operation: 'create' | 'send',
    destination: TelemetryDestination,
    messages: TelemetryMessage[],
    fn: () => Promise<T>,
  ): Promise<T> {
    const {
      context,
      propagation,
      trace,
      defaultTextMapGetter,
      defaultTextMapSetter,
    } = this.getApi();
    const propagator = this.getPropagator(propagation);
    const spans: OtelApi.Span[] = [];

    for (const message of messages) {
      const parent = propagator.extract(
        context.active(),
        message.headers,
        defaultTextMapGetter,
      );
      const span = this.startSpan(operation, destination, message, parent);

      propagator.inject(
        trace.setSpan(parent, span),
        message.headers,
        defaultTextMapSetter,
      );

      spans.push(span);
    }

    try {
      return await fn();
    } catch (error) {
      for (const span of spans) {
        this.recordError(span, error);
      }

      throw error;
    } finally {
      for (const span of spans) {
        span.end();
      }
    }
  }

  private async run<T>(
    span: OtelApi.Span,
    fn: () => Promise<T>,
    ctx: OtelApi.Context,
  ): Promise<T> {
    try {
      return await this.getApi().context.with(ctx, fn);
    } catch (error) {
      this.recordError(span, error);

      throw error;
    } finally {
      span.end();
    }
  }

  private startSpan(
    operation: Operation,
    destination: TelemetryDestination,
    message: TelemetryMessage,
    parent: OtelApi.Context,
  ): OtelApi.Span {
    const { SpanKind } = this.getApi();
    // https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/
    const attributes: OtelApi.Attributes = {
      'messaging.operation.name': operation,
      'messaging.operation.type': operation,
      'messaging.message.id': message.id,
      ...destination.attributes,
    };

    if (destination.name) {
      attributes['messaging.destination.name'] = destination.name;
    }

    if (destination.system) {
      attributes['messaging.system'] = destination.system;
    }

    if (destination.consumerGroup) {
      attributes['messaging.consumer.group.name'] = destination.consumerGroup;
    }

    if (destination.subscription) {
      attributes['messaging.destination.subscription.name'] =
        destination.subscription;
    }

    return this.getTracer().startSpan(
      this.getSpanName(operation, destination, message),
      {
        kind: operation === 'process' ? SpanKind.CONSUMER : SpanKind.PRODUCER,
        attributes,
      },
      parent,
    );
  }

  private getSpanName(
    operation: Operation,
    destination: TelemetryDestination,
    message: TelemetryMessage,
  ): string {
    let name = operation;

    if (operation == 'create') {
      name += ` ${message.name}`;
    } else if (destination.name) {
      name += ` ${destination.name}`;
    }

    if (operation === 'process' && destination.subscription) {
      name += ` (${destination.subscription})`;
    }

    return name;
  }

  private recordError(span: OtelApi.Span, error: unknown): void {
    const { SpanStatusCode } = this.getApi();

    span.recordException(error instanceof Error ? error : String(error));
    span.setAttribute(
      'error.type',
      error instanceof Error ? error.constructor.name : '_OTHER',
    );
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: error instanceof Error ? error.message : String(error),
    });
  }

  private getTracer(): OtelApi.Tracer {
    return (this.config.tracerProvider ?? this.getApi().trace).getTracer(
      packageJson.name,
      packageJson.version,
    );
  }

  private getPropagator(
    propagation: typeof OtelApi.propagation,
  ): OtelApi.TextMapPropagator {
    return this.config.propagator ?? propagation;
  }

  private getApi(): typeof OtelApi {
    if (!this.api) {
      throw new Error('BusOtel is not initialized, call onBusInit first');
    }

    return this.api;
  }
}
