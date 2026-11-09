import type * as OtelApi from '@opentelemetry/api';
import { Inject } from '@nestjs/common';
import packageJson from '../../../package.json' with { type: 'json' };
import { type BusProvider } from '../../provider.js';
import { type OnBusInit } from '../../lifecycle.js';
import {
  type Telemetry,
  type TelemetryBatch,
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

type Operation = 'create' | 'send' | 'receive' | 'process';

/** Keeps the span of a batch, so that process spans can link to it. */
class OtelBatch implements TelemetryBatch {
  public constructor(public readonly span: OtelApi.Span) {}

  public setSize(size: number): void {
    this.span.setAttribute('messaging.batch.message_count', size);
  }
}

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
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    return this.produce('create', destination, message, fn);
  }

  public send<T>(
    destination: TelemetryDestination,
    message: TelemetryMessage,
    fn: () => Promise<T>,
  ): Promise<T> {
    return this.produce('send', destination, message, fn);
  }

  public receive<T>(
    destination: TelemetryDestination,
    fn: (batch: TelemetryBatch) => Promise<T>,
  ): Promise<T> {
    const { context, trace } = this.getApi();
    const parent = context.active();
    const batch = new OtelBatch(
      this.startSpan('receive', destination, undefined, parent),
    );

    return this.run(
      batch.span,
      () => fn(batch),
      trace.setSpan(parent, batch.span),
    );
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
    // a link, not a parent: the producer is what the trace follows, the
    // batch only says which pull delivered the message
    const links =
      message.batch instanceof OtelBatch
        ? [{ context: message.batch.span.spanContext() }]
        : undefined;
    const span = this.startSpan('process', destination, message, parent, links);

    return this.run(span, fn, this.getApi().trace.setSpan(parent, span));
  }

  private async produce<T>(
    operation: 'create' | 'send',
    destination: TelemetryDestination,
    message: TelemetryMessage,
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

    try {
      return await fn();
    } catch (error) {
      this.recordError(span, error);

      throw error;
    } finally {
      span.end();
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
    message: TelemetryMessage | undefined,
    parent: OtelApi.Context,
    links?: OtelApi.Link[],
  ): OtelApi.Span {
    // the type stays the generic one when the operation has a custom name
    const operationName = destination.operation ?? operation;
    // https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/
    const attributes: OtelApi.Attributes = {
      'messaging.operation.name': operationName,
      'messaging.operation.type': operation,
      ...destination.attributes,
    };

    if (message) {
      attributes['messaging.message.id'] = message.id;
      Object.assign(attributes, message.attributes);
    }

    if (destination.name) {
      attributes['messaging.destination.name'] = destination.name;
    }

    if (destination.system) {
      attributes['messaging.system.name'] = destination.system;
    }

    if (destination.clientId) {
      attributes['messaging.client.id'] = destination.clientId;
    }

    if (destination.server) {
      attributes['server.address'] = destination.server.address;

      if (destination.server.port !== undefined) {
        attributes['server.port'] = destination.server.port;
      }
    }

    if (message?.attempt !== undefined) {
      attributes['bus.delivery.attempt'] = message.attempt;
    }

    if (message?.bodySize !== undefined) {
      attributes['messaging.message.body.size'] = message.bodySize;
    }

    if (destination.consumerGroup) {
      attributes['messaging.consumer.group.name'] = destination.consumerGroup;
    }

    if (destination.subscription) {
      attributes['messaging.destination.subscription.name'] =
        destination.subscription;
    }

    return this.getTracer().startSpan(
      this.getSpanName(operationName, operation, destination, message),
      {
        kind: this.getSpanKind(operation),
        attributes,
        links,
      },
      parent,
    );
  }

  private getSpanKind(operation: Operation): OtelApi.SpanKind {
    const { SpanKind } = this.getApi();

    switch (operation) {
      case 'process':
        return SpanKind.CONSUMER;
      case 'receive':
        return SpanKind.CLIENT;
      default:
        return SpanKind.PRODUCER;
    }
  }

  private getSpanName(
    operationName: string,
    operation: Operation,
    destination: TelemetryDestination,
    message: TelemetryMessage | undefined,
  ): string {
    let name = operationName;

    if (operation == 'create' && message) {
      name += ` ${message.name}`;
    } else if (destination.name) {
      name += ` ${destination.name}`;
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
