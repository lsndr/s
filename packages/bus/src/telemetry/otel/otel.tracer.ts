import type * as OtelApi from '@opentelemetry/api';
import packageJson from '../../../package.json' with { type: 'json' };
import {
  type TelemetryBatch,
  type TelemetryDestination,
  type TelemetryMessage,
  type TelemetryTracer,
} from '../telemetry.js';
import { getErrorType } from './error-type.js';

export interface OtelTracerConfig {
  tracerProvider?: OtelApi.TracerProvider;
  propagator?: OtelApi.TextMapPropagator;
}

type Operation = 'create' | 'send' | 'receive' | 'process';

/** Keeps the span of a batch, so that process spans can link to it. */
class OtelBatch implements TelemetryBatch {
  public constructor(public readonly span: OtelApi.Span) {}

  public setSize(size: number): void {
    this.span.setAttribute('messaging.batch.message_count', size);
  }
}

export class OtelTracer implements TelemetryTracer {
  public constructor(
    private readonly api: typeof OtelApi,
    private readonly config: OtelTracerConfig,
  ) {}

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
    const { context, trace } = this.api;
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
    const { context, propagation, defaultTextMapGetter } = this.api;
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

    return this.run(span, fn, this.api.trace.setSpan(parent, span));
  }

  private produce<T>(
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
    } = this.api;
    const propagator = this.getPropagator(propagation);
    const parent = propagator.extract(
      context.active(),
      message.headers,
      defaultTextMapGetter,
    );
    const span = this.startSpan(operation, destination, message, parent);

    const ctx = trace.setSpan(parent, span);

    propagator.inject(ctx, message.headers, defaultTextMapSetter);

    return this.run(span, fn, ctx);
  }

  private async run<T>(
    span: OtelApi.Span,
    fn: () => Promise<T>,
    ctx: OtelApi.Context,
  ): Promise<T> {
    try {
      return await this.api.context.with(ctx, fn);
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
    const { SpanKind } = this.api;

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
    const { SpanStatusCode } = this.api;

    span.recordException(error instanceof Error ? error : String(error));
    span.setAttribute('error.type', getErrorType(error));
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: error instanceof Error ? error.message : String(error),
    });
  }

  private getTracer(): OtelApi.Tracer {
    return (this.config.tracerProvider ?? this.api.trace).getTracer(
      packageJson.name,
      packageJson.version,
    );
  }

  private getPropagator(
    propagation: typeof OtelApi.propagation,
  ): OtelApi.TextMapPropagator {
    return this.config.propagator ?? propagation;
  }
}
