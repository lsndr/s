import { Inject, Logger, Optional } from '@nestjs/common';
import { type OnBusInit } from '../../lifecycle.js';
import { type BusProvider } from '../../provider.js';
import type * as OtelApi from '@opentelemetry/api';
import type * as OtelLogs from '@opentelemetry/api-logs';
import {
  type Attributes,
  type BusTelemetry,
  type ContextCarrier,
  type InstrumentOptions,
  type Observation,
  type ProcessOperation,
  type PublishOperation,
  type TelemetryDescriptor,
  type TransportSpanOptions,
  type TransportTelemetry,
} from '../telemetry.js';
import { Attr, DURATION_BUCKETS, ERROR_TYPE_OTHER, Metric } from './semconv.js';

type Api = typeof OtelApi;
type LogsApi = typeof OtelLogs;

const SCOPE = '@lsndr/bus';

const BusOtelConfigToken = Symbol('BusOtelConfig');

export interface BusOtelOptions {
  /** Defaults to the global tracer provider. */
  tracerProvider?: OtelApi.TracerProvider;
  /** Defaults to the global meter provider. */
  meterProvider?: OtelApi.MeterProvider;
  /** Defaults to the global logger provider. */
  loggerProvider?: OtelLogs.LoggerProvider;
  /**
   * Injects and extracts the creation context of messages. Defaults to the
   * global propagator.
   */
  propagator?: OtelApi.TextMapPropagator;
  /**
   * Prefix of the instruments that are not part of the semantic conventions,
   * the ones transports report.
   *
   * @default 'bus'
   */
  namespace?: string;
}

interface Instruments {
  operationDuration: OtelApi.Histogram;
  sentMessages: OtelApi.Counter;
  consumedMessages: OtelApi.Counter;
  processDuration: OtelApi.Histogram;
}

interface State {
  api: Api;
  /** Missing if `@opentelemetry/api-logs` is not installed. */
  logsApi: LogsApi | undefined;
  tracer: OtelApi.Tracer;
  meter: OtelApi.Meter;
  logger: OtelLogs.Logger | undefined;
  instruments: Instruments;
}

interface Operation {
  name: string;
  type?: string;
}

/** Spans, metrics and exception events of the bus, as OpenTelemetry. */
export class BusOtel implements BusTelemetry, OnBusInit {
  private readonly namespace: string;
  private readonly options: BusOtelOptions;
  private readonly logger = new Logger(BusOtel.name);
  private readonly custom = new Map<string, unknown>();
  private readonly pendingGauges: (() => void)[] = [];
  private state: State | undefined;
  private loading: Promise<void> | undefined;

  public constructor(
    @Optional()
    @Inject(BusOtelConfigToken)
    options?: BusOtelOptions,
  ) {
    // Nest passes undefined when the config is not provided
    this.options = options ?? {};
    this.namespace = this.options.namespace ?? 'bus';
  }

  public static withConfig(options: BusOtelOptions = {}): BusProvider<BusOtel> {
    return {
      useClass: BusOtel,
      providers: [{ provide: BusOtelConfigToken, useValue: options }],
    };
  }

  /**
   * Loads OpenTelemetry. `@opentelemetry/api` is required, and
   * `@opentelemetry/api-logs` is needed to report exceptions as log events.
   * Safe to call more than once.
   *
   * @throws If `@opentelemetry/api` is not installed.
   */
  public onBusInit(): Promise<void> {
    return (this.loading ??= this.load());
  }

  public publish<T>(
    operation: PublishOperation,
    fn: (contexts: ContextCarrier[]) => Promise<T>,
  ): Promise<T> {
    return operation.type === 'create'
      ? this.create(operation, fn)
      : this.send(operation, fn);
  }

  public async process<T>(
    operation: ProcessOperation,
    fn: () => Promise<T>,
  ): Promise<T> {
    const { api, tracer, instruments } = this.ready();
    const { descriptor, eventName, handler, delivery } = operation;
    const destination = descriptor.destination?.(eventName, handler);
    const op: Operation = { name: 'process', type: 'process' };
    const metricAttributes = this.metricAttributes(
      descriptor,
      op,
      destination,
      {
        [Attr.subscriptionName]: handler,
      },
    );
    const creation = this.extract(operation.context);
    const span = tracer.startSpan(
      spanName(op.name, destination),
      {
        kind: api.SpanKind.CONSUMER,
        attributes: compact({
          ...metricAttributes,
          [Attr.messageId]: operation.messageId,
          [Attr.clientId]: descriptor.clientId,
          [`${this.namespace}.delivery.attempt`]: delivery?.attempt,
          ...descriptor.attributes?.(eventName, handler),
          ...delivery?.attributes,
        }),
        links: creation ? [{ context: creation }] : [],
      },
      api.context.active(),
    );

    instruments.consumedMessages.add(1, metricAttributes);

    return this.observe(
      [span],
      fn,
      (errorType, seconds) =>
        instruments.processDuration.record(
          seconds,
          compact({ ...metricAttributes, [Attr.errorType]: errorType }),
        ),
      (error, errorType) =>
        this.emitException('process', 'ERROR', error, span, {
          ...metricAttributes,
          [Attr.errorType]: errorType,
        }),
    );
  }

  /**
   * Transports ask for their instruments as soon as they are created, which is
   * before the application bootstraps and this is initialized. What they
   * report until then is dropped, and gauges are registered once it is.
   */
  public transport(descriptor: TelemetryDescriptor): TransportTelemetry {
    const withSystem = (attributes?: Attributes) =>
      compact({ [Attr.system]: descriptor.system, ...attributes });

    return {
      span: (options, fn) =>
        this.state ? this.transportSpan(descriptor, options, fn) : fn(),
      counter: (name, options) => ({
        add: (value, attributes) =>
          this.customInstrument(`counter:${name}`, (state) =>
            state.meter.createCounter(`${this.namespace}.${name}`, options),
          )?.add(value, withSystem(attributes)),
      }),
      histogram: (name, options) => ({
        record: (value, attributes) =>
          this.customInstrument(`histogram:${name}`, (state) =>
            state.meter.createHistogram(`${this.namespace}.${name}`, options),
          )?.record(value, withSystem(attributes)),
      }),
      gauge: (name, options, observe) => {
        const register = () =>
          this.observeGauge(descriptor, name, options, observe);

        if (this.state) {
          register();
        } else {
          this.pendingGauges.push(register);
        }
      },
    };
  }

  /**
   * Creates the messages: one `create` span for each, which is their creation
   * context. Used for messages that are stored to be sent later.
   */
  private async create<T>(
    operation: PublishOperation,
    fn: (contexts: ContextCarrier[]) => Promise<T>,
  ): Promise<T> {
    const { api, tracer, instruments } = this.ready();
    const { descriptor, messages } = operation;
    const op: Operation = { name: 'create', type: 'create' };
    const spans: OtelApi.Span[] = [];
    const contexts: ContextCarrier[] = [];
    const metricAttributes: OtelApi.Attributes[] = [];

    for (const message of messages) {
      // A creation context that exists already is left as it is
      if (message.context) {
        contexts.push(message.context);
        continue;
      }

      const destination = descriptor.destination?.(message.name);
      const attributes = this.metricAttributes(descriptor, op, destination);
      const span = tracer.startSpan(
        spanName(op.name, destination),
        {
          kind: api.SpanKind.PRODUCER,
          attributes: compact({
            ...attributes,
            [Attr.messageId]: message.id,
            [Attr.clientId]: descriptor.clientId,
            ...descriptor.attributes?.(message.name),
          }),
        },
        api.context.active(),
      );

      spans.push(span);
      metricAttributes.push(attributes);
      contexts.push(this.inject(api.trace.setSpan(api.context.active(), span)));
    }

    return this.observe(
      spans,
      () => fn(contexts),
      (errorType, seconds) => {
        for (const attributes of metricAttributes) {
          instruments.operationDuration.record(
            seconds,
            compact({ ...attributes, [Attr.errorType]: errorType }),
          );
        }
      },
      (error, errorType) => {
        for (const [index, span] of spans.entries()) {
          this.emitException('create', 'WARN', error, span, {
            ...metricAttributes[index],
            [Attr.errorType]: errorType,
          });
        }
      },
    );
  }

  /**
   * Sends the messages with one `send` span. A message without a creation
   * context gets one: the `send` span itself if it is the only message, a
   * `create` span otherwise. The `send` span links to the creation context of
   * every message.
   */
  private async send<T>(
    operation: PublishOperation,
    fn: (contexts: ContextCarrier[]) => Promise<T>,
  ): Promise<T> {
    const { api, tracer, instruments } = this.ready();
    const { descriptor, messages } = operation;
    const op: Operation = { name: 'send', type: 'send' };
    const [first] = messages;
    const single = messages.length === 1 && first !== undefined;
    const ownContext = single && !first.context;
    const destinations = new Set(
      messages.map(({ name }) => descriptor.destination?.(name)),
    );
    // Only one low-cardinality destination can name the operation
    const destination =
      destinations.size === 1 ? [...destinations][0] : undefined;
    const metricAttributes = this.metricAttributes(descriptor, op, destination);
    const contexts: ContextCarrier[] = [];
    const links: OtelApi.Link[] = [];

    if (!ownContext) {
      for (const message of messages) {
        if (message.context) {
          contexts.push(message.context);

          const linked = this.extract(message.context);

          if (linked) {
            links.push({ context: linked });
          }

          continue;
        }

        const createAttributes = this.metricAttributes(
          descriptor,
          { name: 'create', type: 'create' },
          descriptor.destination?.(message.name),
        );
        const create = tracer.startSpan(
          spanName('create', descriptor.destination?.(message.name)),
          {
            kind: api.SpanKind.PRODUCER,
            attributes: compact({
              ...createAttributes,
              [Attr.messageId]: message.id,
              [Attr.clientId]: descriptor.clientId,
              ...descriptor.attributes?.(message.name),
            }),
          },
          api.context.active(),
        );

        contexts.push(
          this.inject(api.trace.setSpan(api.context.active(), create)),
        );
        links.push({ context: create.spanContext() });
        create.end();
      }
    }

    const span = tracer.startSpan(
      spanName(op.name, destination),
      {
        kind: ownContext ? api.SpanKind.PRODUCER : api.SpanKind.CLIENT,
        attributes: compact({
          ...metricAttributes,
          [Attr.messageId]: single ? first.id : undefined,
          [Attr.batchCount]: single ? undefined : messages.length,
          [Attr.clientId]: descriptor.clientId,
          ...(single ? descriptor.attributes?.(first.name) : undefined),
        }),
        links,
      },
      api.context.active(),
    );

    if (ownContext) {
      contexts.push(this.inject(api.trace.setSpan(api.context.active(), span)));
    }

    return this.observe(
      [span],
      () => fn(contexts),
      (errorType, seconds) => {
        const attributes = compact({
          ...metricAttributes,
          [Attr.errorType]: errorType,
        });

        instruments.operationDuration.record(seconds, attributes);
        instruments.sentMessages.add(messages.length, attributes);
      },
      (error, errorType) =>
        this.emitException('send', 'WARN', error, span, {
          ...metricAttributes,
          [Attr.errorType]: errorType,
        }),
    );
  }

  private transportSpan<T>(
    descriptor: TelemetryDescriptor,
    options: TransportSpanOptions,
    fn: () => Promise<T>,
  ): Promise<T> {
    const { api, tracer, instruments } = this.ready();
    const { operation, destination } = options;

    if (!operation) {
      return this.observe(
        [
          tracer.startSpan(
            options.name,
            {
              kind: api.SpanKind.INTERNAL,
              attributes: compact({
                [Attr.system]: descriptor.system,
                ...options.attributes,
              }),
            },
            api.context.active(),
          ),
        ],
        fn,
        () => undefined,
      );
    }

    const metricAttributes = this.metricAttributes(
      descriptor,
      operation,
      destination,
    );
    const span = tracer.startSpan(
      spanName(operation.name, destination),
      {
        kind: api.SpanKind.CLIENT,
        attributes: compact({
          ...metricAttributes,
          [Attr.clientId]: descriptor.clientId,
          ...options.attributes,
        }),
      },
      api.context.active(),
    );

    return this.observe(
      [span],
      fn,
      (errorType, seconds) =>
        instruments.operationDuration.record(
          seconds,
          compact({ ...metricAttributes, [Attr.errorType]: errorType }),
        ),
      (error, errorType) =>
        this.emitException(operation.name, 'WARN', error, span, {
          ...metricAttributes,
          [Attr.errorType]: errorType,
        }),
    );
  }

  private observeGauge(
    descriptor: TelemetryDescriptor,
    name: string,
    options: InstrumentOptions,
    observe: () => Observation[],
  ): void {
    const gauge = this.ready().meter.createObservableGauge(
      `${this.namespace}.${name}`,
      options,
    );

    gauge.addCallback((result) => {
      for (const { value, attributes } of observe()) {
        result.observe(
          value,
          compact({ [Attr.system]: descriptor.system, ...attributes }),
        );
      }
    });
  }

  /**
   * One instrument for each name, however many transports use it. None until
   * this is initialized.
   */
  private customInstrument<I>(
    key: string,
    create: (state: State) => I,
  ): I | undefined {
    const { state } = this;

    if (!state) {
      return undefined;
    }

    let instrument = this.custom.get(key) as I | undefined;

    if (!instrument) {
      instrument = create(state);
      this.custom.set(key, instrument);
    }

    return instrument;
  }

  /**
   * Runs `fn` with the spans' context, sets the spans' status and ends them
   * once it settles.
   *
   * @param record - Records the metrics, with the type of the error if `fn`
   * failed and the duration in seconds.
   * @param onError - Reports the failure of `fn`.
   */
  private async observe<T>(
    spans: OtelApi.Span[],
    fn: () => Promise<T>,
    record: (errorType: string | undefined, seconds: number) => void,
    onError?: (error: unknown, errorType: string) => void,
  ): Promise<T> {
    const { api } = this.ready();
    const start = performance.now();
    let errorType: string | undefined;

    try {
      const [span] = spans;

      return await (spans.length === 1 && span
        ? api.context.with(api.trace.setSpan(api.context.active(), span), fn)
        : fn());
    } catch (error) {
      errorType = getErrorType(error);

      for (const span of spans) {
        span.setAttribute(Attr.errorType, errorType);
        span.setStatus({
          code: api.SpanStatusCode.ERROR,
          message: error instanceof Error ? error.message : undefined,
        });
      }

      onError?.(error, errorType);

      throw error;
    } finally {
      for (const span of spans) {
        span.end();
      }

      record(errorType, (performance.now() - start) / 1_000);
    }
  }

  /**
   * Reports an exception as the log event the conventions define, tied to the
   * span.
   */
  private emitException(
    operation: string,
    severity: 'WARN' | 'ERROR',
    error: unknown,
    span: OtelApi.Span,
    attributes: OtelApi.Attributes,
  ): void {
    const { api, logsApi, logger } = this.ready();

    if (!logsApi || !logger) {
      return;
    }

    logger.emit({
      eventName: `messaging.${operation}.exception`,
      severityNumber: logsApi.SeverityNumber[severity],
      severityText: severity,
      context: api.trace.setSpan(api.context.active(), span),
      attributes: compact({
        ...attributes,
        [Attr.exceptionType]: getErrorType(error),
        [Attr.exceptionMessage]:
          error instanceof Error ? error.message : String(error),
        [Attr.exceptionStacktrace]:
          error instanceof Error ? error.stack : undefined,
      }),
    });
  }

  /** Attributes of the operation that metrics have too. */
  private metricAttributes(
    descriptor: TelemetryDescriptor,
    operation: Operation,
    destination: string | undefined,
    extra?: Attributes,
  ): OtelApi.Attributes {
    return compact({
      [Attr.system]: descriptor.system,
      [Attr.operationName]: operation.name,
      [Attr.operationType]: operation.type,
      [Attr.destinationName]: destination,
      [Attr.consumerGroup]: descriptor.consumerGroup,
      [Attr.serverAddress]: descriptor.server?.address,
      [Attr.serverPort]: descriptor.server?.port,
      ...extra,
    });
  }

  private inject(context: OtelApi.Context): ContextCarrier {
    const { api } = this.ready();
    const carrier: ContextCarrier = {};

    (this.options.propagator ?? api.propagation).inject(
      context,
      carrier,
      api.defaultTextMapSetter,
    );

    return carrier;
  }

  /** The span context of a creation context, if it holds a valid one. */
  private extract(
    carrier: ContextCarrier | undefined,
  ): OtelApi.SpanContext | undefined {
    if (!carrier || Object.keys(carrier).length === 0) {
      return undefined;
    }

    const { api } = this.ready();
    const context = (this.options.propagator ?? api.propagation).extract(
      api.ROOT_CONTEXT,
      carrier,
      api.defaultTextMapGetter,
    );
    const spanContext = api.trace.getSpanContext(context);

    return spanContext && api.trace.isSpanContextValid(spanContext)
      ? spanContext
      : undefined;
  }

  private async load(): Promise<void> {
    const api = await import('@opentelemetry/api').catch((cause: unknown) => {
      throw new Error(
        'BusOtel needs @opentelemetry/api. Install it with `npm install @opentelemetry/api`',
        { cause },
      );
    });
    const logsApi = await import('@opentelemetry/api-logs').catch(() => {
      this.logger.warn(
        'Exceptions are not reported: @opentelemetry/api-logs is not installed',
      );

      return undefined;
    });
    const meter = (this.options.meterProvider ?? api.metrics).getMeter(SCOPE);

    this.state = {
      api,
      logsApi,
      tracer: (this.options.tracerProvider ?? api.trace).getTracer(SCOPE),
      meter,
      logger: logsApi
        ? (this.options.loggerProvider ?? logsApi.logs).getLogger(SCOPE)
        : undefined,
      instruments: {
        operationDuration: meter.createHistogram(Metric.operationDuration, {
          description: 'Duration of messaging operation initiated by a client',
          unit: 's',
          advice: { explicitBucketBoundaries: DURATION_BUCKETS },
        }),
        sentMessages: meter.createCounter(Metric.sentMessages, {
          description: 'Number of messages producer attempted to send',
          unit: '{message}',
        }),
        consumedMessages: meter.createCounter(Metric.consumedMessages, {
          description: 'Number of messages delivered to the application',
          unit: '{message}',
        }),
        processDuration: meter.createHistogram(Metric.processDuration, {
          description: 'Duration of processing operation',
          unit: 's',
          advice: { explicitBucketBoundaries: DURATION_BUCKETS },
        }),
      },
    };

    for (const register of this.pendingGauges.splice(0)) {
      register();
    }
  }

  private ready(): State {
    if (!this.state) {
      throw new Error('BusOtel is not initialized, call onBusInit() first');
    }

    return this.state;
  }
}

function spanName(operation: string, destination: string | undefined): string {
  return destination ? `${operation} ${destination}` : operation;
}

function getErrorType(error: unknown): string {
  return (error instanceof Error && error.constructor.name) || ERROR_TYPE_OTHER;
}

/** Drops the attributes that are not set. */
function compact(attributes: Record<string, unknown>): OtelApi.Attributes {
  return Object.fromEntries(
    Object.entries(attributes).filter(([, value]) => value !== undefined),
  ) as OtelApi.Attributes;
}
