import { context, type Tracer } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import {
  InMemoryLogRecordExporter,
  LoggerProvider,
  SimpleLogRecordProcessor,
  type ReadableLogRecord,
} from '@opentelemetry/sdk-logs';
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
  type DataPoint,
} from '@opentelemetry/sdk-metrics';
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
  type ReadableSpan,
} from '@opentelemetry/sdk-trace-base';
import { BusOtel, type BusOtelOptions } from './bus-otel.js';

export interface OtelFixture {
  telemetry: BusOtel;
  tracer: Tracer;
  spans(): ReadableSpan[];
  /** The finished span of the name. */
  span(name: string): ReadableSpan;
  logs(): ReadableLogRecord[];
  /** Data points of a metric, after flushing. */
  metric(name: string): Promise<DataPoint<unknown>[]>;
  shutdown(): Promise<void>;
}

/** BusOtel with every signal kept in memory, for tests. */
export function createOtelFixture(options?: BusOtelOptions): OtelFixture {
  const contextManager = new AsyncLocalStorageContextManager().enable();

  context.setGlobalContextManager(contextManager);

  const spanExporter = new InMemorySpanExporter();
  const metricExporter = new InMemoryMetricExporter(
    AggregationTemporality.CUMULATIVE,
  );
  const logExporter = new InMemoryLogRecordExporter();
  const tracerProvider = new BasicTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(spanExporter)],
  });
  const meterProvider = new MeterProvider({
    readers: [
      new PeriodicExportingMetricReader({
        exporter: metricExporter,
        exportIntervalMillis: 3_600_000,
      }),
    ],
  });
  const loggerProvider = new LoggerProvider({
    processors: [new SimpleLogRecordProcessor({ exporter: logExporter })],
  });
  const telemetry = new BusOtel({
    tracerProvider,
    meterProvider,
    loggerProvider,
    propagator: new W3CTraceContextPropagator(),
    ...options,
  });

  return {
    telemetry,
    tracer: tracerProvider.getTracer('test'),
    spans: () => spanExporter.getFinishedSpans(),
    span(name: string): ReadableSpan {
      const span = spanExporter.getFinishedSpans().find((s) => s.name === name);

      if (!span) {
        throw new Error(
          `No span ${name}, got ${spanExporter
            .getFinishedSpans()
            .map((s) => s.name)
            .join(', ')}`,
        );
      }

      return span;
    },
    logs: () => logExporter.getFinishedLogRecords(),
    /** Data points of a metric, by attributes. */
    async metric(name: string) {
      await meterProvider.forceFlush();

      const points = metricExporter
        .getMetrics()
        .flatMap(({ scopeMetrics }) => scopeMetrics)
        .flatMap(({ metrics }) => metrics)
        .filter(({ descriptor }) => descriptor.name === name);
      const last = points.at(-1);

      return last?.dataPoints ?? [];
    },
    async shutdown() {
      await Promise.all([
        tracerProvider.shutdown(),
        meterProvider.shutdown(),
        loggerProvider.shutdown(),
      ]);
      context.disable();
      contextManager.disable();
    },
  };
}
