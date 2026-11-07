import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { context, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import packageJson from '../../../package.json' with { type: 'json' };
import { type TelemetryDestination } from '../telemetry.js';
import { BusOtel } from './otel.telemetry.js';

const destination: TelemetryDestination = {
  system: 'rabbitmq',
  name: 'events',
  consumerGroup: 'group',
  subscription: 'Handler',
  attributes: { 'messaging.rabbitmq.destination.routing_key': 'TestEvent' },
};

describe('BusOtel', () => {
  let exporter: InMemorySpanExporter;
  let provider: BasicTracerProvider;
  let contextManager: AsyncLocalStorageContextManager;
  let telemetry: BusOtel;

  beforeEach(async () => {
    exporter = new InMemorySpanExporter();
    provider = new BasicTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(exporter)],
    });
    contextManager = new AsyncLocalStorageContextManager().enable();
    context.setGlobalContextManager(contextManager);

    telemetry = new BusOtel({
      tracerProvider: provider,
      propagator: new W3CTraceContextPropagator(),
    });
    await telemetry.onBusInit();
  });

  afterEach(async () => {
    context.disable();
    contextManager.disable();
    await provider.shutdown();
  });

  it('sends in a producer span and injects the trace context', async () => {
    // arrange
    const headers: Record<string, string> = {};

    // act
    await telemetry.send(
      destination,
      [{ id: 'id-1', name: 'TestEvent', headers }],
      () => Promise.resolve(),
    );

    // assert
    const [span] = exporter.getFinishedSpans();

    expect(span).toMatchObject({
      name: 'send events',
      kind: SpanKind.PRODUCER,
      attributes: {
        'messaging.system': 'rabbitmq',
        'messaging.operation.name': 'send',
        'messaging.operation.type': 'send',
        'messaging.destination.name': 'events',
        'messaging.message.id': 'id-1',
        'messaging.rabbitmq.destination.routing_key': 'TestEvent',
      },
    });
    expect(headers['traceparent']).toContain(span!.spanContext().spanId);
  });

  it('continues the trace of a stored message', async () => {
    // arrange
    const headers: Record<string, string> = {};
    const message = { id: 'id-1', name: 'TestEvent', headers };

    await telemetry.create({ name: 'outbox' }, [message], () =>
      Promise.resolve(),
    );

    // act
    await telemetry.send(destination, [message], () => Promise.resolve());

    // assert
    const spans = exporter.getFinishedSpans();
    const create = spans.find((s) => s.name === 'create TestEvent')!;
    const send = spans.find((s) => s.name === 'send events')!;

    expect(send.parentSpanContext?.spanId).toBe(create.spanContext().spanId);
    expect(send.spanContext().traceId).toBe(create.spanContext().traceId);
    expect(headers['traceparent']).toContain(send.spanContext().spanId);
  });

  it('processes in a consumer span that continues the sent trace', async () => {
    // arrange
    const headers: Record<string, string> = {};
    const message = { id: 'id-1', name: 'TestEvent', headers };

    await telemetry.send(destination, [message], () => Promise.resolve());

    // act
    await telemetry.process(destination, message, () => Promise.resolve());

    // assert
    const spans = exporter.getFinishedSpans();
    const send = spans.find((s) => s.name === 'send events')!;
    const processed = spans.find((s) => s.name === 'process events')!;

    expect(processed.kind).toBe(SpanKind.CONSUMER);
    expect(processed.parentSpanContext?.spanId).toBe(send.spanContext().spanId);
    expect(processed.spanContext().traceId).toBe(send.spanContext().traceId);
    expect(processed.attributes).toMatchObject({
      'messaging.operation.type': 'process',
      'messaging.consumer.group.name': 'group',
      'messaging.destination.subscription.name': 'Handler',
    });
  });

  it('runs the processed function inside the process span', async () => {
    // arrange
    let active: string | undefined;

    // act
    await telemetry.process(
      destination,
      { id: 'id-1', name: 'TestEvent', headers: {} },
      () => {
        active = trace.getSpan(context.active())?.spanContext().spanId;

        return Promise.resolve();
      },
    );

    // assert
    expect(active).toBe(exporter.getFinishedSpans()[0]!.spanContext().spanId);
  });

  it('records a failure of the processed function and rethrows it', async () => {
    // arrange
    const failure = new TypeError('boom');

    // act
    const result = telemetry.process(
      destination,
      { id: 'id-1', name: 'TestEvent', headers: {} },
      () => Promise.reject(failure),
    );

    // assert
    await expect(result).rejects.toBe(failure);

    const [span] = exporter.getFinishedSpans();

    expect(span!.status).toEqual({
      code: SpanStatusCode.ERROR,
      message: 'boom',
    });
    expect(span!.attributes['error.type']).toBe('TypeError');
    expect(span!.events.map((e) => e.name)).toEqual(['exception']);
  });

  it('records a failure of a send on its span', async () => {
    // arrange
    const failure = new Error('nacked');

    // act
    const result = telemetry.send(
      destination,
      [{ id: 'id-1', name: 'TestEvent', headers: {} }],
      () => Promise.reject(failure),
    );

    // assert
    await expect(result).rejects.toBe(failure);
    expect(exporter.getFinishedSpans()[0]!.status.code).toBe(
      SpanStatusCode.ERROR,
    );
  });

  it('names the instrumentation scope after the package', async () => {
    // act
    await telemetry.send(
      destination,
      [{ id: 'id-1', name: 'TestEvent', headers: {} }],
      () => Promise.resolve(),
    );

    // assert
    expect(exporter.getFinishedSpans()[0]!.instrumentationScope).toMatchObject({
      name: packageJson.name,
      version: packageJson.version,
    });
  });

  it('throws when it is not initialized', () => {
    // arrange
    const uninitialized = new BusOtel({});

    // act
    const result = () =>
      uninitialized.process(
        destination,
        { id: 'id-1', name: 'TestEvent', headers: {} },
        () => Promise.resolve(),
      );

    // assert
    expect(result).toThrow('not initialized');
  });
});
