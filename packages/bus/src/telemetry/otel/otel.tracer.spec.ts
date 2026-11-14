import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { context, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import {
  TracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace';
import packageJson from '../../../package.json' with { type: 'json' };
import {
  type TelemetryDestination,
  type TelemetryTracer,
} from '../telemetry.js';
import { OtelTelemetry } from './otel.telemetry.js';

const destination: TelemetryDestination = {
  system: 'rabbitmq',
  name: 'events',
  consumerGroup: 'group',
  subscription: 'Handler',
  clientId: 'client-1',
  server: { address: 'localhost', port: 5672 },
  attributes: [
    { name: ['rabbitmq', 'destination', 'routing_key'], value: 'TestEvent' },
  ],
};

describe('OtelTelemetry tracer', () => {
  let exporter: InMemorySpanExporter;
  let provider: TracerProvider;
  let contextManager: AsyncLocalStorageContextManager;
  let tracer: TelemetryTracer;

  beforeEach(async () => {
    exporter = new InMemorySpanExporter();
    provider = new TracerProvider({
      spanProcessors: [new SimpleSpanProcessor({ exporter })],
    });
    contextManager = new AsyncLocalStorageContextManager().enable();
    context.setGlobalContextManager(contextManager);

    const telemetry = new OtelTelemetry({
      tracerProvider: provider,
      propagator: new W3CTraceContextPropagator(),
    });

    await telemetry.onBusInit();
    tracer = telemetry.getTracer();
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
    await tracer.send(
      destination,
      { id: 'id-1', name: 'TestEvent', headers },
      () => Promise.resolve(),
    );

    // assert
    const [span] = exporter.getFinishedSpans();

    expect(span).toMatchObject({
      name: 'send events',
      kind: SpanKind.PRODUCER,
      attributes: {
        'messaging.system.name': 'rabbitmq',
        'messaging.operation.name': 'send',
        'messaging.operation.type': 'send',
        'messaging.destination.name': 'events',
        'messaging.message.id': 'id-1',
        'messaging.client.id': 'client-1',
        'server.address': 'localhost',
        'server.port': 5672,
        'rabbitmq.destination.routing_key': 'TestEvent',
      },
    });
    expect(headers['traceparent']).toContain(span!.spanContext().spanId);
  });

  it('adds the attributes of the message', async () => {
    // arrange
    const message = {
      id: 'id-1',
      name: 'TestEvent',
      headers: {},
      attributes: [{ name: ['rabbitmq', 'message', 'delivery_tag'], value: 7 }],
    };

    // act
    await tracer.process(destination, message, () => Promise.resolve());

    // assert
    const [span] = exporter.getFinishedSpans();

    expect(span!.attributes).toMatchObject({
      'rabbitmq.message.delivery_tag': 7,
    });
  });

  it('records the delivery attempt of the message', async () => {
    // arrange
    const message = { id: 'id-1', name: 'TestEvent', headers: {}, attempt: 2 };

    // act
    await tracer.process(destination, message, () => Promise.resolve());

    // assert
    const [span] = exporter.getFinishedSpans();

    expect(span!.attributes['bus.delivery.attempt']).toBe(2);
  });

  it('records the body size of the message', async () => {
    // arrange
    const message = { id: 'id-1', name: 'TestEvent', headers: {}, bodySize: 5 };

    // act
    await tracer.send(destination, message, () => Promise.resolve());

    // assert
    const [span] = exporter.getFinishedSpans();

    expect(span!.attributes['messaging.message.body.size']).toBe(5);
  });

  it('continues the trace of a stored message', async () => {
    // arrange
    const headers: Record<string, string> = {};
    const message = { id: 'id-1', name: 'TestEvent', headers };

    await tracer.create({ system: 'outbox' }, message, () => Promise.resolve());

    // act
    await tracer.send(destination, message, () => Promise.resolve());

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

    await tracer.send(destination, message, () => Promise.resolve());

    // act
    await tracer.process(destination, message, () => Promise.resolve());

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

  it('settles in a client span named after the operation', async () => {
    // arrange
    const message = { id: 'id-1', name: 'TestEvent', headers: {} };

    // act
    await tracer.settle({ ...destination, operation: 'ack' }, message, () =>
      Promise.resolve(),
    );

    // assert
    const [span] = exporter.getFinishedSpans();

    expect(span).toMatchObject({
      name: 'ack events',
      kind: SpanKind.CLIENT,
      attributes: {
        'messaging.operation.name': 'ack',
        'messaging.operation.type': 'settle',
        'messaging.destination.name': 'events',
        'messaging.message.id': 'id-1',
      },
    });
  });

  it('settles in a span that continues the sent trace', async () => {
    // arrange
    const headers: Record<string, string> = {};
    const message = { id: 'id-1', name: 'TestEvent', headers };

    await tracer.send(destination, message, () => Promise.resolve());

    // act
    await tracer.settle(destination, message, () => Promise.resolve());

    // assert
    const spans = exporter.getFinishedSpans();
    const send = spans.find((s) => s.name === 'send events')!;
    const settled = spans.find((s) => s.name === 'settle events')!;

    expect(settled.parentSpanContext?.spanId).toBe(send.spanContext().spanId);
    expect(settled.spanContext().traceId).toBe(send.spanContext().traceId);
    expect(settled.links).toEqual([]);
  });

  it('adds the attributes of the settled message', async () => {
    // arrange
    const message = {
      id: 'id-1',
      name: 'TestEvent',
      headers: {},
      attributes: [{ name: ['rabbitmq', 'message', 'delivery_tag'], value: 7 }],
    };

    // act
    await tracer.settle(destination, message, () => Promise.resolve());

    // assert
    const [span] = exporter.getFinishedSpans();

    expect(span!.attributes).toMatchObject({
      'rabbitmq.message.delivery_tag': 7,
    });
  });

  it('leaves out the message id of a settled message without one', async () => {
    // arrange
    const message = { id: '', name: 'TestEvent', headers: {} };

    // act
    await tracer.settle(destination, message, () => Promise.resolve());

    // assert
    const [span] = exporter.getFinishedSpans();

    expect(span!.attributes).not.toHaveProperty('messaging.message.id');
  });

  it('runs the settled function inside the settle span', async () => {
    // arrange
    let active: string | undefined;

    // act
    await tracer.settle(
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

  it('records a failure of a settlement on its span and rethrows it', async () => {
    // arrange
    const failure = new Error('channel closed');

    // act
    const result = tracer.settle(
      destination,
      { id: 'id-1', name: 'TestEvent', headers: {} },
      () => Promise.reject(failure),
    );

    // assert
    await expect(result).rejects.toBe(failure);

    const [span] = exporter.getFinishedSpans();

    expect(span!.status.code).toBe(SpanStatusCode.ERROR);
    expect(span!.attributes['error.type']).toBe('Error');
  });

  it('names spans after the span target', async () => {
    // arrange
    const message = { id: 'id-1', name: 'TestEvent', headers: {} };

    // act
    await tracer.process(
      { ...destination, span: 'events Handler' },
      message,
      () => Promise.resolve(),
    );

    // assert
    const [span] = exporter.getFinishedSpans();

    expect(span!.name).toBe('process events Handler');
  });

  it('names spans without a destination after the operation', async () => {
    // arrange
    const message = { id: 'id-1', name: 'TestEvent', headers: {} };

    // act
    await tracer.send({ system: 'outbox' }, message, () => Promise.resolve());

    // assert
    const [span] = exporter.getFinishedSpans();

    expect(span!.name).toBe('send');
    expect(span!.attributes).not.toHaveProperty('messaging.destination.name');
  });

  it('runs the processed function inside the process span', async () => {
    // arrange
    let active: string | undefined;

    // act
    await tracer.process(
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

  it('runs the sent function inside the send span', async () => {
    // arrange
    let active: string | undefined;

    // act
    await tracer.send(
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

  it('runs the created function inside the create span', async () => {
    // arrange
    let active: string | undefined;

    // act
    await tracer.create(
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
    const result = tracer.process(
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
    const result = tracer.send(
      destination,
      { id: 'id-1', name: 'TestEvent', headers: {} },
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
    await tracer.send(
      destination,
      { id: 'id-1', name: 'TestEvent', headers: {} },
      () => Promise.resolve(),
    );

    // assert
    expect(exporter.getFinishedSpans()[0]!.instrumentationScope).toMatchObject({
      name: packageJson.name,
      version: packageJson.version,
    });
  });

  it('receives in a client span with the size of the batch', async () => {
    // act
    await tracer.receive(destination, (batch) => {
      batch.setSize(3);

      return Promise.resolve();
    });

    // assert
    const [span] = exporter.getFinishedSpans();

    expect(span).toMatchObject({
      name: 'receive events',
      kind: SpanKind.CLIENT,
      attributes: {
        'messaging.operation.type': 'receive',
        'messaging.destination.name': 'events',
        'messaging.batch.message_count': 3,
      },
    });
  });

  it('names a receive after the custom operation of the destination', async () => {
    // act
    await tracer.receive({ ...destination, operation: 'reclaim' }, () =>
      Promise.resolve(),
    );

    // assert
    expect(exporter.getFinishedSpans()[0]).toMatchObject({
      name: 'reclaim events',
      attributes: {
        'messaging.operation.name': 'reclaim',
        'messaging.operation.type': 'receive',
      },
    });
  });

  it('names a send after the custom operation of the destination', async () => {
    // act
    await tracer.send(
      { ...destination, operation: 'publish' },
      { id: 'id-1', name: 'TestEvent', headers: {} },
      () => Promise.resolve(),
    );

    // assert
    expect(exporter.getFinishedSpans()[0]).toMatchObject({
      name: 'publish events',
      attributes: {
        'messaging.operation.name': 'publish',
        'messaging.operation.type': 'send',
      },
    });
  });

  it('names a process after the custom operation of the destination', async () => {
    // act
    await tracer.process(
      { ...destination, operation: 'handle' },
      { id: 'id-1', name: 'TestEvent', headers: {} },
      () => Promise.resolve(),
    );

    // assert
    expect(exporter.getFinishedSpans()[0]).toMatchObject({
      name: 'handle events',
      attributes: {
        'messaging.operation.name': 'handle',
        'messaging.operation.type': 'process',
      },
    });
  });

  it('names a create after the custom operation of the destination', async () => {
    // act
    await tracer.create(
      { operation: 'store' },
      { id: 'id-1', name: 'TestEvent', headers: {} },
      () => Promise.resolve(),
    );

    // assert
    expect(exporter.getFinishedSpans()[0]).toMatchObject({
      name: 'store TestEvent',
      attributes: {
        'messaging.operation.name': 'store',
        'messaging.operation.type': 'create',
      },
    });
  });

  it('names a receive without a destination after the operation', async () => {
    // act
    await tracer.receive({ ...destination, name: undefined }, () =>
      Promise.resolve(),
    );

    // assert
    expect(exporter.getFinishedSpans()[0]!.name).toBe('receive');
  });

  it('records the error of a failing receive', async () => {
    // arrange
    const error = new Error('boom');

    // act
    const result = tracer.receive(destination, () => Promise.reject(error));

    // assert
    await expect(result).rejects.toBe(error);
    expect(exporter.getFinishedSpans()[0]!.status).toMatchObject({
      code: SpanStatusCode.ERROR,
      message: 'boom',
    });
  });

  it('links the process span to the batch it was received in', async () => {
    // arrange
    const message = { id: 'id-1', name: 'TestEvent', headers: {} };

    // act
    await tracer.receive(destination, (batch) =>
      tracer.process(destination, { ...message, batch }, () =>
        Promise.resolve(),
      ),
    );

    // assert
    const spans = exporter.getFinishedSpans();
    const receive = spans.find((s) => s.name === 'receive events')!;
    const process = spans.find((s) => s.name === 'process events')!;

    expect(process.links.map((l) => l.context.spanId)).toEqual([
      receive.spanContext().spanId,
    ]);
  });

  it('keeps the sending span as the parent of a linked process span', async () => {
    // arrange
    const headers: Record<string, string> = {};

    await tracer.send(
      destination,
      { id: 'id-1', name: 'TestEvent', headers },
      () => Promise.resolve(),
    );

    // act
    await tracer.receive(destination, (batch) =>
      tracer.process(
        destination,
        { id: 'id-1', name: 'TestEvent', headers, batch },
        () => Promise.resolve(),
      ),
    );

    // assert
    const spans = exporter.getFinishedSpans();
    const send = spans.find((s) => s.name === 'send events')!;
    const process = spans.find((s) => s.name === 'process events')!;

    expect(process.parentSpanContext?.spanId).toBe(send.spanContext().spanId);
  });

  it('does not link a process span without a batch', async () => {
    // arrange
    const message = { id: 'id-1', name: 'TestEvent', headers: {} };

    // act
    await tracer.process(destination, message, () => Promise.resolve());

    // assert
    expect(exporter.getFinishedSpans()[0]!.links).toEqual([]);
  });
});
