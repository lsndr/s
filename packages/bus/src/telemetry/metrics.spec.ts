import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Logger } from '@nestjs/common';
import {
  AggregationTemporality,
  type DataPoint,
  type Histogram,
  InMemoryMetricExporter,
  MeterProvider,
  type MetricData,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import { Metrics } from './metrics.js';
import { NoopTelemetry } from './noop.telemetry.js';
import { BusOtel } from './otel/otel.telemetry.js';
import { type TelemetryDestination } from './telemetry.js';

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
const message = { id: 'id-1', name: 'TestEvent', headers: {} };

describe('Metrics', () => {
  let exporter: InMemoryMetricExporter;
  let reader: PeriodicExportingMetricReader;
  let provider: MeterProvider;
  let metrics: Metrics;

  beforeEach(async () => {
    exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
    reader = new PeriodicExportingMetricReader({
      exporter,
      // only collected when a test asks for it
      exportIntervalMillis: 3_600_000,
    });
    provider = new MeterProvider({ readers: [reader] });

    const telemetry = new BusOtel({ meterProvider: provider });

    await telemetry.onBusInit();
    metrics = new Metrics(telemetry);
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await provider.shutdown();
  });

  async function collect(): Promise<MetricData[]> {
    exporter.reset();
    await reader.forceFlush();

    return exporter
      .getMetrics()
      .flatMap(({ scopeMetrics }) => scopeMetrics)
      .flatMap(({ metrics }) => metrics);
  }

  async function pointsOf<T = number>(name: string): Promise<DataPoint<T>[]> {
    const metric = (await collect()).find((m) => m.descriptor.name === name);

    return (metric?.dataPoints ?? []) as DataPoint<T>[];
  }

  it('records the duration of a send', async () => {
    // act
    await metrics.send(destination, () => Promise.resolve());

    // assert
    const [point] = await pointsOf<Histogram>(
      'messaging.client.operation.duration',
    );

    expect(point!.value.count).toBe(1);
    expect(point!.attributes).toEqual({
      'messaging.operation.name': 'send',
      'messaging.operation.type': 'send',
      'messaging.system.name': 'rabbitmq',
      'messaging.destination.name': 'events',
      'messaging.consumer.group.name': 'group',
      'messaging.destination.subscription.name': 'Handler',
      'server.address': 'localhost',
      'server.port': 5672,
    });
  });

  it('counts a sent message', async () => {
    // act
    await metrics.send(destination, () => Promise.resolve());

    // assert
    const [point] = await pointsOf('messaging.client.sent.messages');

    expect(point!.value).toBe(1);
    expect(point!.attributes).toMatchObject({
      'messaging.operation.name': 'send',
      'messaging.system.name': 'rabbitmq',
      'messaging.destination.name': 'events',
    });
    expect(point!.attributes).not.toHaveProperty('messaging.operation.type');
  });

  it('counts a message that failed to send with the type of the error', async () => {
    // arrange
    const failure = new TypeError('boom');

    // act
    const result = metrics.send(destination, () => Promise.reject(failure));

    // assert
    await expect(result).rejects.toBe(failure);

    const [point] = await pointsOf('messaging.client.sent.messages');

    expect(point!.attributes['error.type']).toBe('TypeError');
  });

  it('records the duration of a create', async () => {
    // act
    await metrics.create({ system: 'outbox' }, () => Promise.resolve());

    // assert
    const [point] = await pointsOf<Histogram>(
      'messaging.client.operation.duration',
    );

    expect(point!.attributes).toEqual({
      'messaging.operation.name': 'create',
      'messaging.operation.type': 'create',
      'messaging.system.name': 'outbox',
    });
  });

  it('does not count a created message as sent', async () => {
    // act
    await metrics.create({ system: 'outbox' }, () => Promise.resolve());

    // assert
    expect(await pointsOf('messaging.client.sent.messages')).toEqual([]);
  });

  it('records the duration of a receive', async () => {
    // act
    await metrics.receive(destination, () => Promise.resolve());

    // assert
    const [point] = await pointsOf<Histogram>(
      'messaging.client.operation.duration',
    );

    expect(point!.attributes).toMatchObject({
      'messaging.operation.name': 'receive',
      'messaging.operation.type': 'receive',
    });
  });

  it('counts the messages of a received batch as consumed', async () => {
    // act
    await metrics.receive(destination, (batch) => {
      batch.setSize(3);

      return Promise.resolve();
    });

    // assert
    const [point] = await pointsOf('messaging.client.consumed.messages');

    expect(point!.value).toBe(3);
    expect(point!.attributes).toMatchObject({
      'messaging.operation.name': 'receive',
      'messaging.consumer.group.name': 'group',
    });
  });

  it('counts nothing as consumed for an empty batch', async () => {
    // act
    await metrics.receive(destination, () => Promise.resolve());

    // assert
    expect(await pointsOf('messaging.client.consumed.messages')).toEqual([]);
  });

  it('names the operation after the custom one of the destination', async () => {
    // act
    await metrics.receive({ ...destination, operation: 'reclaim' }, () =>
      Promise.resolve(),
    );

    // assert
    const [point] = await pointsOf<Histogram>(
      'messaging.client.operation.duration',
    );

    expect(point!.attributes).toMatchObject({
      'messaging.operation.name': 'reclaim',
      'messaging.operation.type': 'receive',
    });
  });

  it('records the duration of a process', async () => {
    // act
    await metrics.process(destination, message, () => Promise.resolve());

    // assert
    const [point] = await pointsOf<Histogram>('messaging.process.duration');

    expect(point!.value.count).toBe(1);
    expect(point!.attributes).toEqual({
      'messaging.operation.name': 'process',
      'messaging.system.name': 'rabbitmq',
      'messaging.destination.name': 'events',
      'messaging.consumer.group.name': 'group',
      'messaging.destination.subscription.name': 'Handler',
      'server.address': 'localhost',
      'server.port': 5672,
    });
  });

  it('does not record a process as a client operation', async () => {
    // act
    await metrics.process(destination, message, () => Promise.resolve());

    // assert
    expect(await pointsOf('messaging.client.operation.duration')).toEqual([]);
  });

  it('records a failed process with the type of the error and rethrows it', async () => {
    // arrange
    const failure = new TypeError('boom');

    // act
    const result = metrics.process(destination, message, () =>
      Promise.reject(failure),
    );

    // assert
    await expect(result).rejects.toBe(failure);

    const [point] = await pointsOf<Histogram>('messaging.process.duration');

    expect(point!.attributes['error.type']).toBe('TypeError');
  });

  it('counts a processed message without a batch as consumed', async () => {
    // act
    await metrics.process(destination, message, () => Promise.resolve());

    // assert
    const [point] = await pointsOf('messaging.client.consumed.messages');

    expect(point!.value).toBe(1);
  });

  it('does not count a processed message of a batch again', async () => {
    // act
    await metrics.receive(destination, (batch) => {
      batch.setSize(1);

      return metrics.process(destination, { ...message, batch }, () =>
        Promise.resolve(),
      );
    });

    // assert
    const points = await pointsOf('messaging.client.consumed.messages');

    expect(points.map(({ value }) => value)).toEqual([1]);
  });

  it('uses the bucket boundaries the conventions advise', async () => {
    // act
    await metrics.process(destination, message, () => Promise.resolve());

    // assert
    const [point] = await pointsOf<Histogram>('messaging.process.duration');

    expect(point!.value.buckets.boundaries).toEqual([
      0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
    ]);
  });

  it('counts a settlement by its outcome', async () => {
    // act
    await metrics.settle(destination, 'dropped', () => Promise.resolve());

    // assert
    const [point] = await pointsOf('bus.settled.messages');

    expect(point).toMatchObject({
      value: 1,
      attributes: {
        'messaging.system.name': 'rabbitmq',
        'messaging.destination.name': 'events',
        'messaging.consumer.group.name': 'group',
        'bus.settlement.outcome': 'dropped',
      },
    });
    expect(point!.attributes).not.toHaveProperty('messaging.operation.name');
  });

  it('records the duration of a settlement under its operation name', async () => {
    // act
    await metrics.settle({ ...destination, operation: 'nack' }, 'retried', () =>
      Promise.resolve(),
    );

    // assert
    const [point] = await pointsOf<Histogram>(
      'messaging.client.operation.duration',
    );

    expect(point!.value.count).toBe(1);
    expect(point!.attributes).toMatchObject({
      'messaging.operation.name': 'nack',
      'messaging.operation.type': 'settle',
    });
  });

  it('names a settlement after the operation when it has no name', async () => {
    // act
    await metrics.settle(destination, 'acked', () => Promise.resolve());

    // assert
    const [point] = await pointsOf<Histogram>(
      'messaging.client.operation.duration',
    );

    expect(point!.attributes).toMatchObject({
      'messaging.operation.name': 'settle',
      'messaging.operation.type': 'settle',
    });
  });

  it('counts a settlement that failed with the type of the error', async () => {
    // arrange
    const failure = new TypeError('boom');

    // act
    const result = metrics.settle(destination, 'acked', () =>
      Promise.reject(failure),
    );

    // assert
    await expect(result).rejects.toBe(failure);

    const [point] = await pointsOf('bus.settled.messages');

    expect(point).toMatchObject({
      value: 1,
      attributes: {
        'bus.settlement.outcome': 'acked',
        'error.type': 'TypeError',
      },
    });
  });

  it('records the duration of a settlement that failed with the type of the error', async () => {
    // act
    const result = metrics.settle(destination, 'acked', () =>
      Promise.reject(new TypeError('boom')),
    );

    // assert
    await expect(result).rejects.toThrow('boom');

    const [point] = await pointsOf<Histogram>(
      'messaging.client.operation.duration',
    );

    expect(point!.attributes).toMatchObject({ 'error.type': 'TypeError' });
  });

  it('counts a retry without measuring a settlement', async () => {
    // act
    metrics.retry(destination);

    // assert
    const [point] = await pointsOf('bus.settled.messages');

    expect(point).toMatchObject({
      value: 1,
      attributes: { 'bus.settlement.outcome': 'retried' },
    });
    expect(await pointsOf('messaging.client.operation.duration')).toHaveLength(
      0,
    );
  });

  it('records the time since the publish as the delivery latency', async () => {
    // arrange
    const publishedAt = new Date(Date.now() - 2_500);

    // act
    metrics.deliver(destination, publishedAt);

    // assert
    const [point] = await pointsOf<Histogram>('bus.delivery.latency');

    expect(point!.value.sum).toBeGreaterThanOrEqual(2.5);
    expect(point!.value.sum).toBeLessThan(5);
  });

  it('records no negative latency when the publisher clock is ahead', async () => {
    // arrange
    const publishedAt = new Date(Date.now() + 5_000);

    // act
    metrics.deliver(destination, publishedAt);

    // assert
    const [point] = await pointsOf<Histogram>('bus.delivery.latency');

    expect(point!.value.sum).toBe(0);
  });

  it('records the duration of an outbox relay round', async () => {
    // act
    await metrics.relay(() => Promise.resolve(0));

    // assert
    const [point] = await pointsOf<Histogram>('bus.outbox.relay.duration');

    expect(point!.value.count).toBe(1);
    expect(point!.attributes).toEqual({ 'messaging.system.name': 'outbox' });
  });

  it('records a failed relay round with the type of the error and rethrows it', async () => {
    // arrange
    const failure = new TypeError('boom');

    // act
    const result = metrics.relay(() => Promise.reject(failure));

    // assert
    await expect(result).rejects.toBe(failure);

    const [point] = await pointsOf<Histogram>('bus.outbox.relay.duration');

    expect(point!.attributes['error.type']).toBe('TypeError');
  });

  it('counts the records an outbox dispatched', async () => {
    // act
    await metrics.dispatch([new Date(), new Date()], () => Promise.resolve());

    // assert
    const [point] = await pointsOf('bus.outbox.relayed.messages');

    expect(point).toMatchObject({
      value: 2,
      attributes: { 'messaging.system.name': 'outbox' },
    });
  });

  it('records how long each dispatched record waited', async () => {
    // arrange
    const publishedAt = [new Date(Date.now() - 3_000), new Date()];

    // act
    await metrics.dispatch(publishedAt, () => Promise.resolve());

    // assert
    const [point] = await pointsOf<Histogram>('bus.outbox.relay.latency');

    expect(point!.value.count).toBe(2);
    expect(point!.value.sum).toBeGreaterThanOrEqual(3);
  });

  it('records no latency for records that failed to dispatch', async () => {
    // arrange
    const failure = new Error('boom');

    // act
    const result = metrics.dispatch([new Date()], () =>
      Promise.reject(failure),
    );

    // assert
    await expect(result).rejects.toBe(failure);
    expect(await pointsOf('bus.outbox.relay.latency')).toEqual([]);
  });

  it('does not fail the wrapped work when the meter fails', async () => {
    // arrange
    const telemetry = new NoopTelemetry();

    vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    vi.spyOn(telemetry.getMeter(), 'histogram').mockImplementation(() => {
      throw new Error('boom');
    });

    // act
    const result = await new Metrics(telemetry).send(destination, () =>
      Promise.resolve('sent'),
    );

    // assert
    expect(result).toBe('sent');
  });
});
