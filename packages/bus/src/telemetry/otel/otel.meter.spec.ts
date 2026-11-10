import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  AggregationTemporality,
  type DataPoint,
  type Histogram,
  InMemoryMetricExporter,
  MeterProvider,
  type MetricData,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import packageJson from '../../../package.json' with { type: 'json' };
import {
  type TelemetryDestination,
  type TelemetryMeter,
} from '../telemetry.js';
import { BusOtel } from './otel.telemetry.js';

const destination: TelemetryDestination = {
  system: 'rabbitmq',
  name: 'events',
  consumerGroup: 'group',
  subscription: 'Handler',
  clientId: 'client-1',
  server: { address: 'localhost', port: 5672 },
  attributes: { 'rabbitmq.destination.routing_key': 'TestEvent' },
};
const message = { id: 'id-1', name: 'TestEvent', headers: {} };

describe('BusOtel meter', () => {
  let exporter: InMemoryMetricExporter;
  let reader: PeriodicExportingMetricReader;
  let provider: MeterProvider;
  let meter: TelemetryMeter;

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
    meter = telemetry.getMeter();
  });

  afterEach(async () => {
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
    await meter.send(destination, message, () => Promise.resolve());

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
    await meter.send(destination, message, () => Promise.resolve());

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
    const result = meter.send(destination, message, () =>
      Promise.reject(failure),
    );

    // assert
    await expect(result).rejects.toBe(failure);

    const [point] = await pointsOf('messaging.client.sent.messages');

    expect(point!.attributes['error.type']).toBe('TypeError');
  });

  it('records the duration of a create', async () => {
    // act
    await meter.create({ system: 'outbox' }, message, () => Promise.resolve());

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
    await meter.create({ system: 'outbox' }, message, () => Promise.resolve());

    // assert
    expect(await pointsOf('messaging.client.sent.messages')).toEqual([]);
  });

  it('records the duration of a receive', async () => {
    // act
    await meter.receive(destination, () => Promise.resolve());

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
    await meter.receive(destination, (batch) => {
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
    await meter.receive(destination, () => Promise.resolve());

    // assert
    expect(await pointsOf('messaging.client.consumed.messages')).toEqual([]);
  });

  it('names the operation after the custom one of the destination', async () => {
    // act
    await meter.receive({ ...destination, operation: 'reclaim' }, () =>
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
    await meter.process(destination, message, () => Promise.resolve());

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
    await meter.process(destination, message, () => Promise.resolve());

    // assert
    expect(await pointsOf('messaging.client.operation.duration')).toEqual([]);
  });

  it('records a failed process with the type of the error and rethrows it', async () => {
    // arrange
    const failure = new TypeError('boom');

    // act
    const result = meter.process(destination, message, () =>
      Promise.reject(failure),
    );

    // assert
    await expect(result).rejects.toBe(failure);

    const [point] = await pointsOf<Histogram>('messaging.process.duration');

    expect(point!.attributes['error.type']).toBe('TypeError');
  });

  it('counts a processed message without a batch as consumed', async () => {
    // act
    await meter.process(destination, message, () => Promise.resolve());

    // assert
    const [point] = await pointsOf('messaging.client.consumed.messages');

    expect(point!.value).toBe(1);
  });

  it('does not count a processed message of a batch again', async () => {
    // act
    await meter.receive(destination, (batch) => {
      batch.setSize(1);

      return meter.process(destination, { ...message, batch }, () =>
        Promise.resolve(),
      );
    });

    // assert
    const points = await pointsOf('messaging.client.consumed.messages');

    expect(points.map(({ value }) => value)).toEqual([1]);
  });

  it('uses the bucket boundaries the conventions advise', async () => {
    // act
    await meter.process(destination, message, () => Promise.resolve());

    // assert
    const [point] = await pointsOf<Histogram>('messaging.process.duration');

    expect(point!.value.buckets.boundaries).toEqual([
      0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
    ]);
  });

  it('names the instrumentation scope after the package', async () => {
    // arrange
    await meter.process(destination, message, () => Promise.resolve());

    // act
    await reader.forceFlush();

    // assert
    const { scopeMetrics } = exporter.getMetrics()[0]!;

    expect(scopeMetrics[0]!.scope).toMatchObject({
      name: packageJson.name,
      version: packageJson.version,
    });
  });

  it('adds to a counter', async () => {
    // arrange
    const counter = meter.counter('test.counter', { unit: '{thing}' });

    // act
    counter.add(2, { kind: 'a' });

    // assert
    const [point] = await pointsOf('test.counter');

    expect(point).toMatchObject({ value: 2, attributes: { kind: 'a' } });
  });

  it('records to a histogram with its boundaries', async () => {
    // arrange
    const histogram = meter.histogram('test.histogram', {
      boundaries: [1, 10],
    });

    // act
    histogram.record(5);

    // assert
    const [point] = await pointsOf<Histogram>('test.histogram');

    expect(point!.value.buckets).toEqual({
      boundaries: [1, 10],
      counts: [0, 1, 0],
    });
  });

  it('observes a gauge when metrics are collected', async () => {
    // arrange
    meter.gauge('test.gauge', {}, () =>
      Promise.resolve([{ value: 7, attributes: { kind: 'a' } }]),
    );

    // act
    const [point] = await pointsOf('test.gauge');

    // assert
    expect(point).toMatchObject({ value: 7, attributes: { kind: 'a' } });
  });

  it('stops observing a closed gauge', async () => {
    // arrange
    const gauge = meter.gauge('test.gauge', {}, () => [{ value: 7 }]);

    gauge.close();

    // act
    const points = await pointsOf('test.gauge');

    // assert
    expect(points).toEqual([]);
  });
});
