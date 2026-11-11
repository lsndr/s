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
import { type TelemetryMeter } from '../telemetry.js';
import { BusOtel } from './otel.telemetry.js';

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

  it('names the instrumentation scope after the package', async () => {
    // arrange
    meter.counter(['test', 'counter']).add(1);

    // act
    await reader.forceFlush();

    // assert
    const { scopeMetrics } = exporter.getMetrics()[0]!;

    expect(scopeMetrics[0]!.scope).toMatchObject({
      name: packageJson.name,
      version: packageJson.version,
    });
  });

  it('adds to a counter, with dots between the parts of the names', async () => {
    // arrange
    const counter = meter.counter(['test', 'counter'], { unit: '{thing}' });

    // act
    counter.add(2, [{ name: ['test', 'kind'], value: 'a' }]);

    // assert
    const [point] = await pointsOf('bus.test.counter');

    expect(point).toMatchObject({
      value: 2,
      attributes: { 'bus.test.kind': 'a' },
    });
  });

  it('exports a name as the conventions name it', async () => {
    // arrange
    const counter = meter.counter(['sent', 'messages']);

    // act
    counter.add(1, [
      { name: ['system'], value: 'redis' },
      { name: ['error', 'type'], value: 'TypeError' },
    ]);

    // assert
    const [point] = await pointsOf('messaging.client.sent.messages');

    expect(point!.attributes).toEqual({
      'messaging.system.name': 'redis',
      'error.type': 'TypeError',
    });
  });

  it('puts the prefix before a name the conventions have no name for', async () => {
    // arrange
    const counter = meter.counter(['settled', 'messages']);

    // act
    counter.add(1, [{ name: ['settlement', 'outcome'], value: 'acked' }]);

    // assert
    const [point] = await pointsOf('bus.settled.messages');

    expect(point!.attributes).toEqual({ 'bus.settlement.outcome': 'acked' });
  });

  it('exports a name of a broker without the prefix', async () => {
    // arrange
    const counter = meter.counter(['redis', 'stream', 'lag']);

    // act
    counter.add(1, [{ name: ['server', 'address'], value: 'localhost' }]);

    // assert
    const [point] = await pointsOf('redis.stream.lag');

    expect(point!.attributes).toEqual({ 'server.address': 'localhost' });
  });

  it('puts a custom prefix before a name', async () => {
    // arrange
    const telemetry = new BusOtel({ meterProvider: provider, prefix: 'acme' });

    await telemetry.onBusInit();

    // act
    telemetry.getMeter().counter(['settled', 'messages']).add(1);

    // assert
    expect(await pointsOf('acme.settled.messages')).toHaveLength(1);
  });

  it('records to a histogram with its boundaries', async () => {
    // arrange
    const histogram = meter.histogram(['test', 'histogram'], {
      boundaries: [1, 10],
    });

    // act
    histogram.record(5);

    // assert
    const [point] = await pointsOf<Histogram>('bus.test.histogram');

    expect(point!.value.buckets).toEqual({
      boundaries: [1, 10],
      counts: [0, 1, 0],
    });
  });

  it('observes a gauge when metrics are collected', async () => {
    // arrange
    meter.gauge(['test', 'gauge'], {}, () =>
      Promise.resolve([
        { value: 7, attributes: [{ name: ['test', 'kind'], value: 'a' }] },
      ]),
    );

    // act
    const [point] = await pointsOf('bus.test.gauge');

    // assert
    expect(point).toMatchObject({
      value: 7,
      attributes: { 'bus.test.kind': 'a' },
    });
  });

  it('stops observing a closed gauge', async () => {
    // arrange
    const gauge = meter.gauge(['test', 'gauge'], {}, () => [{ value: 7 }]);

    gauge.close();

    // act
    const points = await pointsOf('bus.test.gauge');

    // assert
    expect(points).toEqual([]);
  });

  it('observes a counter when metrics are collected', async () => {
    // arrange
    meter.observableCounter(['test', 'total'], {}, () =>
      Promise.resolve([
        { value: 4, attributes: [{ name: ['test', 'kind'], value: 'a' }] },
      ]),
    );

    // act
    const [point] = await pointsOf('bus.test.total');

    // assert
    expect(point).toMatchObject({
      value: 4,
      attributes: { 'bus.test.kind': 'a' },
    });
  });

  it('stops observing a closed counter', async () => {
    // arrange
    const counter = meter.observableCounter(['test', 'total'], {}, () => [
      { value: 4 },
    ]);

    counter.close();

    // act
    const points = await pointsOf('bus.test.total');

    // assert
    expect(points).toEqual([]);
  });
});
