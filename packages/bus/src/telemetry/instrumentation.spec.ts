import { afterEach, describe, expect, it, vi } from 'vitest';
import { Logger } from '@nestjs/common';
import { Instrumentation } from './instrumentation.js';
import { NoopTelemetry } from './noop.telemetry.js';
import { type TelemetryDestination } from './telemetry.js';

const destination: TelemetryDestination = {
  system: 'redis',
  name: 'stream',
  consumerGroup: 'group',
};
const message = { id: 'id-1', name: 'TestEvent', headers: {} };

describe('Instrumentation', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it.each(['create', 'send', 'process'] as const)(
    'runs a %s once, inside the tracer and the meter',
    async (operation) => {
      // arrange
      const telemetry = new NoopTelemetry();
      const traced = vi.spyOn(telemetry.getTracer(), operation);
      const metered = vi.spyOn(telemetry.getMeter(), operation);
      const fn = vi.fn(() => Promise.resolve('done'));

      // act
      const result = await new Instrumentation(telemetry)[operation](
        destination,
        message,
        fn,
      );

      // assert
      expect(result).toBe('done');
      expect(fn).toHaveBeenCalledTimes(1);
      expect(traced.mock.calls[0]![0]).toBe(destination);
      expect(metered.mock.calls[0]![0]).toBe(destination);
    },
  );

  it('rethrows a failure of the wrapped function', async () => {
    // arrange
    const failure = new Error('boom');

    // act
    const result = new Instrumentation(new NoopTelemetry()).send(
      destination,
      message,
      () => Promise.reject(failure),
    );

    // assert
    await expect(result).rejects.toBe(failure);
  });

  it('gives the size of a batch to the tracer and the meter', async () => {
    // arrange
    const telemetry = new NoopTelemetry();
    const traced = { setSize: vi.fn() };
    const metered = { setSize: vi.fn() };

    vi.spyOn(telemetry.getTracer(), 'receive').mockImplementation((_d, fn) =>
      fn(traced),
    );
    vi.spyOn(telemetry.getMeter(), 'receive').mockImplementation((_d, fn) =>
      fn(metered),
    );

    // act
    await new Instrumentation(telemetry).receive(destination, (batch) => {
      batch.setSize(3);

      return Promise.resolve();
    });

    // assert
    expect(traced.setSize).toHaveBeenCalledWith(3);
    expect(metered.setSize).toHaveBeenCalledWith(3);
  });

  it('processes a message of a batch with the batch each side made', async () => {
    // arrange
    const telemetry = new NoopTelemetry();
    const instrumentation = new Instrumentation(telemetry);
    const traced = { setSize: vi.fn() };
    const metered = { setSize: vi.fn() };
    const tracerProcess = vi.spyOn(telemetry.getTracer(), 'process');
    const meterProcess = vi.spyOn(telemetry.getMeter(), 'process');

    vi.spyOn(telemetry.getTracer(), 'receive').mockImplementation((_d, fn) =>
      fn(traced),
    );
    vi.spyOn(telemetry.getMeter(), 'receive').mockImplementation((_d, fn) =>
      fn(metered),
    );

    // act
    await instrumentation.receive(destination, (batch) =>
      instrumentation.process(destination, { ...message, batch }, () =>
        Promise.resolve(),
      ),
    );

    // assert
    expect(tracerProcess.mock.calls[0]![1].batch).toBe(traced);
    expect(meterProcess.mock.calls[0]![1].batch).toBe(metered);
  });

  it('counts a settlement by its outcome', () => {
    // arrange
    const telemetry = new NoopTelemetry();
    const counter = { add: vi.fn() };
    const create = vi
      .spyOn(telemetry.getMeter(), 'counter')
      .mockReturnValue(counter);

    // act
    new Instrumentation(telemetry).settle(destination, 'dropped');

    // assert
    expect(create.mock.calls[0]![0]).toBe('bus.client.settled.messages');
    expect(counter.add).toHaveBeenCalledWith(1, {
      'messaging.system.name': 'redis',
      'messaging.destination.name': 'stream',
      'messaging.consumer.group.name': 'group',
      'bus.settlement.outcome': 'dropped',
    });
  });

  it('does not throw when the meter fails', () => {
    // arrange
    const telemetry = new NoopTelemetry();

    vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    vi.spyOn(telemetry.getMeter(), 'counter').mockImplementation(() => {
      throw new Error('boom');
    });

    // act
    const result = () =>
      new Instrumentation(telemetry).settle(destination, 'acked');

    // assert
    expect(result).not.toThrow();
  });

  it('records the time since the publish as the delivery latency', () => {
    // arrange
    const telemetry = new NoopTelemetry();
    const histogram = { record: vi.fn() };
    const create = vi
      .spyOn(telemetry.getMeter(), 'histogram')
      .mockReturnValue(histogram);

    vi.useFakeTimers({ now: new Date('2026-01-01T00:00:02.500Z') });

    // act
    new Instrumentation(telemetry).deliver(
      destination,
      new Date('2026-01-01T00:00:00.000Z'),
    );

    // assert
    expect(create.mock.calls[0]![0]).toBe('bus.delivery.latency');
    expect(histogram.record).toHaveBeenCalledWith(2.5, {
      'messaging.system.name': 'redis',
      'messaging.destination.name': 'stream',
      'messaging.consumer.group.name': 'group',
    });
  });

  it('records no negative latency when the publisher clock is ahead', () => {
    // arrange
    const telemetry = new NoopTelemetry();
    const histogram = { record: vi.fn() };

    vi.spyOn(telemetry.getMeter(), 'histogram').mockReturnValue(histogram);
    vi.useFakeTimers({ now: new Date('2026-01-01T00:00:00.000Z') });

    // act
    new Instrumentation(telemetry).deliver(
      destination,
      new Date('2026-01-01T00:00:05.000Z'),
    );

    // assert
    expect(histogram.record.mock.calls[0]![0]).toBe(0);
  });
});
