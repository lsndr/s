import { afterEach, describe, expect, it, vi } from 'vitest';
import { Instrumentation } from './instrumentation.js';
import { NoopTelemetry } from './noop.telemetry.js';
import {
  type TelemetryCounter,
  type TelemetryDestination,
  type TelemetryHistogram,
} from './telemetry.js';

const destination: TelemetryDestination = {
  system: 'redis',
  name: 'stream',
  consumerGroup: 'group',
};
const message = { id: 'id-1', name: 'TestEvent', headers: {} };

/** A telemetry whose instruments can be looked up by their dotted name. */
function setup() {
  const telemetry = new NoopTelemetry();
  const counters = new Map<
    string,
    { add: ReturnType<typeof vi.fn<TelemetryCounter['add']>> }
  >();
  const histograms = new Map<
    string,
    { record: ReturnType<typeof vi.fn<TelemetryHistogram['record']>> }
  >();

  vi.spyOn(telemetry.getMeter(), 'counter').mockImplementation((name) => {
    const counter = { add: vi.fn<TelemetryCounter['add']>() };

    counters.set(name.join('.'), counter);

    return counter;
  });
  vi.spyOn(telemetry.getMeter(), 'histogram').mockImplementation((name) => {
    const histogram = { record: vi.fn<TelemetryHistogram['record']>() };

    histograms.set(name.join('.'), histogram);

    return histogram;
  });

  return {
    telemetry,
    instrumentation: new Instrumentation(telemetry),
    added: (name: string) => counters.get(name)?.add.mock.calls,
    recorded: (name: string) => histograms.get(name)?.record.mock.calls,
  };
}

describe('Instrumentation', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it.each(['create', 'send', 'process'] as const)(
    'runs a %s once, inside the tracer',
    async (operation) => {
      // arrange
      const { telemetry, instrumentation } = setup();
      const traced = vi.spyOn(telemetry.getTracer(), operation);
      const fn = vi.fn(() => Promise.resolve('done'));

      // act
      const result = await instrumentation[operation](destination, message, fn);

      // assert
      expect(result).toBe('done');
      expect(fn).toHaveBeenCalledTimes(1);
      expect(traced.mock.calls[0]![0]).toBe(destination);
    },
  );

  it('measures an operation it traces', async () => {
    // arrange
    const { instrumentation, added } = setup();

    // act
    await instrumentation.send(destination, message, () => Promise.resolve());

    // assert
    expect(added('sent.messages')).toEqual([
      [
        1,
        expect.arrayContaining([
          { name: ['operation', 'name'], value: 'send' },
        ]),
      ],
    ]);
  });

  it('rethrows a failure of the wrapped function', async () => {
    // arrange
    const { instrumentation } = setup();
    const failure = new Error('boom');

    // act
    const result = instrumentation.send(destination, message, () =>
      Promise.reject(failure),
    );

    // assert
    await expect(result).rejects.toBe(failure);
  });

  it('gives the size of a batch to the tracer and the metrics', async () => {
    // arrange
    const { telemetry, instrumentation, added } = setup();
    const traced = { setSize: vi.fn() };

    vi.spyOn(telemetry.getTracer(), 'receive').mockImplementation((_d, fn) =>
      fn(traced),
    );

    // act
    await instrumentation.receive(destination, (batch) => {
      batch.setSize(3);

      return Promise.resolve();
    });

    // assert
    expect(traced.setSize).toHaveBeenCalledWith(3);
    expect(added('consumed.messages')?.[0]?.[0]).toBe(3);
  });

  it('traces a message of a batch with the batch the tracer made', async () => {
    // arrange
    const { telemetry, instrumentation } = setup();
    const traced = { setSize: vi.fn() };
    const process = vi.spyOn(telemetry.getTracer(), 'process');

    vi.spyOn(telemetry.getTracer(), 'receive').mockImplementation((_d, fn) =>
      fn(traced),
    );

    // act
    await instrumentation.receive(destination, (batch) =>
      instrumentation.process(destination, { ...message, batch }, () =>
        Promise.resolve(),
      ),
    );

    // assert
    expect(process.mock.calls[0]![1].batch).toBe(traced);
  });

  it('does not count a processed message of a batch as consumed again', async () => {
    // arrange
    const { instrumentation, added } = setup();

    // act
    await instrumentation.receive(destination, (batch) => {
      batch.setSize(1);

      return instrumentation.process(destination, { ...message, batch }, () =>
        Promise.resolve(),
      );
    });

    // assert
    expect(added('consumed.messages')?.map(([value]) => value)).toEqual([1]);
  });

  it('counts a settlement by its outcome', () => {
    // arrange
    const { instrumentation, added } = setup();

    // act
    instrumentation.settle(destination, 'dropped');

    // assert
    expect(added('settled.messages')).toEqual([
      [
        1,
        [
          { name: ['system'], value: 'redis' },
          { name: ['destination', 'name'], value: 'stream' },
          { name: ['consumer', 'group', 'name'], value: 'group' },
          { name: ['settlement', 'outcome'], value: 'dropped' },
        ],
      ],
    ]);
  });

  it('records a delivery latency', () => {
    // arrange
    const { instrumentation, recorded } = setup();

    // act
    instrumentation.deliver(destination, new Date());

    // assert
    expect(recorded('delivery.latency')).toHaveLength(1);
  });
});
