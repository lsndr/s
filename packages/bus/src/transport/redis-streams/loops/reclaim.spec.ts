import { setTimeout as sleep } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import { GenericContainer, Wait } from 'testcontainers';
import { createClient, type RedisClientType } from 'redis';
import { NoopTelemetry } from '../../../telemetry/noop.telemetry.js';
import { Clients } from '../clients.js';
import { type Processor } from '../processor.js';
import { Handler } from '../handler.js';
import { Streams } from '../streams.js';
import { ReclaimLoop } from './reclaim.js';

class TestEvent {}

interface SetupOptions {
  /** Number of streams. Defaults to 1. */
  streams?: number;
  batchSize?: number;
  retriesDelay?: number;
  reclaimInterval?: number;
}

describe('ReclaimLoop', { timeout: 60_000 }, () => {
  let container: Awaited<ReturnType<GenericContainer['start']>>;
  let clients: Clients<'command'>;
  let redis: RedisClientType;
  const releases: (() => void)[] = [];

  async function setup({
    streams: count = 1,
    batchSize = 10,
    retriesDelay = 100,
    reclaimInterval = 1000,
  }: SetupOptions = {}) {
    const group = `group:${crypto.randomUUID()}`;
    const consumer = 'consumer';
    const streams = new Streams(10);
    const names: string[] = [];

    for (let i = 0; i < count; i++) {
      const name = `stream:${crypto.randomUUID()}`;

      await redis.xGroupCreate(name, group, '0', { MKSTREAM: true });
      streams.register(name, TestEvent, new Handler('handler', vi.fn()));
      names.push(name);
    }

    const process = vi.fn(
      (..._args: Parameters<Processor['process']>): Promise<void> =>
        Promise.resolve(),
    );
    const telemetry = new NoopTelemetry();
    const loop = new ReclaimLoop(
      clients,
      streams,
      { process } as unknown as Processor,
      telemetry,
      { group, consumer, batchSize, retriesDelay, reclaimInterval },
    );

    return {
      loop,
      telemetry,
      streams,
      process,
      group,
      consumer,
      names,
      next: (signal = new AbortController().signal) => loop.next(signal),
      abandon: async (name: string, n = 1) => {
        const ids: string[] = [];

        for (let i = 0; i < n; i++) {
          ids.push(await redis.xAdd(name, '*', { payload: `${i}` }));
        }

        await redis.xReadGroup(
          group,
          'dead',
          { key: name, id: '>' },
          { COUNT: n },
        );

        return ids;
      },
      stale: async () => {
        await sleep(retriesDelay + 50);
      },
      hold: (name: string, ids: string[]) => {
        const held = Promise.withResolvers<void>();

        for (const id of ids) {
          streams.run(name, id, () => held.promise);
        }

        releases.push(() => held.resolve());
      },
      pending: async (name: string) =>
        await redis.xPendingRange(name, group, '-', '+', 100),
      streamOf: (name: string) => {
        const stream = streams.all().find((s) => s.name === name);

        if (!stream) {
          throw new Error(`Stream ${name} is not registered`);
        }

        return stream;
      },
    };
  }

  beforeAll(async () => {
    container = await new GenericContainer('redis:8.2')
      .withExposedPorts(6379)
      .withWaitStrategy(
        Wait.forAll([
          Wait.forListeningPorts(),
          Wait.forLogMessage(/Ready to accept connections/),
        ]),
      )
      .start();

    const url = `redis://${container.getHost()}:${container.getMappedPort(6379)}`;

    clients = new Clients<'command'>(url);
    redis = createClient({ url });
    await redis.connect();
  }, 60_000);

  afterAll(async () => {
    await redis.close();
    await clients.destroy();
    await container.stop();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    releases.splice(0).forEach((release) => release());
  });

  describe('telemetry', () => {
    it('claims inside a receive of the stream', async () => {
      // arrange
      const { next, abandon, stale, telemetry, names, group, consumer } =
        await setup();
      const [name] = names as [string];
      const receive = vi.spyOn(telemetry, 'receive');

      await abandon(name);
      await stale();

      // act
      await next();

      // assert
      expect(receive.mock.calls[0]![0]).toMatchObject({
        system: 'redis',
        operation: 'reclaim',
        name,
        consumerGroup: group,
        clientId: consumer,
      });
    });

    it('records how many entries were claimed', async () => {
      // arrange
      const { next, abandon, stale, telemetry, names } = await setup();
      const [name] = names as [string];
      const batch = { setSize: vi.fn() };

      vi.spyOn(telemetry, 'receive').mockImplementation((_d, fn) => fn(batch));
      await abandon(name, 2);
      await stale();

      // act
      await next();

      // assert
      expect(batch.setSize).toHaveBeenCalledWith(2);
    });

    it('hands the batch on to the processed entries', async () => {
      // arrange
      const { next, abandon, stale, telemetry, process, names } = await setup();
      const [name] = names as [string];
      const batch = { setSize: vi.fn() };

      vi.spyOn(telemetry, 'receive').mockImplementation((_d, fn) => fn(batch));
      await abandon(name);
      await stale();

      // act
      await next();

      // assert
      expect(process.mock.calls[0]![3]).toBe(batch);
    });
  });

  describe('reclaiming', () => {
    it('runs abandoned entries with the next attempt', async () => {
      // arrange
      const { next, abandon, stale, process, names, streamOf } = await setup();
      const [name] = names as [string];
      const ids = await abandon(name, 2);

      await stale();

      // act
      await next();

      // assert
      expect(process).toHaveBeenCalledTimes(2);
      expect(process).toHaveBeenCalledWith(
        streamOf(name),
        { id: ids[0], message: { payload: Buffer.from('0') } },
        2,
        expect.anything(),
      );
      expect(process).toHaveBeenCalledWith(
        streamOf(name),
        { id: ids[1], message: { payload: Buffer.from('1') } },
        2,
        expect.anything(),
      );
    });

    it('takes the ownership of the claimed entries', async () => {
      // arrange
      const { next, abandon, stale, pending, names, consumer } = await setup();
      const [name] = names as [string];
      const [id] = (await abandon(name)) as [string];

      await stale();

      // act
      await next();

      // assert
      expect(await pending(name)).toEqual([
        expect.objectContaining({ id, consumer, deliveriesCounter: 2 }),
      ]);
    });

    it('does not claim the entries that are not idle for long enough', async () => {
      // arrange
      const { next, abandon, process, pending, names } = await setup({
        retriesDelay: 10_000,
      });
      const [name] = names as [string];
      const [id] = (await abandon(name)) as [string];

      // act
      await next();

      // assert
      expect(process).not.toHaveBeenCalled();
      expect(await pending(name)).toEqual([
        expect.objectContaining({ id, consumer: 'dead', deliveriesCounter: 1 }),
      ]);
    });

    it('does not claim the entries in flight', async () => {
      // arrange
      const { next, abandon, hold, stale, process, names } = await setup();
      const [name] = names as [string];
      const client = await clients.getBinary('command');
      const xClaim = vi.spyOn(client, 'xClaim');
      const ids = await abandon(name, 2);

      hold(name, ids);
      await stale();

      // act
      await next();

      // assert
      expect(xClaim).not.toHaveBeenCalled();
      expect(process).not.toHaveBeenCalled();
    });

    it.each([1, 5])(
      'claims at most batch size entries of a stream at once',
      async (batchSize) => {
        // arrange
        const { next, abandon, stale, process, names } = await setup({
          batchSize,
        });
        const [name] = names as [string];

        await abandon(name, batchSize * 2);
        await stale();

        // act
        await next();

        // assert
        expect(process).toHaveBeenCalledTimes(batchSize);
      },
    );

    it('reclaims the entries of several streams', async () => {
      // arrange
      const { next, abandon, stale, process, names, streamOf } = await setup({
        streams: 2,
      });
      const [a, b] = names as [string, string];
      const [idA] = await abandon(a);
      const [idB] = await abandon(b);

      await stale();

      // act
      await next();

      // assert
      expect(process).toHaveBeenCalledTimes(2);
      expect(process).toHaveBeenCalledWith(
        streamOf(a),
        expect.objectContaining({ id: idA }),
        2,
        expect.anything(),
      );
      expect(process).toHaveBeenCalledWith(
        streamOf(b),
        expect.objectContaining({ id: idB }),
        2,
        expect.anything(),
      );
    });
  });

  describe('abort', () => {
    it('does not call Redis when it is aborted', async () => {
      // arrange
      const { next, abandon, stale, process, names } = await setup();
      const [name] = names as [string];
      const client = await clients.get('command');
      const xPendingRange = vi.spyOn(client, 'xPendingRange');
      const controller = new AbortController();

      await abandon(name);
      await stale();
      controller.abort();

      // act
      await next(controller.signal);

      // assert
      expect(xPendingRange).not.toHaveBeenCalled();
      expect(process).not.toHaveBeenCalled();
    });

    it('does not claim the entries when aborted while checking them', async () => {
      // arrange
      const { next, abandon, stale, process, pending, names } = await setup();
      const [name] = names as [string];
      const client = await clients.get('command');
      const original = client.xPendingRange.bind(client);
      const controller = new AbortController();
      const xClaim = vi.spyOn(await clients.getBinary('command'), 'xClaim');
      const [id] = (await abandon(name)) as [string];

      vi.spyOn(client, 'xPendingRange').mockImplementation(async (...args) => {
        const reply = await original(...args);

        controller.abort();

        return reply;
      });
      await stale();

      // act
      await next(controller.signal);

      // assert
      expect(xClaim).not.toHaveBeenCalled();
      expect(process).not.toHaveBeenCalled();
      expect(await pending(name)).toEqual([
        expect.objectContaining({ id, consumer: 'dead', deliveriesCounter: 1 }),
      ]);
    });

    it('does not run the claimed entries when aborted while claiming them', async () => {
      // arrange
      const { next, abandon, stale, process, names } = await setup();
      const [name] = names as [string];
      const client = await clients.getBinary('command');
      const original = client.xClaim.bind(client);
      const controller = new AbortController();

      await abandon(name);
      vi.spyOn(client, 'xClaim').mockImplementation(async (...args) => {
        const reply = await original(...args);

        controller.abort();

        return reply;
      });
      await stale();

      // act
      await next(controller.signal);

      // assert
      expect(process).not.toHaveBeenCalled();
    });

    it('does not log the errors that happen after it is aborted', async () => {
      // arrange
      const { next, names } = await setup();
      const [name] = names as [string];
      const client = await clients.get('command');
      const error = vi.spyOn(Logger.prototype, 'error').mockReturnValue();
      const controller = new AbortController();

      vi.spyOn(client, 'xPendingRange').mockImplementation(() => {
        controller.abort();

        return Promise.reject(new Error(`Connection lost: ${name}`));
      });

      // act
      await next(controller.signal);

      // assert
      expect(error).not.toHaveBeenCalled();
    });
  });

  describe('errors', () => {
    it('logs a failing stream and still reclaims the others', async () => {
      // arrange
      const { next, abandon, stale, process, names, streamOf } = await setup({
        streams: 2,
      });
      const [failing, working] = names as [string, string];
      const client = await clients.get('command');
      const error = vi.spyOn(Logger.prototype, 'error').mockReturnValue();
      const [id] = (await abandon(working)) as [string];

      vi.spyOn(client, 'xPendingRange').mockRejectedValueOnce(
        new Error('Connection lost'),
      );
      await stale();

      // act
      await next();

      // assert
      expect(error).toHaveBeenCalledExactlyOnceWith(
        expect.stringContaining(failing),
        expect.any(String),
      );
      expect(process).toHaveBeenCalledExactlyOnceWith(
        streamOf(working),
        expect.objectContaining({ id }),
        2,
        expect.anything(),
      );
    });
  });

  describe('interval', () => {
    it.each([
      { random: 0, expected: 900 },
      { random: 0.5, expected: 1000 },
      { random: 0.999, expected: 1100 },
    ])(
      'is $expected ms when the jitter is $random',
      async ({ random, expected }) => {
        // arrange
        const { loop } = await setup({ reclaimInterval: 1000 });

        vi.spyOn(Math, 'random').mockReturnValue(random);

        // act
        const interval = loop.interval();

        // assert
        expect(interval).toBeCloseTo(expected, 0);
      },
    );
  });
});
