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
import { Clients } from '../clients.js';
import { type Processor } from '../processor.js';
import { Streams } from '../streams.js';
import { ReadLoop } from './read.js';

class TestEvent {}

interface SetupOptions {
  /** Number of streams. Defaults to 1. */
  streams?: number;
  slotsPerStream?: number;
  batchSize?: number;
}

describe('ReadLoop', { timeout: 60_000 }, () => {
  let container: Awaited<ReturnType<GenericContainer['start']>>;
  let clients: Clients<'read'>;
  let redis: RedisClientType;

  /** Creates streams that are registered in `Streams`, and a loop reading them. */
  async function setup({
    streams: count = 1,
    slotsPerStream = 10,
    batchSize = 10,
  }: SetupOptions = {}) {
    const group = `group:${crypto.randomUUID()}`;
    const consumer = 'consumer';
    const streams = new Streams(slotsPerStream);
    const names: string[] = [];

    for (let i = 0; i < count; i++) {
      const name = `stream:${crypto.randomUUID()}`;

      await redis.xGroupCreate(name, group, '0', { MKSTREAM: true });
      streams.register(name, TestEvent, 'handler', vi.fn());
      names.push(name);
    }

    const process = vi.fn(
      (..._args: Parameters<Processor['process']>): Promise<void> =>
        Promise.resolve(),
    );
    const loop = new ReadLoop(
      clients,
      streams,
      { process } as unknown as Processor,
      { group, consumer, batchSize, blockTimeout: 50 },
    );

    return {
      loop,
      streams,
      process,
      group,
      consumer,
      names,
      next: (signal = new AbortController().signal) => loop.next(signal),
      add: async (name: string, n = 1) => {
        const ids: string[] = [];

        for (let i = 0; i < n; i++) {
          ids.push(await redis.xAdd(name, '*', { payload: `${i}` }));
        }

        return ids;
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

    clients = new Clients<'read'>(url);
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
  });

  describe('reading', () => {
    it('runs every new entry with the first attempt', async () => {
      const { next, add, process, names, streamOf } = await setup();
      const [name] = names as [string];
      const ids = await add(name, 2);

      await next();

      expect(process).toHaveBeenCalledTimes(2);
      expect(process).toHaveBeenCalledWith(
        streamOf(name),
        { id: ids[0], message: { payload: '0' } },
        1,
      );
      expect(process).toHaveBeenCalledWith(
        streamOf(name),
        { id: ids[1], message: { payload: '1' } },
        1,
      );
    });

    it('reads the entries of several streams at once', async () => {
      const { next, add, process, names, streamOf } = await setup({
        streams: 2,
      });
      const [a, b] = names as [string, string];
      const [idA] = await add(a);
      const [idB] = await add(b);

      await next();

      expect(process).toHaveBeenCalledTimes(2);
      expect(process).toHaveBeenCalledWith(
        streamOf(a),
        expect.objectContaining({ id: idA }),
        1,
      );
      expect(process).toHaveBeenCalledWith(
        streamOf(b),
        expect.objectContaining({ id: idB }),
        1,
      );
    });

    it('runs nothing when the read times out without entries', async () => {
      const { next, process } = await setup({ streams: 2 });

      await next();

      expect(process).not.toHaveBeenCalled();
    });

    it('does not call Redis when it is aborted', async () => {
      const { next, add, process, names } = await setup();
      const [name] = names as [string];
      const client = await clients.get('read');
      const xReadGroup = vi.spyOn(client, 'xReadGroup');
      const controller = new AbortController();

      await add(name);
      controller.abort();
      await next(controller.signal);

      expect(xReadGroup).not.toHaveBeenCalled();
      expect(process).not.toHaveBeenCalled();
    });
  });

  describe('slots', () => {
    it('does not read a stream that has no free slot', async () => {
      const { next, add, process, names, streamOf, pending } = await setup({
        streams: 2,
        slotsPerStream: 1,
      });
      const [full, free] = names as [string, string];
      const held = Promise.withResolvers<void>();

      await add(full);
      process.mockReturnValueOnce(held.promise);
      await next();
      process.mockClear();

      const [idFull] = (await add(full)) as [string];
      const [idFree] = (await add(free)) as [string];

      await next();

      expect(process).toHaveBeenCalledExactlyOnceWith(
        streamOf(free),
        expect.objectContaining({ id: idFree }),
        1,
      );
      expect(await pending(full)).toHaveLength(1);
      expect(await pending(full)).not.toContainEqual(
        expect.objectContaining({ id: idFull }),
      );

      held.resolve();
    });

    it('reads the stream again once its slot is free', async () => {
      const { next, add, process, names, streamOf, streams } = await setup({
        slotsPerStream: 1,
      });
      const [name] = names as [string];
      const held = Promise.withResolvers<void>();

      await add(name);
      process.mockReturnValueOnce(held.promise);
      await next();
      process.mockClear();

      const [id] = (await add(name)) as [string];

      held.resolve();
      await streams.drain();
      await next();

      expect(process).toHaveBeenCalledExactlyOnceWith(
        streamOf(name),
        expect.objectContaining({ id }),
        1,
      );
    });
  });

  describe('options', () => {
    it.each([1, 5, 10])(
      'reads at most batch size entries of a stream at once',
      async (i) => {
        const { next, add, process, names, pending } = await setup({
          batchSize: i,
        });
        const [name] = names as [string];

        await add(name, i * 2);
        await next();

        expect(process).toHaveBeenCalledTimes(i);
        expect(await pending(name)).toHaveLength(i);
      },
    );
  });
});
