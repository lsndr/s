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
import { Logger } from '@nestjs/common';
import { Clients } from '../clients.js';
import { type Processor } from '../processor.js';
import { Handler } from '../handler.js';
import { Streams } from '../streams.js';
import { ReadLoop } from './read.js';

class TestEvent {}

interface SetupOptions {
  /** Number of streams. Defaults to 1. */
  streams?: number;
  slotsPerStream?: number;
  batchSize?: number;
  blockTimeout?: number;
}

describe('ReadLoop', { timeout: 60_000 }, () => {
  let container: Awaited<ReturnType<GenericContainer['start']>>;
  let clients: Clients<'read' | 'command'>;
  let redis: RedisClientType;
  // cleanups of what a test leaves running, they run after the test
  const cleanups: (() => Promise<void> | void)[] = [];

  async function setup({
    streams: count = 1,
    slotsPerStream = 10,
    batchSize = 10,
    blockTimeout = 50,
  }: SetupOptions = {}) {
    const group = `group:${crypto.randomUUID()}`;
    const consumer = 'consumer';
    const streams = new Streams(slotsPerStream);
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
    const loop = new ReadLoop(
      clients,
      streams,
      { process } as unknown as Processor,
      { group, consumer, batchSize, blockTimeout },
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
      register: (name: string) =>
        streams.register(name, TestEvent, new Handler('handler', vi.fn())),
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

  async function blocked() {
    await vi.waitFor(async () => {
      const list = await redis.clientList();

      expect(list.some((c) => c.cmd === 'xreadgroup')).toBe(true);
    });
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

    clients = new Clients<'read' | 'command'>(url);
    redis = createClient({ url });
    await redis.connect();
  }, 60_000);

  afterAll(async () => {
    await redis.close();
    await clients.destroy();
    await container.stop();
  });

  afterEach(async () => {
    vi.restoreAllMocks();

    for (const cleanup of cleanups.splice(0)) {
      await cleanup();
    }
  });

  describe('reading', () => {
    it('runs every new entry with the first attempt', async () => {
      // arrange
      const { next, add, process, names, streamOf } = await setup();
      const [name] = names as [string];
      const ids = await add(name, 2);

      // act
      await next();

      // assert
      expect(process).toHaveBeenCalledTimes(2);
      expect(process).toHaveBeenCalledWith(
        streamOf(name),
        { id: ids[0], message: { payload: Buffer.from('0') } },
        1,
      );
      expect(process).toHaveBeenCalledWith(
        streamOf(name),
        { id: ids[1], message: { payload: Buffer.from('1') } },
        1,
      );
    });

    it('reads the entries of several streams at once', async () => {
      // arrange
      const { next, add, process, names, streamOf } = await setup({
        streams: 2,
      });
      const [a, b] = names as [string, string];
      const [idA] = await add(a);
      const [idB] = await add(b);

      // act
      await next();

      // assert
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
      // arrange
      const { next, process } = await setup({ streams: 2 });

      // act
      await next();

      // assert
      expect(process).not.toHaveBeenCalled();
    });

    it('does not call Redis when it is aborted', async () => {
      // arrange
      const { next, add, process, names } = await setup();
      const [name] = names as [string];
      const client = await clients.getBinary('read');
      const xReadGroup = vi.spyOn(client, 'xReadGroup');
      const controller = new AbortController();

      await add(name);
      controller.abort();

      // act
      await next(controller.signal);

      // assert
      expect(xReadGroup).not.toHaveBeenCalled();
      expect(process).not.toHaveBeenCalled();
    });
  });

  describe('slots', () => {
    it('does not read a stream that has no free slot', async () => {
      // arrange
      const { next, add, process, names, streamOf, pending } = await setup({
        streams: 2,
        slotsPerStream: 1,
      });
      const [full, free] = names as [string, string];
      const held = Promise.withResolvers<void>();

      cleanups.push(held.resolve);
      await add(full);
      process.mockReturnValueOnce(held.promise);
      await next();
      process.mockClear();

      const [idFull] = (await add(full)) as [string];
      const [idFree] = (await add(free)) as [string];

      // act
      await next();

      // assert
      expect(process).toHaveBeenCalledExactlyOnceWith(
        streamOf(free),
        expect.objectContaining({ id: idFree }),
        1,
      );
      expect(await pending(full)).toHaveLength(1);
      expect(await pending(full)).not.toContainEqual(
        expect.objectContaining({ id: idFull }),
      );
    });

    it('reads the stream again once its slot is free', async () => {
      // arrange
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

      // act
      await next();

      // assert
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
        // arrange
        const { next, add, process, names, pending } = await setup({
          batchSize: i,
        });
        const [name] = names as [string];

        await add(name, i * 2);

        // act
        await next();

        // assert
        expect(process).toHaveBeenCalledTimes(i);
        expect(await pending(name)).toHaveLength(i);
      },
    );
  });

  describe('unblocking', () => {
    const BLOCK = 10_000;

    async function block(options: SetupOptions = {}) {
      const ctx = await setup({ ...options, blockTimeout: BLOCK });
      const started = performance.now();
      const reading = ctx.next();

      await blocked();

      return { ...ctx, reading, elapsed: () => performance.now() - started };
    }

    it('returns right away when a stream is registered', async () => {
      // arrange
      const { reading, register, elapsed } = await block();

      // act
      register(`stream:${crypto.randomUUID()}`);
      await reading;

      // assert
      expect(elapsed()).toBeLessThan(BLOCK / 2);
    });

    it('returns right away when a slot of a stream outside of the read is released', async () => {
      // arrange
      const ctx = await setup({
        streams: 2,
        slotsPerStream: 1,
        blockTimeout: BLOCK,
      });
      const [full] = ctx.names as [string];
      const held = Promise.withResolvers<void>();

      cleanups.push(held.resolve);

      // fills the slot of the first stream, so only the second one is read
      await ctx.add(full);
      ctx.process.mockReturnValueOnce(held.promise);
      await ctx.next(AbortSignal.abort());
      ctx.streams.run(full, 'held', () => held.promise);

      const started = performance.now();
      const reading = ctx.next();

      await blocked();

      // act
      held.resolve();
      await reading;

      // assert
      expect(performance.now() - started).toBeLessThan(BLOCK / 2);
    });

    it('keeps blocking when a stream of the read becomes full', async () => {
      // arrange
      const { reading, streams, names } = await block({ slotsPerStream: 1 });
      const [name] = names as [string];
      const never = new Promise<void>(() => undefined);

      // ends the blocked read of the test
      cleanups.push(async () => {
        await redis.clientUnblock(clients.id('read')!, 'TIMEOUT');
        await reading;
      });

      // act
      streams.run(name, '1', () => never);

      const result = await Promise.race([
        reading.then(() => 'returned'),
        new Promise((resolve) => setTimeout(resolve, 300, 'blocked')),
      ]);

      // assert
      expect(result).toBe('blocked');
    });

    it('warns and waits for the timeout when the id of the read client is unknown', async () => {
      // arrange
      const warn = vi
        .spyOn(Logger.prototype, 'warn')
        .mockImplementation(() => undefined);

      vi.spyOn(clients, 'id').mockReturnValue(undefined);

      const { next, register } = await setup({ blockTimeout: 300 });
      const reading = next();

      await blocked();

      // act
      register(`stream:${crypto.randomUUID()}`);
      await reading;

      // assert
      expect(warn).toHaveBeenCalled();
    });

    it('unblocks again when the read was not blocked yet', async () => {
      // arrange
      const command = await clients.get('command');
      const clientUnblock = vi.spyOn(command, 'clientUnblock');

      // as if the command reached Redis before the read blocked
      clientUnblock.mockResolvedValueOnce(0);

      const { reading, register, elapsed } = await block();

      // act
      register(`stream:${crypto.randomUUID()}`);
      await reading;

      // assert
      expect(clientUnblock).toHaveBeenCalledTimes(2);
      expect(elapsed()).toBeLessThan(BLOCK / 2);
    });

    it('processes the entries when the unblock fails', async () => {
      // arrange
      const warn = vi
        .spyOn(Logger.prototype, 'warn')
        .mockImplementation(() => undefined);
      const command = await clients.get('command');

      vi.spyOn(command, 'clientUnblock').mockRejectedValue(new Error('lost'));

      const { reading, register, add, names, process } = await block();
      const [name] = names as [string];

      // act
      register(`stream:${crypto.randomUUID()}`);
      await add(name);
      await reading;

      // assert
      expect(warn).toHaveBeenCalledOnce();
      expect(process).toHaveBeenCalledOnce();
    });
  });
});
