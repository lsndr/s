import { setTimeout as sleep } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import { GenericContainer, Wait } from 'testcontainers';
import { createClient, type RedisClientType } from 'redis';
import { Clients } from '../clients.js';
import { Handler } from '../handler.js';
import { Streams } from '../streams.js';
import { StaleConsumersLoop } from './stale-consumers.js';

class TestEvent {}

interface SetupOptions {
  /** Number of streams. Defaults to 1. */
  streams?: number;
  consumerTtl?: number;
}

describe('StaleConsumersLoop', { timeout: 60_000 }, () => {
  let container: Awaited<ReturnType<GenericContainer['start']>>;
  let clients: Clients<'command'>;
  let redis: RedisClientType;

  /** Creates streams that are registered in `Streams`, and a loop cleaning their groups. */
  async function setup({
    streams: count = 1,
    consumerTtl = 100,
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

    const loop = new StaleConsumersLoop(clients, streams, {
      group,
      consumer,
      consumerTtl,
    });

    return {
      loop,
      group,
      consumer,
      names,
      next: (signal = new AbortController().signal) => loop.next(signal),
      /** Adds a consumer that has no pending entries. */
      idle: async (name: string, consumerName: string) => {
        await redis.xGroupCreateConsumer(name, group, consumerName);
      },
      /** Adds a consumer that has a pending entry. */
      busy: async (name: string, consumerName: string) => {
        await redis.xAdd(name, '*', { payload: '0' });
        await redis.xReadGroup(group, consumerName, { key: name, id: '>' });
      },
      /** Waits until the consumers have been idle for longer than the TTL. */
      stale: async () => {
        await sleep(consumerTtl + 50);
      },
      consumers: async (name: string) =>
        (await redis.xInfoConsumers(name, group)).map(({ name }) => name),
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

  beforeEach(async () => {
    await redis.scriptFlush();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('deleting', () => {
    it('deletes the consumers without pending entries that are idle for long enough', async () => {
      const { next, idle, stale, consumers, names } = await setup();
      const [name] = names as [string];

      await idle(name, 'gone');
      await stale();
      await next();

      expect(await consumers(name)).toEqual([]);
    });

    it('does not delete the consumers that are not idle for long enough', async () => {
      const { next, idle, consumers, names } = await setup({
        consumerTtl: 10_000,
      });
      const [name] = names as [string];

      await idle(name, 'recent');
      await next();

      expect(await consumers(name)).toEqual(['recent']);
    });

    it('does not delete the consumers that have pending entries', async () => {
      const { next, busy, stale, consumers, names } = await setup();
      const [name] = names as [string];

      await busy(name, 'working');
      await stale();
      await next();

      expect(await consumers(name)).toEqual(['working']);
    });

    it('does not delete its own consumer', async () => {
      const { next, idle, stale, consumers, consumer, names } = await setup();
      const [name] = names as [string];

      await idle(name, consumer);
      await idle(name, 'gone');
      await stale();
      await next();

      expect(await consumers(name)).toEqual([consumer]);
    });

    it('deletes the stale consumers of several streams', async () => {
      const { next, idle, stale, consumers, names } = await setup({
        streams: 2,
      });
      const [a, b] = names as [string, string];

      await idle(a, 'gone');
      await idle(b, 'gone');
      await stale();
      await next();

      expect(await consumers(a)).toEqual([]);
      expect(await consumers(b)).toEqual([]);
    });

    it('logs the deleted consumers', async () => {
      const { next, idle, stale, group, names } = await setup();
      const [name] = names as [string];
      const log = vi.spyOn(Logger.prototype, 'log').mockReturnValue();

      await idle(name, 'gone');
      await stale();
      await next();

      expect(log).toHaveBeenCalledExactlyOnceWith(
        `Removed stale consumer gone of group ${group}`,
      );
    });
  });

  describe('abort', () => {
    it('does not call Redis when it is aborted', async () => {
      const { next, idle, stale, consumers, names } = await setup();
      const [name] = names as [string];
      const client = await clients.get('command');
      const evalSha = vi.spyOn(client, 'evalSha');
      const evalScript = vi.spyOn(client, 'eval');
      const controller = new AbortController();

      await idle(name, 'gone');
      await stale();
      controller.abort();
      await next(controller.signal);

      expect(evalSha).not.toHaveBeenCalled();
      expect(evalScript).not.toHaveBeenCalled();
      expect(await consumers(name)).toEqual(['gone']);
    });

    it('does not move on to the next stream when aborted while cleaning one', async () => {
      const { next, idle, stale, consumers, names } = await setup({
        streams: 2,
      });
      const [first, second] = names as [string, string];
      const client = await clients.get('command');
      const original = client.evalSha.bind(client);
      const controller = new AbortController();

      await idle(first, 'gone');
      await idle(second, 'gone');
      vi.spyOn(client, 'evalSha').mockImplementation(async (...args) => {
        try {
          return await original(...args);
        } finally {
          controller.abort();
        }
      });
      await stale();
      await next(controller.signal);

      expect(await consumers(first)).toEqual([]);
      expect(await consumers(second)).toEqual(['gone']);
    });

    it('does not log the errors that happen after it is aborted', async () => {
      const { next, names } = await setup();
      const [name] = names as [string];
      const client = await clients.get('command');
      const error = vi.spyOn(Logger.prototype, 'error').mockReturnValue();
      const controller = new AbortController();

      vi.spyOn(client, 'evalSha').mockImplementation(() => {
        controller.abort();

        return Promise.reject(new Error(`Connection lost: ${name}`));
      });

      await expect(next(controller.signal)).resolves.toBeUndefined();
      expect(error).not.toHaveBeenCalled();
    });
  });

  describe('errors', () => {
    it('logs a failing stream and still cleans the others', async () => {
      const { next, idle, stale, consumers, group, names } = await setup({
        streams: 2,
      });
      const [failing, working] = names as [string, string];
      const error = vi.spyOn(Logger.prototype, 'error').mockReturnValue();

      await redis.xGroupDestroy(failing, group);
      await idle(working, 'gone');
      await stale();
      await next();

      expect(error).toHaveBeenCalledExactlyOnceWith(
        expect.stringContaining(failing),
        expect.any(String),
      );
      expect(await consumers(working)).toEqual([]);
    });
  });

  describe('interval', () => {
    it.each([
      { consumerTtl: 1000, expected: 500 },
      { consumerTtl: 3_600_000, expected: 1_800_000 },
    ])(
      'is $expected ms for a consumer TTL of $consumerTtl ms',
      async ({ consumerTtl, expected }) => {
        const { loop } = await setup({ consumerTtl });

        expect(loop.interval()).toBe(expected);
      },
    );
  });
});
