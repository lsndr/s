import { setTimeout as sleep } from 'node:timers/promises';
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
import { Streams } from '../streams.js';
import { RefreshLoop } from './refresh.js';

class TestEvent {}

interface SetupOptions {
  /** Number of streams. Defaults to 1. */
  streams?: number;
  retriesDelay?: number;
}

describe('RefreshLoop', { timeout: 60_000 }, () => {
  let container: Awaited<ReturnType<GenericContainer['start']>>;
  let clients: Clients<'command'>;
  let redis: RedisClientType;
  const releases: (() => void)[] = [];

  /** Creates streams that are registered in `Streams`, and a loop refreshing them. */
  async function setup({
    streams: count = 1,
    retriesDelay = 1000,
  }: SetupOptions = {}) {
    const group = `group:${crypto.randomUUID()}`;
    const consumer = 'consumer';
    const streams = new Streams(10);
    const names: string[] = [];

    for (let i = 0; i < count; i++) {
      const name = `stream:${crypto.randomUUID()}`;

      await redis.xGroupCreate(name, group, '0', { MKSTREAM: true });
      streams.register(name, TestEvent, 'handler', vi.fn());
      names.push(name);
    }

    const loop = new RefreshLoop(clients, streams, {
      group,
      consumer,
      retriesDelay,
    });

    return {
      loop,
      streams,
      group,
      consumer,
      names,
      next: () => loop.next(),
      /** Adds entries and makes them pending for the consumer. */
      deliver: async (name: string, n = 1) => {
        for (let i = 0; i < n; i++) {
          await redis.xAdd(name, '*', { payload: `${i}` });
        }

        const reply =
          (await redis.xReadGroup(
            group,
            consumer,
            { key: name, id: '>' },
            { COUNT: n },
          )) ?? [];

        const ids: string[] = [];

        for (const { messages } of reply) {
          for (const entry of messages) {
            ids.push(entry.id);
          }
        }

        return ids;
      },
      /** Lists the given entries as in flight until released. */
      hold: (name: string, ids: string[]) => {
        const held = Promise.withResolvers<void>();

        for (const id of ids) {
          streams.run(name, id, () => held.promise);
        }

        releases.push(() => held.resolve());
      },
      pending: async (name: string) =>
        await redis.xPendingRange(name, group, '-', '+', 100),
    };
  }

  beforeAll(async () => {
    container = await new GenericContainer('redis:8.2')
      .withExposedPorts(6379)
      .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
      .start();

    const url = `redis://${container.getHost()}:${container.getMappedPort(6379)}`;

    clients = new Clients<'command'>(url);
    redis = createClient({ url });
    await redis.connect();
  }, 60_000);

  afterAll(async () => {
    await redis.close();
    await clients.close();
    await container.stop();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    releases.splice(0).forEach((release) => release());
  });

  describe('refreshing', () => {
    it('resets the idle time of the entries in flight', async () => {
      const { next, deliver, hold, pending, names } = await setup();
      const [name] = names as [string];
      const ids = await deliver(name, 2);
      hold(name, ids);
      await sleep(100);

      const before = await pending(name);
      await next();
      const after = await pending(name);

      for (const entry of after) {
        const previous = before.find(({ id }) => id === entry.id);

        expect(entry.millisecondsSinceLastDelivery).toBeLessThan(
          previous?.millisecondsSinceLastDelivery ?? 0,
        );
      }
    });

    it('does not refresh the entries that are not in flight', async () => {
      const { next, deliver, hold, pending, names } = await setup();
      const [name] = names as [string];
      const [running, idle] = (await deliver(name, 2)) as [string, string];
      hold(name, [running]);

      await sleep(500);
      await next();

      const entries = await pending(name);
      const runningEntries = entries.find(({ id }) => id === running);
      const idleEntries = entries.find(({ id }) => id === idle);

      expect(runningEntries?.millisecondsSinceLastDelivery).toBeLessThan(500);
      expect(idleEntries?.millisecondsSinceLastDelivery).toBeGreaterThanOrEqual(
        500,
      );
    });

    it('does not count a refresh as a delivery', async () => {
      const { next, deliver, hold, pending, names } = await setup();
      const [name] = names as [string];
      const ids = await deliver(name);

      hold(name, ids);
      await next();
      await next();

      expect(await pending(name)).toEqual([
        expect.objectContaining({ id: ids[0], deliveriesCounter: 1 }),
      ]);
    });

    it('ignores the entries in flight that are no longer pending', async () => {
      const { next, deliver, hold, pending, names, group } = await setup();
      const [name] = names as [string];
      const ids = await deliver(name);

      hold(name, ids);
      await redis.xAck(name, group, ids);

      await expect(next()).resolves.toBeUndefined();
      expect(await pending(name)).toHaveLength(0);
    });

    it('does not call Redis for a stream without entries in flight', async () => {
      const { next, deliver, names } = await setup();
      const [name] = names as [string];
      const client = await clients.get('command');
      const xClaimJustId = vi.spyOn(client, 'xClaimJustId');

      await deliver(name);
      await next();

      expect(xClaimJustId).not.toHaveBeenCalled();
    });
  });

  describe('interval', () => {
    it.each([
      { retriesDelay: 1000, expected: 500 },
      { retriesDelay: 30, expected: 15 },
      { retriesDelay: 10, expected: 10 },
      { retriesDelay: 0, expected: 10 },
    ])(
      'is $expected ms for a retries delay of $retriesDelay ms',
      async ({ retriesDelay, expected }) => {
        const { loop } = await setup({ retriesDelay });

        expect(loop.interval()).toBe(expected);
      },
    );
  });
});
