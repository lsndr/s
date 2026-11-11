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
import { Logger } from '@nestjs/common';
import { createClient, type RedisClientType } from 'redis';
import { Backlog } from './backlog.js';
import { Clients } from './clients.js';
import { Handler } from './handler.js';
import { Streams } from './streams.js';

class TestEvent {}

describe('Backlog', { timeout: 60_000 }, () => {
  let container: Awaited<ReturnType<GenericContainer['start']>>;
  let clients: Clients<'command'>;
  let redis: RedisClientType;

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

    clients = new Clients(url);
    redis = createClient({ url });
    await redis.connect();
  });

  afterAll(async () => {
    await redis.close();
    await clients.destroy();
    await container.stop();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /** A stream with `added` entries, of which the group has read `read`. */
  async function setup({ added = 0, read = 0, create = true } = {}) {
    const name = `stream:${crypto.randomUUID()}`;
    const group = `group:${crypto.randomUUID()}`;
    const streams = new Streams(10);

    if (create) {
      await redis.xGroupCreate(name, group, '0', { MKSTREAM: true });
    }

    for (let i = 0; i < added; i++) {
      await redis.xAdd(name, '*', { payload: 'x' });
    }

    if (read > 0) {
      await redis.xReadGroup(
        group,
        'consumer',
        { key: name, id: '>' },
        { COUNT: read },
      );
    }

    streams.register(name, TestEvent, new Handler('handler', vi.fn()));

    const server = { address: 'localhost', port: 6379 };
    const backlog = new Backlog(clients, streams, { group, server });
    const attributes = [
      { name: ['system'], value: 'redis' },
      { name: ['destination', 'name'], value: name },
      { name: ['consumer', 'group', 'name'], value: group },
      { name: ['server', 'address'], value: 'localhost' },
      { name: ['server', 'port'], value: 6379 },
    ];

    return { backlog, streams, name, attributes };
  }

  it('observes the slots of each stream', async () => {
    // arrange
    const { backlog, attributes } = await setup();

    // act
    const observations = backlog.slots();

    // assert
    expect(observations).toEqual([{ value: 10, attributes }]);
  });

  it('observes how long each stream had no free slot', async () => {
    // arrange
    const { backlog, attributes } = await setup();

    // act
    const observations = backlog.fullDuration();

    // assert
    expect(observations).toEqual([{ value: 0, attributes }]);
  });

  it('observes the entries delivered and not acknowledged as pending', async () => {
    // arrange
    const { backlog, attributes } = await setup({ added: 3, read: 2 });

    // act
    const observations = await backlog.pending();

    // assert
    expect(observations).toEqual([{ value: 2, attributes }]);
  });

  it('observes the entries not delivered yet as the lag', async () => {
    // arrange
    const { backlog, attributes } = await setup({ added: 3, read: 2 });

    // act
    const observations = await backlog.lag();

    // assert
    expect(observations).toEqual([{ value: 1, attributes }]);
  });

  it('observes the entries in flight', async () => {
    // arrange
    const { backlog, streams, name, attributes } = await setup();

    streams.run(name, '1-0', () => new Promise(() => undefined));

    // act
    const observations = backlog.inflight();

    // assert
    expect(observations).toEqual([{ value: 1, attributes }]);
  });

  it('asks Redis once for gauges observed together', async () => {
    // arrange
    const { backlog } = await setup({ added: 1 });
    const client = await clients.get('command');
    const xInfoGroups = vi.spyOn(client, 'xInfoGroups');

    // act
    await Promise.all([backlog.pending(), backlog.lag()]);

    // assert
    expect(xInfoGroups).toHaveBeenCalledTimes(1);
  });

  it('observes nothing for a stream that cannot be queried', async () => {
    // arrange
    const { backlog } = await setup({ create: false });

    vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});

    // act
    const observations = await backlog.pending();

    // assert
    expect(observations).toEqual([]);
  });
});
