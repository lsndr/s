import { NoopTelemetry } from '../../telemetry/noop.telemetry.js';
import { hostname } from 'node:os';
import { Logger } from '@nestjs/common';
import { createClient, type RedisClientType } from 'redis';
import { GenericContainer, Wait } from 'testcontainers';
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import {
  RedisStreamsTransport,
  type RedisStreamsTransportConfig,
} from './transport.js';
import { Streams } from './streams.js';
import { JsonSerializer } from '../../serialization/json.serializer.js';
const serializer = new JsonSerializer();

await serializer.onBusInit();

class FirstEvent {
  public constructor(public readonly value: string) {}
}

async function publishMany(
  transport: RedisStreamsTransport,
  count: number,
): Promise<void> {
  for (let i = 0; i < count; i++) {
    await transport.publish(new FirstEvent(String(i)));
  }
}

describe('RedisStreamsTransport', { timeout: 60_000 }, () => {
  let container: Awaited<ReturnType<GenericContainer['start']>>;
  let url: string;
  let redis: RedisClientType;
  const transports: RedisStreamsTransport[] = [];
  const shutDown = new WeakSet<RedisStreamsTransport>();
  // resolvers of handlers that a test leaves running
  const holds: (() => void)[] = [];

  async function shutdown(transport: RedisStreamsTransport) {
    if (!shutDown.has(transport)) {
      shutDown.add(transport);
      await transport.onBusShutdown();
    }
  }

  /**
   * Creates a transport with a random group and key prefix, so tests don't
   * see each other's streams.
   */
  async function createTransport(
    config: Partial<RedisStreamsTransportConfig> = {},
  ) {
    const group = config.group ?? `group:${crypto.randomUUID()}`;
    const keyPrefix = config.keyPrefix ?? `${crypto.randomUUID()}:`;
    const transport = new RedisStreamsTransport(
      {
        url,
        retries: { max: 3, delay: 100 },
        reclaimInterval: 25,
        ...config,
        group,
        keyPrefix,
      } satisfies RedisStreamsTransportConfig,
      serializer,
      new NoopTelemetry(),
    );

    transports.push(transport);
    await transport.onBusInit();

    return {
      group,
      keyPrefix,
      transport,
      stream: (event: { name: string }) => `${keyPrefix}${event.name}`,
    };
  }

  async function entriesOf(key: string) {
    return (await redis.xRange(key, '-', '+')) ?? [];
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
    url = `redis://${container.getHost()}:${container.getMappedPort(6379)}`;
    redis = createClient({ url });
    await redis.connect();
  }, 60_000);

  afterAll(async () => {
    await redis.close();
    await container.stop();
  });

  afterEach(async () => {
    vi.restoreAllMocks();

    for (const resolve of holds.splice(0)) {
      resolve();
    }

    for (const transport of transports.splice(0)) {
      await shutdown(transport);
    }
  });

  describe('subscribe', () => {
    it('names the stream after the key prefix and the event', async () => {
      // arrange
      const keyPrefix = `custom:${crypto.randomUUID()}:`;
      const { transport } = await createTransport({ keyPrefix });

      // act
      await transport.subscribe(FirstEvent, 'Handler', vi.fn());

      // assert
      expect(await redis.exists(`${keyPrefix}FirstEvent`)).toBe(1);
    });

    it('joins a group that already exists', async () => {
      // arrange
      const { transport, group, stream } = await createTransport();
      const other = await createTransport({
        group,
        keyPrefix: stream(FirstEvent).replace(/FirstEvent$/, ''),
      });
      await transport.subscribe(FirstEvent, 'Handler', vi.fn());

      // act
      const subscribed = other.transport.subscribe(
        FirstEvent,
        'Handler',
        vi.fn(),
      );

      // assert
      await expect(subscribed).resolves.toBeUndefined();
    });

    it('rejects a handler that is already subscribed to the event', async () => {
      // arrange
      const { transport } = await createTransport();
      await transport.subscribe(FirstEvent, 'Handler', vi.fn());

      // act
      const subscribed = transport.subscribe(FirstEvent, 'Handler', vi.fn());

      // assert
      await expect(subscribed).rejects.toThrow(/already subscribed/);
    });
  });

  describe('publish', () => {
    it('trims the acknowledged entries above the max length', async () => {
      // arrange
      const { transport, group, stream } = await createTransport({
        trim: { maxLength: 10 },
      });
      const key = stream(FirstEvent);
      const count = 500;

      await redis.xGroupCreate(key, group, '0', { MKSTREAM: true });
      await publishMany(transport, count);
      const entries = await entriesOf(key);
      await redis.xReadGroup(group, 'consumer', { key, id: '>' });
      await redis.xAck(
        key,
        group,
        entries.map(({ id }) => id),
      );

      // act
      await transport.publish(new FirstEvent('trigger'));

      // assert
      expect(await redis.xLen(key)).toBeLessThan(count);
    });

    it('keeps the unacknowledged entries above the max length', async () => {
      // arrange
      const { transport, group, stream } = await createTransport({
        trim: { maxLength: 10 },
      });
      const key = stream(FirstEvent);
      const count = 500;

      await redis.xGroupCreate(key, group, '0', { MKSTREAM: true });

      // act
      await publishMany(transport, count);

      // assert
      expect(await redis.xLen(key)).toBe(count);
    });

    it('trims unacknowledged entries above the max length when not only acknowledged', async () => {
      // arrange
      const { transport, group, stream } = await createTransport({
        trim: { maxLength: 10, onlyAcked: false },
      });
      const key = stream(FirstEvent);
      const count = 500;

      await redis.xGroupCreate(key, group, '0', { MKSTREAM: true });

      // act
      await publishMany(transport, count);

      // assert
      expect(await redis.xLen(key)).toBeLessThan(count);
    });
  });

  describe('config', () => {
    it.each([
      ['retries.max', { retries: { max: -1 } }],
      ['retries.delay', { retries: { delay: 10 } }],
      ['concurrency', { concurrency: 0 }],
      ['trim.maxLength', { trim: { maxLength: 0 } }],
      ['reclaimInterval', { reclaimInterval: 0 }],
      ['shutdownTimeout', { shutdownTimeout: 0 }],
    ] satisfies [string, Partial<RedisStreamsTransportConfig>][])(
      'rejects an invalid %s',
      (option, config) => {
        // act
        const create = () =>
          new RedisStreamsTransport(
            { url, group: 'group', ...config },
            serializer,
            new NoopTelemetry(),
          );

        // assert
        expect(create).toThrow(`Invalid Redis Streams option ${option}`);
      },
    );
  });

  describe('consumer', () => {
    it('reads with the configured name', async () => {
      // arrange
      const { transport, group, stream } = await createTransport({
        consumer: 'my-consumer',
      });

      await transport.subscribe(FirstEvent, 'Handler', vi.fn());
      transport.onBusBootstrap();

      // act
      await transport.publish(new FirstEvent('x'));

      // assert
      await vi.waitFor(async () => {
        const consumers = await redis.xInfoConsumers(stream(FirstEvent), group);
        expect(consumers.map(({ name }) => name)).toEqual(['my-consumer']);
      });
    });

    it('defaults the name to the host name and the process id', async () => {
      // arrange
      const { transport, group, stream } = await createTransport();

      await transport.subscribe(FirstEvent, 'Handler', vi.fn());
      transport.onBusBootstrap();

      // act
      await transport.publish(new FirstEvent('x'));

      // assert
      await vi.waitFor(async () => {
        const consumers = await redis.xInfoConsumers(stream(FirstEvent), group);
        expect(consumers.map(({ name }) => name)).toEqual([
          `${hostname()}-${process.pid}`,
        ]);
      });
    });
  });

  describe('shutdown', () => {
    it('waits for the entries in flight and removes the consumer', async () => {
      // arrange
      const { transport, group, stream } = await createTransport();
      const started = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const drain = Streams.prototype.drain;
      let finished = false;

      // lets the entry finish once shutdown has started to wait for it
      vi.spyOn(Streams.prototype, 'drain').mockImplementation(function (
        this: Streams,
        ...args
      ) {
        release.resolve();

        return drain.apply(this, args);
      });
      await transport.subscribe(FirstEvent, 'Slow', async () => {
        started.resolve();
        await release.promise;
        finished = true;
      });
      transport.onBusBootstrap();
      await transport.publish(new FirstEvent('x'));
      await started.promise;

      // act
      await shutdown(transport);

      // assert
      expect(finished).toBe(true);
      expect(await redis.xInfoConsumers(stream(FirstEvent), group)).toEqual([]);
    });

    it('stops waiting after the shutdown timeout and keeps the entry pending', async () => {
      // arrange
      const warn = vi.spyOn(Logger.prototype, 'warn').mockReturnValue();
      vi.spyOn(Logger.prototype, 'error').mockReturnValue();
      const { transport, group, stream } = await createTransport({
        shutdownTimeout: 100,
        consumer: 'stuck-consumer',
      });
      const started = Promise.withResolvers<void>();
      const stuck = Promise.withResolvers<void>();

      holds.push(stuck.resolve);
      await transport.subscribe(FirstEvent, 'Stuck', () => {
        started.resolve();

        return stuck.promise;
      });
      transport.onBusBootstrap();
      await transport.publish(new FirstEvent('x'));
      await started.promise;

      // act
      await shutdown(transport);

      // assert
      const pending = await redis.xPending(stream(FirstEvent), group);
      const consumers = await redis.xInfoConsumers(stream(FirstEvent), group);
      expect(pending.pending).toBe(1);
      expect(consumers.map(({ name }) => name)).toEqual(['stuck-consumer']);
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining('Shutdown timeout reached'),
      );
    });
  });
});
