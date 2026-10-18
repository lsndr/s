import { hostname } from 'node:os';
import { Logger } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
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
import { createOtelFixture } from '../../telemetry/otel/otel.fixture.js';
import { type BusTelemetry, Telemetry } from '../../telemetry/telemetry.js';
import { TransportConfig } from '../transport.js';
import {
  RedisStreamsTransport,
  type RedisStreamsTransportConfig,
} from './transport.js';
import { Streams } from './streams.js';

class FirstEvent {
  public constructor(public readonly value: string) {}
}

describe('RedisStreamsTransport', { timeout: 60_000 }, () => {
  let container: Awaited<ReturnType<GenericContainer['start']>>;
  let url: string;
  let redis: RedisClientType;
  const modules: TestingModule[] = [];
  const shutDown = new WeakSet<RedisStreamsTransport>();

  /** Shuts the transport down once, as the bus does. */
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
    telemetry?: BusTelemetry,
  ) {
    const group = config.group ?? `group:${crypto.randomUUID()}`;
    const keyPrefix = config.keyPrefix ?? `${crypto.randomUUID()}:`;
    const module = await Test.createTestingModule({
      providers: [
        RedisStreamsTransport,
        {
          provide: TransportConfig,
          useValue: {
            url,
            retries: { max: 3, delay: 100 },
            reclaimInterval: 25,
            ...config,
            group,
            keyPrefix,
          } satisfies RedisStreamsTransportConfig,
        },
        ...(telemetry ? [{ provide: Telemetry, useValue: telemetry }] : []),
      ],
    }).compile();

    modules.push(module);
    await module.init();

    return {
      module,
      group,
      keyPrefix,
      transport: module.get(RedisStreamsTransport),
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
    for (const m of modules) {
      await shutdown(m.get(RedisStreamsTransport));
    }

    await Promise.all(modules.map((m) => m.close()));
    await container.stop();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('subscribe', () => {
    it('names the stream after the key prefix and the event', async () => {
      const keyPrefix = `custom:${crypto.randomUUID()}:`;
      const { transport } = await createTransport({ keyPrefix });

      await transport.subscribe(FirstEvent, 'Handler', vi.fn());

      expect(await redis.exists(`${keyPrefix}FirstEvent`)).toBe(1);
    });

    it('joins a group that already exists', async () => {
      const { transport, group, stream } = await createTransport();
      const other = await createTransport({
        group,
        keyPrefix: stream(FirstEvent).replace(/FirstEvent$/, ''),
      });

      await transport.subscribe(FirstEvent, 'Handler', vi.fn());
      const subscribed = other.transport.subscribe(
        FirstEvent,
        'Handler',
        vi.fn(),
      );

      await expect(subscribed).resolves.toBeUndefined();
    });

    it('rejects a handler that is already subscribed to the event', async () => {
      const { transport } = await createTransport();

      await transport.subscribe(FirstEvent, 'Handler', vi.fn());
      const subscribed = transport.subscribe(FirstEvent, 'Handler', vi.fn());

      await expect(subscribed).rejects.toThrow(/already subscribed/);
    });
  });

  describe('publish', () => {
    it('trims the acknowledged entries above the max length', async () => {
      const { transport, group, stream } = await createTransport({
        trim: { maxLength: 10 },
      });
      const key = stream(FirstEvent);
      const count = 500;

      await redis.xGroupCreate(key, group, '0', { MKSTREAM: true });
      await transport.publish(
        Array.from({ length: count }, (_, i) => ({
          event: new FirstEvent(String(i)),
        })),
      );
      const entries = await entriesOf(key);
      await redis.xReadGroup(group, 'consumer', { key, id: '>' });
      await redis.xAck(
        key,
        group,
        entries.map(({ id }) => id),
      );
      await transport.publish(new FirstEvent('trigger'));

      expect(await redis.xLen(key)).toBeLessThan(count);
    });

    it('keeps the unacknowledged entries above the max length', async () => {
      const { transport, group, stream } = await createTransport({
        trim: { maxLength: 10 },
      });
      const key = stream(FirstEvent);
      const count = 500;

      await redis.xGroupCreate(key, group, '0', { MKSTREAM: true });
      await transport.publish(
        Array.from({ length: count }, (_, i) => ({
          event: new FirstEvent(String(i)),
        })),
      );

      expect(await redis.xLen(key)).toBe(count);
    });

    it('trims unacknowledged entries above the max length when not only acknowledged', async () => {
      const { transport, group, stream } = await createTransport({
        trim: { maxLength: 10, onlyAcked: false },
      });
      const key = stream(FirstEvent);
      const count = 500;

      await redis.xGroupCreate(key, group, '0', { MKSTREAM: true });
      await transport.publish(
        Array.from({ length: count }, (_, i) => ({
          event: new FirstEvent(String(i)),
        })),
      );

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
        expect(
          () =>
            new RedisStreamsTransport({
              url,
              group: 'group',
              ...config,
            }),
        ).toThrow(`Invalid Redis Streams option ${option}`);
      },
    );
  });

  describe('consumer', () => {
    it('reads with the configured name', async () => {
      const { transport, group, stream } = await createTransport({
        consumer: 'my-consumer',
      });

      await transport.subscribe(FirstEvent, 'Handler', vi.fn());
      transport.onBusBootstrap();
      await transport.publish(new FirstEvent('x'));

      await vi.waitFor(async () => {
        const consumers = await redis.xInfoConsumers(stream(FirstEvent), group);
        expect(consumers.map(({ name }) => name)).toEqual(['my-consumer']);
      });
    });

    it('defaults the name to the host name and the process id', async () => {
      const { transport, group, stream } = await createTransport();

      await transport.subscribe(FirstEvent, 'Handler', vi.fn());
      transport.onBusBootstrap();
      await transport.publish(new FirstEvent('x'));

      await vi.waitFor(async () => {
        const consumers = await redis.xInfoConsumers(stream(FirstEvent), group);
        expect(consumers.map(({ name }) => name)).toEqual([
          `${hostname()}-${process.pid}`,
        ]);
      });
    });
  });

  describe('telemetry', () => {
    it('describes itself as a Redis server with a stream per event', async () => {
      const { transport, group, keyPrefix } = await createTransport();
      const parsed = new URL(url);

      const descriptor = transport.getTelemetryDescriptor();

      expect(descriptor).toMatchObject({
        system: 'redis',
        consumerGroup: group,
        server: { address: parsed.hostname, port: Number(parsed.port) },
      });
      expect(descriptor.destination?.('FirstEvent')).toBe(
        `${keyPrefix}FirstEvent`,
      );
    });

    it('reports the entries in flight', async () => {
      const otel = createOtelFixture();

      await otel.telemetry.onBusInit();

      try {
        const { transport, stream } = await createTransport({}, otel.telemetry);
        let release!: () => void;
        const blocked = new Promise<void>((resolve) => (release = resolve));
        const handler = vi.fn().mockReturnValue(blocked);

        await transport.subscribe(FirstEvent, 'FirstHandler', handler);
        transport.onBusBootstrap();
        await transport.publish(new FirstEvent('a'));
        await vi.waitFor(() => expect(handler).toHaveBeenCalledOnce());

        const [inflight] = await otel.metric('bus.redis_streams.inflight');

        expect(inflight?.value).toBe(1);
        expect(inflight?.attributes).toEqual({
          'messaging.system': 'redis',
          'messaging.destination.name': stream(FirstEvent),
        });

        release();
        await vi.waitFor(async () => {
          const [after] = await otel.metric('bus.redis_streams.inflight');

          expect(after?.value).toBe(0);
        });

        await shutdown(transport);
      } finally {
        await otel.shutdown();
      }
    });
  });

  describe('shutdown', () => {
    it('waits for the entries in flight and removes the consumer', async () => {
      const { transport, module, group, stream } = await createTransport();
      const started = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const drain = Streams.prototype.drain;
      let finished = false;

      // Lets the entry finish once shutdown has started to wait for it
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
      await shutdown(transport);
      await module.close();

      expect(finished).toBe(true);
      expect(await redis.xInfoConsumers(stream(FirstEvent), group)).toEqual([]);
    });

    it('stops waiting after the shutdown timeout and keeps the entry pending', async () => {
      const warn = vi.spyOn(Logger.prototype, 'warn').mockReturnValue();
      vi.spyOn(Logger.prototype, 'error').mockReturnValue();
      const { transport, module, group, stream } = await createTransport({
        shutdownTimeout: 100,
        consumer: 'stuck-consumer',
      });
      const started = Promise.withResolvers<void>();
      const stuck = Promise.withResolvers<void>();

      await transport.subscribe(FirstEvent, 'Stuck', () => {
        started.resolve();

        return stuck.promise;
      });
      transport.onBusBootstrap();
      await transport.publish(new FirstEvent('x'));
      await started.promise;
      await shutdown(transport);
      await module.close();

      const pending = await redis.xPending(stream(FirstEvent), group);
      const consumers = await redis.xInfoConsumers(stream(FirstEvent), group);
      expect(pending.pending).toBe(1);
      expect(consumers.map(({ name }) => name)).toEqual(['stuck-consumer']);
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining('Shutdown timeout reached'),
      );

      stuck.resolve();
    });
  });
});
