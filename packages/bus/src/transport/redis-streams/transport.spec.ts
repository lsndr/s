import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { GenericContainer, Wait } from 'testcontainers';
import { setTimeout } from 'node:timers/promises';
import { Test, type TestingModule } from '@nestjs/testing';
import { createClient, type RedisClientType } from 'redis';
import { TransportConfig } from '../transport.js';
import {
  RedisStreamsTransport,
  type RedisStreamsTransportConfig,
} from './transport.js';

class FirstEvent {
  public constructor(public readonly value: string) {}
}

class SecondEvent {
  public constructor(public readonly value: string) {}
}

describe('RedisStreamsTransport', { timeout: 60_000 }, () => {
  let container: Awaited<ReturnType<GenericContainer['start']>>;
  let url: string;
  let redis: RedisClientType;
  const modules: TestingModule[] = [];

  const createTransport = async (
    config: Partial<RedisStreamsTransportConfig> = {},
    group = crypto.randomUUID(),
    retries = { max: 3, delay: 100 },
  ) => {
    const module = await Test.createTestingModule({
      providers: [
        RedisStreamsTransport,
        {
          provide: TransportConfig,
          useValue: {
            url,
            group,
            retries,
            blockTimeout: 500,
            reclaimInterval: 25,
            ...config,
          } satisfies RedisStreamsTransportConfig,
        },
      ],
    }).compile();

    await module.init();
    modules.push(module);

    return { module, group, transport: module.get(RedisStreamsTransport) };
  };

  beforeAll(async () => {
    container = await new GenericContainer('redis:8.2')
      .withExposedPorts(6379)
      .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
      .start();
    url = `redis://${container.getHost()}:${container.getMappedPort(6379)}`;
    redis = createClient({ url });
    await redis.connect();
  }, 60_000);

  afterAll(async () => {
    await redis.close();
    await Promise.all(modules.map((m) => m.close()));
    await container.stop();
  });

  it('retries only the handlers that failed and clears the done hash', async () => {
    const { transport, group } = await createTransport();
    const ok = vi.fn();
    const flaky = vi
      .fn()
      .mockRejectedValueOnce(new Error('boom'))
      .mockResolvedValue(undefined);

    await transport.subscribe([
      { event: FirstEvent, handler: 'Ok', callback: ok },
      { event: FirstEvent, handler: 'Flaky', callback: flaky },
    ]);
    await transport.publish(new FirstEvent('a'));

    await vi.waitFor(() => expect(flaky).toHaveBeenCalledTimes(2));
    await vi.waitFor(async () => {
      const pending = await redis.xPending('events:FirstEvent', group);
      expect(pending.pending).toBe(0);
    });

    expect(ok).toHaveBeenCalledTimes(1);
    expect(await redis.keys(`events:done:${group}:*`)).toEqual([]);
  });

  it('caps the entries in flight per stream below 2 * batchSize', async () => {
    const batchSize = 2;
    const { transport } = await createTransport({ batchSize });
    let running = 0;
    let peak = 0;
    let handled = 0;

    await transport.subscribe(FirstEvent, 'Slow', async () => {
      peak = Math.max(peak, ++running);
      await setTimeout(150);
      running--;
      handled++;
    });
    await transport.publish(
      Array.from({ length: 10 }, (_, i) => ({
        event: new FirstEvent(String(i)),
      })),
    );

    await vi.waitFor(() => expect(handled).toBe(10), { timeout: 10_000 });
    expect(peak).toBeLessThanOrEqual(2 * batchSize - 1);
  });

  it('keeps reading other streams while one is full', async () => {
    const { transport } = await createTransport({ batchSize: 1 });
    const release = Promise.withResolvers<void>();
    const second = vi.fn();

    await transport.subscribe([
      {
        event: FirstEvent,
        handler: 'Blocked',
        callback: () => release.promise,
      },
      { event: SecondEvent, handler: 'Free', callback: second },
    ]);
    await transport.publish(new FirstEvent('block'));
    await setTimeout(100);
    await transport.publish(new SecondEvent('go'));

    await vi.waitFor(() => expect(second).toHaveBeenCalledTimes(1));
    release.resolve();
  });

  it('consumes subscriptions registered after start', async () => {
    const { transport } = await createTransport();
    const first = vi.fn();
    const second = vi.fn();

    await transport.subscribe(FirstEvent, 'First', first);
    await transport.subscribe(SecondEvent, 'Second', second);
    await transport.publish(new SecondEvent('late'));

    await vi.waitFor(() => expect(second).toHaveBeenCalledTimes(1), {
      timeout: 5_000,
    });
    expect(first).not.toHaveBeenCalled();
  });

  it('waits for entries in flight on shutdown and removes the consumer', async () => {
    const { transport, module, group } = await createTransport();
    let finished = false;

    await transport.subscribe(FirstEvent, 'Slow', async () => {
      await setTimeout(300);
      finished = true;
    });
    await transport.publish(new FirstEvent('x'));
    await setTimeout(100);
    await module.close();

    expect(finished).toBe(true);
    expect(await redis.xInfoConsumers('events:FirstEvent', group)).toEqual([]);
  });

  it('gives up waiting after shutdownTimeout and keeps the entry pending', async () => {
    const { transport, module, group } = await createTransport({
      shutdownTimeout: 100,
    });

    await transport.subscribe(
      FirstEvent,
      'Stuck',
      () => new Promise<void>(() => undefined),
    );
    await transport.publish(new FirstEvent('x'));
    await setTimeout(100);
    await module.close();

    const pending = await redis.xPending('events:FirstEvent', group);
    expect(pending.pending).toBe(1);
  });
});
