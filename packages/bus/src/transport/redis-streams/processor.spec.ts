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
import { Logger } from '@nestjs/common';
import { createClient, RESP_TYPES, type RedisClientType } from 'redis';
import { JsonSerializer } from '../../serialization/json.serializer.js';
import { Clients } from './clients.js';
import { Processor, type ProcessorOptions } from './processor.js';
import { type Callback, Handler } from './handler.js';
import { type Stream } from './streams.js';

class TestEvent {
  public constructor(public readonly value: string) {}
}

interface SetupOptions {
  handlers: Record<string, Callback>;
  /** Raw message of the entry. Defaults to a serialized `TestEvent`. */
  message?: Record<string, Buffer>;
  options?: Partial<ProcessorOptions>;
}

describe('Processor', { timeout: 60_000 }, () => {
  let container: Awaited<ReturnType<GenericContainer['start']>>;
  let clients: Clients<'command'>;
  let redis: RedisClientType;
  let log: ReturnType<typeof mockLogger>;
  const serializer = new JsonSerializer();

  function mockLogger() {
    return {
      log: vi.spyOn(Logger.prototype, 'log').mockImplementation(() => {}),
      warn: vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {}),
      error: vi.spyOn(Logger.prototype, 'error').mockImplementation(() => {}),
      verbose: vi
        .spyOn(Logger.prototype, 'verbose')
        .mockImplementation(() => {}),
    };
  }

  /** Creates a stream with one entry that is pending for a consumer. */
  async function setup({ handlers, message, options }: SetupOptions) {
    const name = `stream:${crypto.randomUUID()}`;
    const group = `group:${crypto.randomUUID()}`;
    const processorOptions: ProcessorOptions = {
      group,
      keyPrefix: 'test:',
      maxRetries: 3,
      retriesDelay: 100,
      reclaimInterval: 25,
      ...options,
    };

    await redis.xGroupCreate(name, group, '0', { MKSTREAM: true });
    await redis.xAdd(
      name,
      '*',
      message ?? {
        payload: Buffer.from(serializer.serialize(new TestEvent('hello')).data),
      },
    );

    const reply = await redis
      .withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer })
      .xReadGroup(group, 'consumer', { key: name, id: '>' });
    const raw = reply?.[0]?.messages[0];

    if (!raw) {
      throw new Error('Entry was not read');
    }

    const entry = { ...raw, id: String(raw.id) };

    const stream: Stream = {
      name,
      event: TestEvent,
      handlers: Object.entries(handlers).map(
        ([name, callback]) => new Handler(name, callback),
      ),
    };

    return {
      stream,
      entry,
      options: processorOptions,
      processor: new Processor(clients, serializer, processorOptions),
      doneKey: `test:done:${group}:TestEvent:${entry.id}`,
      pending: async () => (await redis.xPending(name, group)).pending,
    };
  }

  /** Makes `exec` of the n-th (1-based) `multi()` call reject, once. */
  async function failExecOfMulti(nth: number) {
    const client = await clients.get('command');
    const original = client.multi.bind(client);
    let calls = 0;

    vi.spyOn(client, 'multi').mockImplementation(() => {
      const multi = original();

      if (++calls === nth) {
        vi.spyOn(multi, 'exec').mockRejectedValueOnce(new Error('exec failed'));
      }

      return multi;
    });
  }

  beforeAll(async () => {
    await serializer.onBusInit();

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

  beforeEach(() => {
    log = mockLogger();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('when every handler succeeds', () => {
    it('acknowledges the entry and clears the done hash', async () => {
      const { processor, stream, entry, doneKey, pending } = await setup({
        handlers: { A: vi.fn(), B: vi.fn() },
      });

      await processor.process(stream, entry, 1);

      expect(await pending()).toBe(0);
      expect(await redis.exists(doneKey)).toBe(0);
      expect(log.log).toHaveBeenCalledWith(
        expect.stringContaining('handled successfully'),
      );
    });

    it('passes the parsed event to the handlers', async () => {
      const handler = vi.fn();
      const { processor, stream, entry } = await setup({
        handlers: { A: handler },
      });

      await processor.process(stream, entry, 1);

      expect(handler).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          name: 'TestEvent',
          event: expect.any(TestEvent),
          publishedAt: expect.any(Date),
        }),
      );
      expect(handler).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          event: expect.objectContaining({ value: 'hello' }),
        }),
      );
    });
  });

  describe('when a handler fails and retries are left', () => {
    it('leaves the entry pending and records the handlers that succeeded', async () => {
      const ok = vi.fn();
      const flaky = vi.fn().mockRejectedValue(new Error('boom'));
      const { processor, stream, entry, doneKey, pending } = await setup({
        handlers: { Ok: ok, Flaky: flaky },
      });

      await processor.process(stream, entry, 1);

      expect(await pending()).toBe(1);
      expect(await redis.hGetAll(doneKey)).toEqual({ Ok: '1' });
      expect(await redis.pTTL(doneKey)).toBeGreaterThan(0);
      expect(log.error).toHaveBeenCalledExactlyOnceWith(
        expect.stringContaining('(handler: Flaky, retryCount: 0): Error: boom'),
        expect.any(String),
      );
    });

    it('runs only the handlers that have not succeeded yet on the next attempt', async () => {
      const ok = vi.fn();
      const flaky = vi
        .fn()
        .mockRejectedValueOnce(new Error('boom'))
        .mockResolvedValue(undefined);
      const { processor, stream, entry, doneKey, pending } = await setup({
        handlers: { Ok: ok, Flaky: flaky },
      });

      await processor.process(stream, entry, 1);
      await processor.process(stream, entry, 2);

      expect(ok).toHaveBeenCalledExactlyOnceWith(expect.anything());
      expect(flaky).toHaveBeenCalledTimes(2);
      expect(await pending()).toBe(0);
      expect(await redis.exists(doneKey)).toBe(0);
    });

    it('skips the handlers that are already in the done hash', async () => {
      const done = vi.fn();
      const todo = vi.fn();
      const { processor, stream, entry, doneKey } = await setup({
        handlers: { Done: done, Todo: todo },
      });

      await redis.hSet(doneKey, 'Done', '1');
      await processor.process(stream, entry, 2);

      expect(done).not.toHaveBeenCalled();
      expect(todo).toHaveBeenCalledExactlyOnceWith(expect.anything());
    });
  });

  describe('when a handler fails and retries are exhausted', () => {
    it('acknowledges the entry, clears the done hash and logs the failure', async () => {
      const { processor, stream, entry, doneKey, pending } = await setup({
        handlers: {
          Ok: vi.fn(),
          Broken: vi.fn().mockRejectedValue(new Error('boom')),
        },
        options: { maxRetries: 2 },
      });

      await processor.process(stream, entry, 3);

      expect(await pending()).toBe(0);
      expect(await redis.exists(doneKey)).toBe(0);
      expect(log.error).toHaveBeenCalledWith(
        expect.stringContaining(
          '(handler: Broken) failed permanently after 2 retries: Error: boom',
        ),
        expect.any(String),
      );
    });

    it('keeps the entry pending on the last attempt that still has a retry', async () => {
      const { processor, stream, entry, pending } = await setup({
        handlers: { Broken: vi.fn().mockRejectedValue(new Error('boom')) },
        options: { maxRetries: 2 },
      });

      await processor.process(stream, entry, 2);

      expect(await pending()).toBe(1);
    });
  });

  describe('when something goes wrong', () => {
    it('still acknowledges the entry if recording a handler fails', async () => {
      const handler = vi.fn();
      const { processor, stream, entry, doneKey, pending } = await setup({
        handlers: { A: handler },
      });

      // 1st multi() records the handler, 2nd one acknowledges the entry
      await failExecOfMulti(1);
      await processor.process(stream, entry, 1);

      expect(handler).toHaveBeenCalledExactlyOnceWith(expect.anything());
      expect(await pending()).toBe(0);
      expect(await redis.exists(doneKey)).toBe(0);
      expect(log.warn).toHaveBeenCalledExactlyOnceWith(
        expect.stringContaining(`Failed to record handler A of ${entry.id}`),
        expect.any(String),
      );
    });
  });
});
