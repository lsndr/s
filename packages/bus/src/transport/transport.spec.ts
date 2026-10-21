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
import {
  RabbitmqTransport,
  RabbitmqTransportConfig,
} from './rabbitmq/transport.js';
import {
  RedisStreamsTransport,
  RedisStreamsTransportConfig,
} from './redis-streams/transport.js';
import { type Transport } from './transport.js';
import {
  hasOnBusBootstrap,
  hasOnBusInit,
  hasOnBusShutdown,
} from '../lifecycle.js';
import { setTimeout } from 'node:timers/promises';
import { IsDate, IsString } from 'class-validator';
import { type Serializer } from '../serialization/serializer.js';
import { JsonSerializer } from '../serialization/json.serializer.js';
const serializer = new JsonSerializer();

await serializer.onBusInit();

class TestEvent {
  @IsString()
  public value: string;

  @IsDate()
  public createdAt: Date;

  public constructor(props: { value: string }) {
    this.value = props.value;
    this.createdAt = new Date();
  }
}

class OtherEvent {
  public readonly kind = 'other';
}

/** Options that every transport takes. */
interface CommonConfig {
  group: string;
  retries: { max: number; delay: number };
}

interface TransportTest {
  name: string;
  setup: () => Promise<{
    createTransport: (
      config: CommonConfig,
      serializer?: Serializer,
    ) => Promise<Transport>;
    teardown: () => Promise<void>;
  }>;
}

/** Does what the bus does before the handlers are subscribed. */
async function init(transport: Transport) {
  if (hasOnBusInit(transport)) {
    await transport.onBusInit();
  }
}

/** Does what the bus does when it shuts down. */
async function shutdown(transport: Transport) {
  if (hasOnBusShutdown(transport)) {
    await transport.onBusShutdown();
  }
}

/** Does what the bus does once the handlers are subscribed. */
async function bootstrap(transport: Transport) {
  if (hasOnBusBootstrap(transport)) {
    await transport.onBusBootstrap();
  }
}

describe('Transport', { timeout: 60_000 }, () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  describe.each<TransportTest>([
    {
      name: 'RabbitmqTransport',
      setup: async () => {
        const container = await new GenericContainer('rabbitmq:3-management')
          .withExposedPorts(5672)
          .withWaitStrategy(
            Wait.forAll([
              Wait.forListeningPorts(),
              Wait.forLogMessage(/Server startup complete/),
            ]),
          )
          .start();
        const url = `amqp://${container.getHost()}:${container.getMappedPort(5672)}`;
        const transports: Transport[] = [];

        const createTransport = async (
          busConfig: CommonConfig,
          custom: Serializer = serializer,
        ) => {
          const transport = new RabbitmqTransport(
            {
              url,
              ...busConfig,
            } satisfies RabbitmqTransportConfig,
            custom,
          );

          await init(transport);

          transports.push(transport);

          return transport;
        };

        const teardown = async () => {
          await Promise.all(transports.map(shutdown));
          await container.stop();
        };

        return { createTransport, teardown };
      },
    },
    {
      name: 'RedisStreamsTransport',
      setup: async () => {
        const container = await new GenericContainer('redis:8.2')
          .withExposedPorts(6379)
          .withWaitStrategy(
            Wait.forAll([
              Wait.forListeningPorts(),
              Wait.forLogMessage(/Ready to accept connections/),
            ]),
          )
          .start();
        const url = `redis://${container.getHost()}:${container.getMappedPort(6379)}`;
        const transports: Transport[] = [];

        const createTransport = async (
          busConfig: CommonConfig,
          custom: Serializer = serializer,
        ) => {
          const transport = new RedisStreamsTransport(
            {
              url,
              ...busConfig,
              reclaimInterval: 25,
              // One entry at a time, so that a busy instance leaves the
              // next entries to the others
              concurrency: 1,
            } satisfies RedisStreamsTransportConfig,
            custom,
          );

          await init(transport);

          transports.push(transport);

          return transport;
        };

        const teardown = async () => {
          await Promise.all(transports.map(shutdown));
          await container.stop();
        };

        return { createTransport, teardown };
      },
    },
  ])('$name', ({ setup }) => {
    let teardown: () => Promise<void>;
    let createTransport: (
      config: CommonConfig,
      serializer?: Serializer,
    ) => Promise<Transport>;

    beforeAll(async () => {
      const setupResult = await setup();

      teardown = setupResult.teardown;
      createTransport = setupResult.createTransport;
    }, 60_000);

    afterAll(async () => {
      await teardown();
    });

    it('republishes persisted data under its original id, name and time', async () => {
      const config: CommonConfig = {
        group: crypto.randomUUID(),
        retries: { max: 0, delay: 100 },
      };
      const transport = await createTransport(config);
      const handler = vi.fn();
      const publishedAt = new Date('2026-01-02T03:04:05.000Z');

      await transport.subscribe(TestEvent, 'TestHandler', handler);

      await bootstrap(transport);
      await transport.publish(
        { value: 'hello' },
        { id: 'persisted-id', name: 'TestEvent', meta: { a: 1 }, publishedAt },
      );

      await vi.waitFor(() => {
        expect(handler).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: 'persisted-id',
            event: expect.objectContaining({ value: 'hello' }),
            meta: { a: 1 },
            publishedAt,
          }),
        );
      });
    });

    it('subscribes and publishes in batches', async () => {
      const transport = await createTransport({
        group: crypto.randomUUID(),
        retries: { max: 0, delay: 100 },
      });
      const testHandler = vi.fn();
      const otherHandler = vi.fn();

      await transport.subscribe([
        { event: TestEvent, handler: 'TestHandler', callback: testHandler },
        { event: OtherEvent, handler: 'OtherHandler', callback: otherHandler },
      ]);

      await bootstrap(transport);
      await transport.publish([
        { event: new TestEvent({ value: 'first' }), options: { id: 'id-1' } },
        { event: new OtherEvent(), options: { id: 'id-2' } },
        { event: new TestEvent({ value: 'second' }), options: { id: 'id-3' } },
      ]);

      await vi.waitFor(() => {
        expect(testHandler.mock.calls.map(([payload]) => payload.id)).toEqual(
          expect.arrayContaining(['id-1', 'id-3']),
        );
        expect(testHandler).toHaveBeenCalledTimes(2);
        expect(otherHandler).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({ id: 'id-2' }),
        );
      });
    });

    it('delivers message to multiple groups', async () => {
      const subscribe = async () => {
        const handler = vi.fn();
        const config: CommonConfig = {
          group: crypto.randomUUID(),
          retries: { max: 0, delay: 100 },
        };
        const transport = await createTransport(config);
        await transport.subscribe(TestEvent, 'TestHandler', handler);
        await bootstrap(transport);

        return handler;
      };
      const subscriber1 = await subscribe();
      const subscriber2 = await subscribe();
      const subscriber3 = await subscribe();

      const publisher = await createTransport({
        group: crypto.randomUUID(),
        retries: { max: 0, delay: 100 },
      });
      const event = new TestEvent({ value: 'hello' });

      await publisher.publish(event);

      await vi.waitFor(() => {
        expect(subscriber1).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: expect.any(String),
            event,
            meta: {},
            publishedAt: expect.any(Date),
          }),
        );
        expect(subscriber2).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: expect.any(String),
            event,
            meta: {},
            publishedAt: expect.any(Date),
          }),
        );
        expect(subscriber3).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: expect.any(String),
            event,
            meta: {},
            publishedAt: expect.any(Date),
          }),
        );
      });
    });

    it('distributes messages between multiple instances of the same consumers in the same group', async () => {
      const group = crypto.randomUUID();
      const config: CommonConfig = {
        group,
        retries: { max: 0, delay: 100 },
      };
      const subscribe = async () => {
        // Stays busy, so that the other entries go to the other instances
        const handler = vi.fn((_payload: unknown) => setTimeout(1_000));
        const transport = await createTransport(config);
        await transport.subscribe(TestEvent, 'TestHandler', handler);
        await bootstrap(transport);

        return handler;
      };
      const publisher = await createTransport(config);
      const subscriber1 = await subscribe();
      const subscriber2 = await subscribe();
      const subscriber3 = await subscribe();

      const event1 = new TestEvent({ value: '1' });
      const event2 = new TestEvent({ value: '2' });
      const event3 = new TestEvent({ value: '3' });

      await publisher.publish(event1);
      await publisher.publish(event2);
      await publisher.publish(event3);

      await vi.waitFor(() => {
        expect(subscriber1).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: expect.any(String),
            event: event1,
            meta: {},
            publishedAt: expect.any(Date),
          }),
        );
        expect(subscriber2).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: expect.any(String),
            event: event2,
            meta: {},
            publishedAt: expect.any(Date),
          }),
        );
        expect(subscriber3).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: expect.any(String),
            event: event3,
            meta: {},
            publishedAt: expect.any(Date),
          }),
        );
      });
    });

    it('delivers message to multiple handlers in the same group', async () => {
      const config: CommonConfig = {
        group: crypto.randomUUID(),
        retries: { max: 0, delay: 100 },
      };
      const handler1 = vi.fn();
      const handler2 = vi.fn();
      const transport = await createTransport(config);
      await transport.subscribe([
        { event: TestEvent, handler: 'HandlerA', callback: handler1 },
        { event: TestEvent, handler: 'HandlerB', callback: handler2 },
      ]);
      await bootstrap(transport);

      const publisher = await createTransport(config);
      const event = new TestEvent({ value: 'multi-handler' });
      await publisher.publish(event);

      await vi.waitFor(() => {
        expect(handler1).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({ event }),
        );
        expect(handler2).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({ event }),
        );
      });
    });

    it.each([1, 5, 10])(
      'retries a failing callback up to %s times',
      async (max) => {
        const delay = 100;
        const config: CommonConfig = {
          group: crypto.randomUUID(),
          retries: { max, delay },
        };
        const transport = await createTransport(config);
        const handler = vi.fn().mockImplementation(() => {
          throw new Error();
        });
        await transport.subscribe(TestEvent, 'TestHandler', handler);
        await bootstrap(transport);

        await transport.publish(new TestEvent({ value: 'retry-me' }));

        await setTimeout(delay * (max * 2));
        expect(handler).toHaveBeenCalledTimes(max + 1); // 1 initial attempt + max retries
      },
    );

    describe('assertHealthy', () => {
      it('resolves when transport is connected', async () => {
        const transport = await createTransport({
          group: crypto.randomUUID(),
          retries: { max: 0, delay: 100 },
        });

        await expect(transport.assertHealthy()).resolves.toBeUndefined();
      });

      it('throws when transport is not initialized', async () => {
        // intentionally skip onBusInit() so channel is never set
        const transport = new RabbitmqTransport(
          {
            url: 'amqp://localhost',
            group: crypto.randomUUID(),
            retries: { max: 0, delay: 100 },
          } satisfies RabbitmqTransportConfig,
          serializer,
        );

        await expect(transport.assertHealthy()).rejects.toThrow();
      });
    });
  });
});
