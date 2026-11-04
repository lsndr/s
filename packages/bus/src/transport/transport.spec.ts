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
import { SpanKind, SpanStatusCode } from '@opentelemetry/api';
import { createOtelFixture } from '../telemetry/otel/otel.fixture.js';
import { instrumentTransport } from '../telemetry/instrument.js';
import { type BusTelemetry } from '../telemetry/telemetry.js';
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

interface CommonConfig {
  group: string;
  retries: { max: number; delay: number };
}

interface TransportTest {
  name: string;
  /** The `messaging.system` the transport reports. */
  system: string;
  setup: () => Promise<{
    createTransport: (
      config: CommonConfig,
      serializer?: Serializer,
      telemetry?: BusTelemetry,
    ) => Promise<Transport>;
    teardown: () => Promise<void>;
  }>;
}

async function init(transport: Transport) {
  if (hasOnBusInit(transport)) {
    await transport.onBusInit();
  }
}

async function shutdown(transport: Transport) {
  if (hasOnBusShutdown(transport)) {
    await transport.onBusShutdown();
  }
}

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
      system: 'rabbitmq',
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
          telemetry?: BusTelemetry,
        ) => {
          const transport = new RabbitmqTransport(
            {
              url,
              ...busConfig,
            } satisfies RabbitmqTransportConfig,
            custom,
            telemetry,
          );

          await init(transport);

          if (telemetry) {
            instrumentTransport(transport, telemetry);
          }

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
      system: 'redis',
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
          telemetry?: BusTelemetry,
        ) => {
          const transport = new RedisStreamsTransport(
            {
              url,
              ...busConfig,
              reclaimInterval: 25,
              // one entry at a time, so that a busy instance leaves the
              // next entries to the others
              concurrency: 1,
            } satisfies RedisStreamsTransportConfig,
            custom,
            telemetry,
          );

          await init(transport);

          if (telemetry) {
            instrumentTransport(transport, telemetry);
          }

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
  ])('$name', ({ setup, system }) => {
    let teardown: () => Promise<void>;
    let createTransport: (
      config: CommonConfig,
      serializer?: Serializer,
      telemetry?: BusTelemetry,
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
      // arrange
      const config: CommonConfig = {
        group: crypto.randomUUID(),
        retries: { max: 0, delay: 100 },
      };
      const transport = await createTransport(config);
      const handler = vi.fn();
      const publishedAt = new Date('2026-01-02T03:04:05.000Z');

      await transport.subscribe(TestEvent, 'TestHandler', handler);

      await bootstrap(transport);

      // act
      await transport.publish(
        { value: 'hello' },
        { id: 'persisted-id', name: 'TestEvent', meta: { a: 1 }, publishedAt },
      );

      // assert
      await vi.waitFor(() => {
        expect(handler).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: 'persisted-id',
            event: expect.objectContaining({ value: 'hello' }),
            meta: { a: 1 },
            publishedAt,
          }),
          expect.objectContaining({ attempt: 1 }),
        );
      });
    });

    it('subscribes and publishes in batches', async () => {
      // arrange
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

      // act
      await transport.publish([
        { event: new TestEvent({ value: 'first' }), options: { id: 'id-1' } },
        { event: new OtherEvent(), options: { id: 'id-2' } },
        { event: new TestEvent({ value: 'second' }), options: { id: 'id-3' } },
      ]);

      // assert
      await vi.waitFor(() => {
        expect(testHandler.mock.calls.map(([payload]) => payload.id)).toEqual(
          expect.arrayContaining(['id-1', 'id-3']),
        );
        expect(testHandler).toHaveBeenCalledTimes(2);
        expect(otherHandler).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({ id: 'id-2' }),
          expect.objectContaining({ attempt: 1 }),
        );
      });
    });

    it('delivers message to multiple groups', async () => {
      // arrange
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

      // act
      await publisher.publish(event);

      // assert
      await vi.waitFor(() => {
        expect(subscriber1).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: expect.any(String),
            event,
            meta: {},
            publishedAt: expect.any(Date),
          }),
          expect.objectContaining({ attempt: 1 }),
        );
        expect(subscriber2).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: expect.any(String),
            event,
            meta: {},
            publishedAt: expect.any(Date),
          }),
          expect.objectContaining({ attempt: 1 }),
        );
        expect(subscriber3).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: expect.any(String),
            event,
            meta: {},
            publishedAt: expect.any(Date),
          }),
          expect.objectContaining({ attempt: 1 }),
        );
      });
    });

    it('distributes messages between multiple instances of the same consumers in the same group', async () => {
      // arrange
      const group = crypto.randomUUID();
      const config: CommonConfig = {
        group,
        retries: { max: 0, delay: 100 },
      };
      const subscribe = async () => {
        // stays busy, so that the other entries go to the other instances
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

      // act
      await publisher.publish(event1);
      await publisher.publish(event2);
      await publisher.publish(event3);

      // assert
      await vi.waitFor(() => {
        expect(subscriber1).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: expect.any(String),
            event: event1,
            meta: {},
            publishedAt: expect.any(Date),
          }),
          expect.objectContaining({ attempt: 1 }),
        );
        expect(subscriber2).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: expect.any(String),
            event: event2,
            meta: {},
            publishedAt: expect.any(Date),
          }),
          expect.objectContaining({ attempt: 1 }),
        );
        expect(subscriber3).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            id: expect.any(String),
            event: event3,
            meta: {},
            publishedAt: expect.any(Date),
          }),
          expect.objectContaining({ attempt: 1 }),
        );
      });
    });

    it('delivers message to multiple handlers in the same group', async () => {
      // arrange
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

      // act
      await publisher.publish(event);

      // assert
      await vi.waitFor(() => {
        expect(handler1).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({ event }),
          expect.objectContaining({ attempt: 1 }),
        );
        expect(handler2).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({ event }),
          expect.objectContaining({ attempt: 1 }),
        );
      });
    });

    it.each([1, 5, 10])(
      'retries a failing callback up to %s times',
      async (max) => {
        // arrange
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

        // act
        await transport.publish(new TestEvent({ value: 'retry-me' }));
        await setTimeout(delay * (max * 2));

        // assert
        expect(handler).toHaveBeenCalledTimes(max + 1); // 1 initial attempt + max retries
      },
    );

    describe('telemetry', () => {
      it('reports a message that fails once and is retried', async () => {
        const otel = createOtelFixture();

        await otel.telemetry.onBusInit();

        try {
          const group = crypto.randomUUID();
          const transport = await createTransport(
            { group, retries: { max: 1, delay: 100 } },
            undefined,
            otel.telemetry,
          );
          const handler = vi
            .fn()
            .mockRejectedValueOnce(new Error('first attempt fails'))
            .mockResolvedValue(undefined);

          await transport.subscribe(TestEvent, 'TestHandler', handler);
          await bootstrap(transport);
          await transport.publish(new TestEvent({ value: 'trace-me' }));

          await vi.waitFor(() => {
            expect(handler).toHaveBeenCalledTimes(2);
            expect(
              otel.spans().filter((span) => span.name.startsWith('ack')),
            ).toHaveLength(1);
          });

          const send = otel.spans().find((s) => s.name.startsWith('send'))!;
          const processes = otel
            .spans()
            .filter((s) => s.name.startsWith('process'))
            .sort(
              (a, b) =>
                Number(a.attributes['bus.delivery.attempt']) -
                Number(b.attributes['bus.delivery.attempt']),
            );

          expect(send.kind).toBe(SpanKind.PRODUCER);
          expect(send.attributes).toMatchObject({
            'messaging.system': system,
            'messaging.operation.type': 'send',
            'messaging.consumer.group.name': group,
            'server.address': expect.any(String),
            'server.port': expect.any(Number),
          });
          expect(processes).toHaveLength(2);
          expect(processes.map((s) => s.kind)).toEqual([
            SpanKind.CONSUMER,
            SpanKind.CONSUMER,
          ]);
          expect(
            processes.map((s) => s.attributes['bus.delivery.attempt']),
          ).toEqual([1, 2]);
          expect(processes[0]!.status.code).toBe(SpanStatusCode.ERROR);
          expect(processes[1]!.status.code).not.toBe(SpanStatusCode.ERROR);

          for (const process of processes) {
            expect(process.attributes).toMatchObject({
              'messaging.system': system,
              'messaging.destination.subscription.name': 'TestHandler',
            });
            expect(process.links.map((l) => l.context.spanId)).toEqual([
              send.spanContext().spanId,
            ]);
          }

          const [retries] = await otel.metric('bus.retries');

          expect(retries?.value).toBe(1);
          expect(retries?.attributes).toMatchObject({
            'messaging.system': system,
            'messaging.destination.subscription.name': 'TestHandler',
          });
          expect(await otel.metric('bus.dropped')).toEqual([]);
        } finally {
          await otel.shutdown();
        }
      });

      it('counts the message that is dropped after the last retry', async () => {
        const otel = createOtelFixture();

        await otel.telemetry.onBusInit();

        try {
          const transport = await createTransport(
            { group: crypto.randomUUID(), retries: { max: 0, delay: 100 } },
            undefined,
            otel.telemetry,
          );
          const handler = vi.fn().mockRejectedValue(new Error('always'));

          await transport.subscribe(TestEvent, 'TestHandler', handler);
          await bootstrap(transport);
          await transport.publish(new TestEvent({ value: 'drop-me' }));

          await vi.waitFor(async () => {
            expect((await otel.metric('bus.dropped'))[0]?.value).toBe(1);
          });
          expect(await otel.metric('bus.retries')).toEqual([]);
        } finally {
          await otel.shutdown();
        }
      });
    });

    describe('assertHealthy', () => {
      it('resolves when transport is connected', async () => {
        // arrange
        const transport = await createTransport({
          group: crypto.randomUUID(),
          retries: { max: 0, delay: 100 },
        });

        // act
        const healthy = transport.assertHealthy();

        // assert
        await expect(healthy).resolves.toBeUndefined();
      });

      it('throws when transport is not initialized', async () => {
        // arrange
        // intentionally skip onBusInit() so channel is never set
        const transport = new RabbitmqTransport(
          {
            url: 'amqp://localhost',
            group: crypto.randomUUID(),
            retries: { max: 0, delay: 100 },
          } satisfies RabbitmqTransportConfig,
          serializer,
        );

        // act
        const healthy = transport.assertHealthy();

        // assert
        await expect(healthy).rejects.toThrow();
      });
    });
  });
});
