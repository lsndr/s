import { NoopTelemetry } from '../telemetry/noop.telemetry.js';
import { type Telemetry } from '../telemetry/telemetry.js';
import { OtelNames } from '../telemetry/otel/otel.names.js';
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
      telemetry?: Telemetry,
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
          telemetry: Telemetry = new NoopTelemetry(),
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
          telemetry: Telemetry = new NoopTelemetry(),
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
      telemetry?: Telemetry,
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
      // arrange
      const config: CommonConfig = {
        group: crypto.randomUUID(),
        retries: { max: 0, delay: 100 },
      };
      const handler1 = vi.fn();
      const handler2 = vi.fn();
      const transport = await createTransport(config);
      await transport.subscribe(TestEvent, 'HandlerA', handler1);
      await transport.subscribe(TestEvent, 'HandlerB', handler2);
      await bootstrap(transport);

      const publisher = await createTransport(config);
      const event = new TestEvent({ value: 'multi-handler' });

      // act
      await publisher.publish(event);

      // assert
      await vi.waitFor(() => {
        expect(handler1).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({ event }),
        );
        expect(handler2).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({ event }),
        );
      });
    });

    describe('telemetry', () => {
      /**
       * Every queue also gets what later tests publish, so the handler only
       * fails the event of its own test.
       */
      async function setupSettlement(
        retries: CommonConfig['retries'],
        outcome: 'succeeds' | 'fails',
      ) {
        const telemetry = new NoopTelemetry();
        const counter = { add: vi.fn() };
        const histogram = { record: vi.fn() };
        const durations = { record: vi.fn() };

        // only the instruments of the settlements and the latency are looked at
        vi.spyOn(telemetry.getMeter(), 'counter').mockImplementation((name) =>
          name.join('.') === 'settled.messages' ? counter : { add: vi.fn() },
        );
        vi.spyOn(telemetry.getMeter(), 'histogram').mockImplementation(
          (name) => {
            switch (name.join('.')) {
              case 'delivery.latency':
                return histogram;
              case 'operation.duration':
                return durations;
              default:
                return { record: vi.fn() };
            }
          },
        );

        const group = crypto.randomUUID();
        const value = crypto.randomUUID();
        const transport = await createTransport(
          { group, retries },
          serializer,
          telemetry,
        );

        await transport.subscribe(TestEvent, 'TestHandler', ({ event }) =>
          outcome === 'fails' && event.value === value
            ? Promise.reject(new Error('boom'))
            : Promise.resolve(),
        );
        await bootstrap(transport);

        return {
          group,
          histogram,
          settleOperations: () =>
            durations.record.mock.calls
              .map(([, attributes]) =>
                new OtelNames('bus').toAttributes(attributes),
              )
              .filter(
                (attributes) =>
                  attributes['messaging.operation.type'] === 'settle',
              )
              .map((attributes) => attributes['messaging.operation.name']),
          publish: () => transport.publish(new TestEvent({ value })),
          outcomes: () =>
            counter.add.mock.calls.map(
              ([, attributes]) =>
                new OtelNames('bus').toAttributes(attributes)[
                  'bus.settlement.outcome'
                ],
            ),
          attributes: () =>
            new OtelNames('bus').toAttributes(counter.add.mock.calls[0]?.[1]),
        };
      }

      it('counts a handled message as acked', async () => {
        // arrange
        const { publish, outcomes, attributes, group } = await setupSettlement(
          { max: 0, delay: 100 },
          'succeeds',
        );

        // act
        await publish();

        // assert
        await vi.waitFor(() => expect(outcomes()).toEqual(['acked']));
        expect(attributes()).toMatchObject({
          'messaging.consumer.group.name': group,
        });
      });

      it('measures the ack of a handled message', async () => {
        // arrange
        const { publish, settleOperations } = await setupSettlement(
          { max: 0, delay: 100 },
          'succeeds',
        );

        // act
        await publish();

        // assert
        await vi.waitFor(() => expect(settleOperations()).toEqual(['ack']));
      });

      it('counts a failed message without retries left as dropped', async () => {
        // arrange
        const { publish, outcomes } = await setupSettlement(
          { max: 0, delay: 100 },
          'fails',
        );

        // act
        await publish();

        // assert
        await vi.waitFor(() => expect(outcomes()).toEqual(['dropped']));
      });

      it('counts a failed message with retries left as retried', async () => {
        // arrange
        const { publish, outcomes } = await setupSettlement(
          { max: 1, delay: 100 },
          'fails',
        );

        // act
        await publish();

        // assert
        await vi.waitFor(() =>
          expect(outcomes()).toEqual(['retried', 'dropped']),
        );
      });

      it('records the delivery latency once, not for the retries', async () => {
        // arrange
        const { publish, outcomes, histogram } = await setupSettlement(
          { max: 1, delay: 100 },
          'fails',
        );

        // act
        await publish();

        // assert
        await vi.waitFor(() => expect(outcomes()).toHaveLength(2));
        expect(histogram.record).toHaveBeenCalledExactlyOnceWith(
          expect.any(Number),
          expect.arrayContaining([
            {
              name: ['system'],
              value: expect.any(String),
            },
          ]),
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
          new NoopTelemetry(),
        );

        // act
        const healthy = transport.assertHealthy();

        // assert
        await expect(healthy).rejects.toThrow();
      });
    });
  });
});
