import { Inject, Injectable, type ModuleMetadata } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import { setTimeout } from 'node:timers/promises';
import { afterEach, describe, expect, it, type Mock, vi } from 'vitest';
import { BusModule } from '../../bus.module.js';
import { EventHandler } from '../../decorators/event-handler.decorator.js';
import { Dispatcher, type PublishOptions } from '../dispatcher.js';
import { type EventCallback } from '../subscriber.js';
import { type EventConstructor } from '../../types.js';
import { Health } from '../health.js';
import { Transport } from '../transport.js';
import { type BusProvider } from '../../provider.js';
import { Serializer } from '../../serialization/serializer.js';
import { type JsonSerializer } from '../../serialization/json.serializer.js';
import {
  Telemetry,
  type TelemetryCounter,
  type TelemetryHistogram,
} from '../../telemetry/telemetry.js';
import { Outbox, type OutboxConfig, type OutboxRecord } from './outbox.js';

class TestEvent {
  public value: string;

  public constructor(props: { value: string }) {
    this.value = props.value;
  }
}

interface InnerConfig {
  url: string;
}

class InnerTransport implements Transport {
  public static publish =
    vi.fn<(event: object, options?: PublishOptions) => Promise<void>>();
  public static subscribe = vi
    .fn<
      (
        event: EventConstructor<any>,
        handler: string,
        callback: EventCallback<any>,
      ) => Promise<void>
    >()
    .mockResolvedValue(undefined);
  public static assertHealthy = vi.fn().mockResolvedValue(undefined);
  public static onBusBootstrap = vi.fn();
  public static onBusShutdown = vi.fn();

  public assertHealthy = InnerTransport.assertHealthy;

  public constructor(private readonly config: InnerConfig) {}

  public static withConfig(config: InnerConfig): BusProvider<InnerTransport> {
    return { useFactory: () => new InnerTransport(config) };
  }

  public onBusBootstrap(): void {
    InnerTransport.onBusBootstrap();
  }

  public onBusShutdown(): void {
    InnerTransport.onBusShutdown();
  }

  public getConfig(): InnerConfig {
    return this.config;
  }

  public publish(event: object, options?: PublishOptions): Promise<void> {
    return InnerTransport.publish(event, options);
  }

  public subscribe(
    event: EventConstructor<any>,
    handler: string,
    callback: EventCallback<any>,
  ): Promise<void> {
    return InnerTransport.subscribe(event, handler, callback);
  }
}

interface MemoryOutboxConfig extends OutboxConfig {
  batchSize: number;
}

const Dependency = Symbol('Dependency');

@Injectable()
class MemoryOutbox extends Outbox<MemoryOutboxConfig> {
  public static pending: OutboxRecord[] = [];
  public static relay = vi.fn();
  public static save = vi.fn();

  public constructor(@Inject(Dependency) public readonly dependency: string) {
    super();
  }

  public getInnerTransport() {
    return this.transport;
  }

  public getConfig(): MemoryOutboxConfig {
    return this.config;
  }

  protected save(record: OutboxRecord): Promise<void> {
    MemoryOutbox.save(record);
    MemoryOutbox.pending.push(record);

    return Promise.resolve();
  }

  protected async relay(): Promise<number> {
    MemoryOutbox.relay();

    const batch = MemoryOutbox.pending.slice(0, this.config.batchSize);

    if (batch.length === 0) {
      return 0;
    }

    await this.dispatch(batch);
    MemoryOutbox.pending = MemoryOutbox.pending.slice(batch.length);

    return batch.length;
  }
}

@EventHandler(TestEvent)
class TestHandler {
  public handle = vi.fn();
}

const modules: TestingModule[] = [];
const closed = new WeakSet<TestingModule>();

// closing twice would run the shutdown hooks twice
async function close(module: TestingModule) {
  if (!closed.has(module)) {
    closed.add(module);
    await module.close();
  }
}

async function compile(metadata: ModuleMetadata) {
  const module = await Test.createTestingModule(metadata).compile();

  modules.push(module);

  return module;
}

async function createModule(
  batchSize = 10,
  transport = InnerTransport.withConfig({
    url: 'inner://',
  }),
) {
  const module = await compile({
    imports: [
      BusModule.forRoot({
        transport: MemoryOutbox.withConfig({
          transport,
          interval: 10,
          batchSize,
        }),
        providers: [TestHandler, { provide: Dependency, useValue: 'dep' }],
      }),
    ],
  });

  // the bus initializes it on bootstrap, which tests publish before
  await module.get<JsonSerializer>(Serializer).onBusInit();

  return module;
}

describe('Outbox', () => {
  afterEach(async () => {
    for (const module of modules.splice(0)) {
      await close(module);
    }

    vi.clearAllMocks();
    vi.restoreAllMocks();
    InnerTransport.publish.mockReset();
    MemoryOutbox.pending = [];
  });

  it('stores published events', async () => {
    // arrange
    const module = await createModule();
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    // act
    await dispatcher.publish(new TestEvent({ value: 'a' }), {
      id: 'id-1',
      meta: { key: 'value' },
    });

    // assert
    expect(MemoryOutbox.pending).toEqual([
      {
        id: 'id-1',
        name: 'TestEvent',
        publishedAt: expect.any(Date),
        headers: {},
        data: JSON.stringify({
          id: 'id-1',
          name: 'TestEvent',
          event: { value: 'a' },
          meta: { key: 'value' },
          publishedAt: MemoryOutbox.pending[0]!.publishedAt.toISOString(),
        }),
      },
    ]);
  });

  it('does not publish the stored events right away', async () => {
    // arrange
    const module = await createModule();
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    // act
    await dispatcher.publish(new TestEvent({ value: 'a' }), { id: 'id-1' });

    // assert
    expect(InnerTransport.publish).not.toHaveBeenCalled();
  });

  it('bootstraps the inner transport with the bus', async () => {
    // arrange
    const module = await createModule();

    // act
    await module.init();

    // assert
    expect(InnerTransport.onBusBootstrap).toHaveBeenCalledOnce();
    expect(InnerTransport.onBusShutdown).not.toHaveBeenCalled();
  });

  it('shuts the inner transport down with the bus', async () => {
    // arrange
    const module = await createModule();

    await module.init();

    // act
    await close(module);

    // assert
    expect(InnerTransport.onBusShutdown).toHaveBeenCalledOnce();
  });

  it('relays every event of a page of stored events', async () => {
    // arrange
    const module = await createModule(2);
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    await dispatcher.publish(new TestEvent({ value: 'a' }), { id: 'id-1' });
    await dispatcher.publish(new TestEvent({ value: 'b' }), { id: 'id-2' });

    // act
    await module.init();
    await vi.waitFor(() => expect(MemoryOutbox.pending).toHaveLength(0));

    // assert
    expect(
      InnerTransport.publish.mock.calls.map(([, options]) => options?.id),
    ).toEqual(expect.arrayContaining(['id-1', 'id-2']));
  });

  it('stores the headers with the record and relays them', async () => {
    // arrange
    const module = await createModule();
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    await dispatcher.publish(new TestEvent({ value: 'a' }), {
      id: 'id-1',
      headers: { traceparent: 'tp' },
    });

    // act
    await module.init();
    await vi.waitFor(() => expect(MemoryOutbox.pending).toHaveLength(0));

    // assert
    expect(InnerTransport.publish.mock.calls[0]![1]?.headers).toEqual({
      traceparent: 'tp',
    });
  });

  it('wraps storing the records in a telemetry create', async () => {
    // arrange
    const module = await createModule();
    const create = vi.spyOn(
      module.get<Telemetry>(Telemetry).getTracer(),
      'create',
    );
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    // act
    await dispatcher.publish(new TestEvent({ value: 'a' }), { id: 'id-1' });

    // assert
    expect(create).toHaveBeenCalledWith(
      { system: 'outbox' },
      {
        id: 'id-1',
        name: 'TestEvent',
        headers: {},
        bodySize: expect.any(Number),
      },
      expect.any(Function),
    );
  });

  describe('relay metrics', () => {
    const outbox = [{ name: ['system'], value: 'outbox' }];

    /** A module whose meter hands out the given instruments. */
    async function createMeteredModule() {
      const module = await createModule();
      const meter = module.get<Telemetry>(Telemetry).getMeter();
      const counter = { add: vi.fn<TelemetryCounter['add']>() };
      const histograms = new Map<
        string,
        { record: Mock<TelemetryHistogram['record']> }
      >();

      // only the counter of the relayed records is looked at
      vi.spyOn(meter, 'counter').mockImplementation((name) =>
        name.join('.') === 'outbox.relayed.messages'
          ? counter
          : { add: vi.fn() },
      );
      vi.spyOn(meter, 'histogram').mockImplementation((name) => {
        const key = name.join('.');
        const histogram = histograms.get(key) ?? {
          record: vi.fn<TelemetryHistogram['record']>(),
        };

        histograms.set(key, histogram);

        return histogram;
      });

      return {
        module,
        counter,
        dispatcher: module.get<Dispatcher>(Dispatcher),
        recorded: (name: string) => histograms.get(name)?.record.mock.calls,
      };
    }

    it('counts the relayed records', async () => {
      // arrange
      const { module, dispatcher, counter } = await createMeteredModule();

      InnerTransport.publish.mockResolvedValue(undefined);
      await dispatcher.publish(new TestEvent({ value: 'a' }));
      await dispatcher.publish(new TestEvent({ value: 'b' }));

      // act
      await module.init();
      await vi.waitFor(() => expect(MemoryOutbox.pending).toHaveLength(0));

      // assert
      expect(counter.add).toHaveBeenCalledExactlyOnceWith(2, outbox);
    });

    it('counts records that failed to relay with the type of the error', async () => {
      // arrange
      const { module, dispatcher, counter } = await createMeteredModule();

      InnerTransport.publish.mockRejectedValue(new TypeError('boom'));
      await dispatcher.publish(new TestEvent({ value: 'a' }));

      // act
      await module.init();

      // assert
      await vi.waitFor(() =>
        expect(counter.add).toHaveBeenCalledWith(1, [
          ...outbox,
          { name: ['error', 'type'], value: 'TypeError' },
        ]),
      );
    });

    it('records how long each relayed record waited', async () => {
      // arrange
      const { module, dispatcher, recorded } = await createMeteredModule();

      InnerTransport.publish.mockResolvedValue(undefined);
      await dispatcher.publish(new TestEvent({ value: 'a' }), {
        publishedAt: new Date(Date.now() - 5_000),
      });

      // act
      await module.init();
      await vi.waitFor(() => expect(MemoryOutbox.pending).toHaveLength(0));

      // assert
      const [[latency, attributes]] = recorded('outbox.relay.latency') as [
        [number, object],
      ];

      expect(latency).toBeGreaterThanOrEqual(5);
      expect(latency).toBeLessThan(10);
      expect(attributes).toEqual(outbox);
    });

    it('records no latency for records that failed to relay', async () => {
      // arrange
      const { module, dispatcher, counter, recorded } =
        await createMeteredModule();

      InnerTransport.publish.mockRejectedValue(new Error('boom'));
      await dispatcher.publish(new TestEvent({ value: 'a' }));

      // act
      await module.init();
      await vi.waitFor(() => expect(counter.add).toHaveBeenCalled());

      // assert
      expect(recorded('outbox.relay.latency')).toEqual([]);
    });

    it('records the duration of a relay round', async () => {
      // arrange
      const { module, recorded } = await createMeteredModule();

      // act
      await module.init();

      // assert
      await vi.waitFor(() =>
        expect(recorded('outbox.relay.duration')?.[0]).toEqual([
          expect.any(Number),
          outbox,
        ]),
      );
    });
  });

  it('relays stored events to the transport as they were stored', async () => {
    // arrange
    const module = await createModule(1);
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    await dispatcher.publish(new TestEvent({ value: 'a' }), { id: 'id-1' });
    await dispatcher.publish(new TestEvent({ value: 'b' }), { id: 'id-2' });
    const [first, second] = MemoryOutbox.pending;

    // act
    await module.init();
    await vi.waitFor(() => expect(MemoryOutbox.pending).toHaveLength(0));

    // assert
    expect(InnerTransport.publish.mock.calls).toEqual([
      [
        { value: 'a' },
        {
          id: 'id-1',
          name: 'TestEvent',
          meta: {},
          publishedAt: first!.publishedAt,
          headers: {},
        },
      ],
      [
        { value: 'b' },
        {
          id: 'id-2',
          name: 'TestEvent',
          meta: {},
          publishedAt: second!.publishedAt,
          headers: {},
        },
      ],
    ]);
  });

  it('keeps records pending and retries when the transport fails', async () => {
    // arrange
    InnerTransport.publish.mockRejectedValueOnce(new Error('broker down'));

    const module = await createModule();
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    await dispatcher.publish(new TestEvent({ value: 'a' }), { id: 'id-1' });

    // act
    await module.init();
    await vi.waitFor(() => expect(MemoryOutbox.pending).toHaveLength(0));

    // assert
    expect(InnerTransport.publish).toHaveBeenCalledTimes(2);
  });

  it('subscribes handlers through the transport', async () => {
    // arrange
    const module = await createModule();

    // act
    await module.init();

    // assert
    expect(InnerTransport.subscribe).toHaveBeenCalledExactlyOnceWith(
      TestEvent,
      TestHandler.name,
      expect.any(Function),
    );
  });

  it('checks health through the transport', async () => {
    // arrange
    const module = await createModule();

    await module.init();

    // act
    await module.get<Health>(Health).assertHealthy();

    // assert
    expect(InnerTransport.assertHealthy).toHaveBeenCalledOnce();
  });

  it('gives the outbox its config and its dependencies', async () => {
    // arrange
    const module = await createModule(7);

    // act
    const outbox = module.get<MemoryOutbox>(Transport);

    // assert
    expect(outbox.getConfig()).toMatchObject({ batchSize: 7, interval: 10 });
    expect(outbox.dependency).toBe('dep');
    expect(outbox.getInnerTransport()).toBeInstanceOf(InnerTransport);
  });

  it.each<[string, () => BusProvider<Transport>]>([
    ['a class', () => InnerTransport],
    ['a value', () => ({ useValue: new InnerTransport({ url: 'inner://' }) })],
    [
      'a factory with dependencies',
      () => ({
        inject: [Dependency],
        useFactory: () => new InnerTransport({ url: 'inner://' }),
      }),
    ],
    ['an existing provider', () => ({ useExisting: InnerTransport })],
  ])('relays to a transport given as %s', async (_name, provider) => {
    // arrange
    const module = await compile({
      imports: [
        BusModule.forRoot({
          transport: MemoryOutbox.withConfig({
            transport: provider(),
            interval: 10,
            batchSize: 10,
          }),
          providers: [
            { provide: Dependency, useValue: 'dep' },
            {
              provide: InnerTransport,
              useValue: new InnerTransport({ url: 'inner://' }),
            },
          ],
        }),
      ],
    });

    await module.init();

    // act
    await module
      .get<Dispatcher>(Dispatcher)
      .publish(new TestEvent({ value: 'a' }));

    // assert
    await vi.waitFor(() => expect(InnerTransport.publish).toHaveBeenCalled());
  });

  it('stops relaying when the module is closed', async () => {
    // arrange
    const module = await createModule();

    await module.init();
    await vi.waitFor(() => expect(MemoryOutbox.relay).toHaveBeenCalled());
    await close(module);

    const calls = MemoryOutbox.relay.mock.calls.length;

    // act
    await setTimeout(50);

    // assert
    expect(MemoryOutbox.relay).toHaveBeenCalledTimes(calls);
  });
});
