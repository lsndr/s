import { Inject, Injectable, type ModuleMetadata } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import { setTimeout } from 'node:timers/promises';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { BusModule } from '../../bus.module.js';
import { EventHandler } from '../../decorators/event-handler.decorator.js';
import {
  Dispatcher,
  type PublishEvent,
  type PublishOptions,
} from '../dispatcher.js';
import { type EventCallback, type Subscription } from '../subscriber.js';
import { type EventConstructor } from '../../types.js';
import { Health } from '../health.js';
import { Transport } from '../transport.js';
import { type BusProvider } from '../../provider.js';
import { Serializer } from '../../serialization/serializer.js';
import { type JsonSerializer } from '../../serialization/json.serializer.js';
import { NoopTelemetry } from '../../telemetry/noop.telemetry.js';
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
  public static publish = vi.fn<(events: PublishEvent[]) => Promise<void>>();
  public static subscribe = vi
    .fn<(subscriptions: Subscription[]) => Promise<void>>()
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

  public publish(
    eventOrEvents: object | PublishEvent[],
    options?: PublishOptions,
  ): Promise<void> {
    const events = Array.isArray(eventOrEvents)
      ? eventOrEvents
      : [{ event: eventOrEvents, options }];

    return InnerTransport.publish(events);
  }

  public subscribe(
    eventOrSubscriptions: EventConstructor<any> | Subscription[],
    handler?: string,
    callback?: EventCallback<any>,
  ): Promise<void> {
    const subscriptions = Array.isArray(eventOrSubscriptions)
      ? eventOrSubscriptions
      : [
          {
            event: eventOrSubscriptions,
            handler: handler!,
            callback: callback!,
          },
        ];

    return InnerTransport.subscribe(subscriptions);
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

  protected save(records: OutboxRecord[]): Promise<void> {
    MemoryOutbox.save(records);
    MemoryOutbox.pending.push(...records);

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

  it('stores a batch of events with a single save', async () => {
    // arrange
    const module = await createModule();
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    // act
    await dispatcher.publish([
      { event: new TestEvent({ value: 'a' }), options: { id: 'id-1' } },
      { event: new TestEvent({ value: 'b' }), options: { id: 'id-2' } },
    ]);

    // assert
    expect(MemoryOutbox.save).toHaveBeenCalledOnce();
    expect(MemoryOutbox.pending.map(({ id }) => id)).toEqual(['id-1', 'id-2']);
  });

  it('does not save an empty batch', async () => {
    // arrange
    const module = await createModule();
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    // act
    await dispatcher.publish([]);

    // assert
    expect(MemoryOutbox.save).not.toHaveBeenCalled();
  });

  it('relays a batch of stored events with a single publish', async () => {
    // arrange
    const module = await createModule(2);
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    await dispatcher.publish([
      { event: new TestEvent({ value: 'a' }), options: { id: 'id-1' } },
      { event: new TestEvent({ value: 'b' }), options: { id: 'id-2' } },
    ]);

    // act
    await module.init();
    await vi.waitFor(() => expect(MemoryOutbox.pending).toHaveLength(0));

    // assert
    expect(InnerTransport.publish).toHaveBeenCalledOnce();
    expect(
      InnerTransport.publish.mock.calls[0]![0].map(
        ({ options }) => options?.id,
      ),
    ).toEqual(['id-1', 'id-2']);
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
    expect(
      InnerTransport.publish.mock.calls[0]![0][0]!.options?.headers,
    ).toEqual({ traceparent: 'tp' });
  });

  it('wraps storing the records in a telemetry create', async () => {
    // arrange
    const create = vi.spyOn(NoopTelemetry.prototype, 'create');
    const module = await createModule();
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    // act
    await dispatcher.publish(new TestEvent({ value: 'a' }), { id: 'id-1' });

    // assert
    expect(create).toHaveBeenCalledWith(
      { name: 'outbox' },
      [{ id: 'id-1', name: 'TestEvent', headers: {} }],
      expect.any(Function),
    );
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
        [
          {
            event: { value: 'a' },
            options: {
              id: 'id-1',
              name: 'TestEvent',
              meta: {},
              publishedAt: first!.publishedAt,
              headers: {},
            },
          },
        ],
      ],
      [
        [
          {
            event: { value: 'b' },
            options: {
              id: 'id-2',
              name: 'TestEvent',
              meta: {},
              publishedAt: second!.publishedAt,
              headers: {},
            },
          },
        ],
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
    expect(InnerTransport.subscribe).toHaveBeenCalledExactlyOnceWith([
      {
        event: TestEvent,
        handler: TestHandler.name,
        callback: expect.any(Function),
      },
    ]);
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
