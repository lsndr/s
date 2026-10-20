import { Inject, Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
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
import { type TransportProvider } from '../provider.js';
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

  public static withConfig(
    config: InnerConfig,
  ): TransportProvider<InnerTransport> {
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

async function createModule(
  batchSize = 10,
  transport = InnerTransport.withConfig({
    url: 'inner://',
  }),
) {
  const module = await Test.createTestingModule({
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
  }).compile();

  return module;
}

describe('Outbox', () => {
  afterEach(() => {
    vi.clearAllMocks();
    InnerTransport.publish.mockReset();
    MemoryOutbox.pending = [];
  });

  it('stores published events without publishing them', async () => {
    const module = await createModule();
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    await dispatcher.publish(new TestEvent({ value: 'a' }), {
      id: 'id-1',
      meta: { key: 'value' },
    });

    expect(MemoryOutbox.pending).toEqual([
      {
        id: 'id-1',
        name: 'TestEvent',
        event: { value: 'a' },
        meta: { key: 'value' },
        publishedAt: expect.any(Date),
      },
    ]);
    expect(InnerTransport.publish).not.toHaveBeenCalled();

    await module.close();
  });

  it('forwards the bus lifecycle hooks to the inner transport', async () => {
    const module = await createModule();

    await module.init();

    expect(InnerTransport.onBusBootstrap).toHaveBeenCalledOnce();
    expect(InnerTransport.onBusShutdown).not.toHaveBeenCalled();

    await module.close();

    expect(InnerTransport.onBusShutdown).toHaveBeenCalledOnce();
  });

  it('stores a batch of events with a single save', async () => {
    const module = await createModule();
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    await dispatcher.publish([
      { event: new TestEvent({ value: 'a' }), options: { id: 'id-1' } },
      { event: new TestEvent({ value: 'b' }), options: { id: 'id-2' } },
    ]);

    expect(MemoryOutbox.save).toHaveBeenCalledOnce();
    expect(MemoryOutbox.pending.map(({ id }) => id)).toEqual(['id-1', 'id-2']);

    await dispatcher.publish([]);
    expect(MemoryOutbox.save).toHaveBeenCalledOnce();

    await module.close();
  });

  it('relays a batch of stored events with a single publish', async () => {
    const module = await createModule(2);
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    await dispatcher.publish([
      { event: new TestEvent({ value: 'a' }), options: { id: 'id-1' } },
      { event: new TestEvent({ value: 'b' }), options: { id: 'id-2' } },
    ]);

    await module.init();
    await vi.waitFor(() => expect(MemoryOutbox.pending).toHaveLength(0));

    expect(InnerTransport.publish).toHaveBeenCalledOnce();
    expect(
      InnerTransport.publish.mock.calls[0]![0].map(
        ({ options }) => options?.id,
      ),
    ).toEqual(['id-1', 'id-2']);

    await module.close();
  });

  it('relays stored events to the transport as they were stored', async () => {
    const module = await createModule(1);
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    await dispatcher.publish(new TestEvent({ value: 'a' }), { id: 'id-1' });
    await dispatcher.publish(new TestEvent({ value: 'b' }), { id: 'id-2' });
    const [first, second] = MemoryOutbox.pending;

    await module.init();
    await vi.waitFor(() => expect(MemoryOutbox.pending).toHaveLength(0));

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
            },
          },
        ],
      ],
    ]);

    await module.close();
  });

  it('keeps records pending and retries when the transport fails', async () => {
    InnerTransport.publish.mockRejectedValueOnce(new Error('broker down'));

    const module = await createModule();
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    await dispatcher.publish(new TestEvent({ value: 'a' }), { id: 'id-1' });
    await module.init();

    await vi.waitFor(() => expect(MemoryOutbox.pending).toHaveLength(0));
    expect(InnerTransport.publish).toHaveBeenCalledTimes(2);

    await module.close();
  });

  it('subscribes handlers and checks health through the transport', async () => {
    const module = await createModule();

    await module.init();

    expect(InnerTransport.subscribe).toHaveBeenCalledExactlyOnceWith([
      {
        event: TestEvent,
        handler: TestHandler.name,
        callback: expect.any(Function),
      },
    ]);

    await module.get<Health>(Health).assertHealthy();
    expect(InnerTransport.assertHealthy).toHaveBeenCalledOnce();

    await module.close();
  });

  it('gives the outbox its config and its dependencies', async () => {
    const module = await createModule(7);
    const outbox = module.get<MemoryOutbox>(Transport);

    expect(outbox.getConfig()).toMatchObject({ batchSize: 7, interval: 10 });
    expect(outbox.dependency).toBe('dep');
    expect(outbox.getInnerTransport()).toBeInstanceOf(InnerTransport);

    await module.close();
  });

  it.each<[string, () => TransportProvider]>([
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
    const module = await Test.createTestingModule({
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
    }).compile();

    await module.init();
    await module
      .get<Dispatcher>(Dispatcher)
      .publish(new TestEvent({ value: 'a' }));
    await vi.waitFor(() => expect(InnerTransport.publish).toHaveBeenCalled());

    await module.close();
  });

  it('stops relaying when the module is closed', async () => {
    const module = await createModule();

    await module.init();
    await vi.waitFor(() => expect(MemoryOutbox.relay).toHaveBeenCalled());
    await module.close();

    const calls = MemoryOutbox.relay.mock.calls.length;
    await setTimeout(50);

    expect(MemoryOutbox.relay).toHaveBeenCalledTimes(calls);
  });
});
