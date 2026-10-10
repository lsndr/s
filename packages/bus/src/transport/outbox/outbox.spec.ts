import { Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { setTimeout } from 'node:timers/promises';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { BusModule } from '../../bus.module.js';
import { EventHandler } from '../../decorators/event-handler.decorator.js';
import { Dispatcher, type PublishEvent } from '../dispatcher.js';
import { type EventCallback, type Subscription } from '../subscriber.js';
import { type EventConstructor } from '../../types.js';
import { Health } from '../health.js';
import { Transport, type TransportPublishOptions } from '../transport.js';
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

@Injectable()
class InnerTransport extends Transport<InnerConfig> {
  public static publish =
    vi.fn<(events: PublishEvent<TransportPublishOptions>[]) => Promise<void>>();
  public static subscribe = vi
    .fn<(subscriptions: Subscription[]) => Promise<void>>()
    .mockResolvedValue(undefined);
  public static assertHealthy = vi.fn().mockResolvedValue(undefined);
  public static configs: InnerConfig[] = [];

  public assertHealthy = InnerTransport.assertHealthy;

  public getConfig(): InnerConfig {
    return this.config;
  }

  public publish(
    eventOrEvents: object | PublishEvent<TransportPublishOptions>[],
    options?: TransportPublishOptions,
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

@Injectable()
class MemoryOutbox extends Outbox<MemoryOutboxConfig> {
  public static pending: OutboxRecord[] = [];
  public static relay = vi.fn();
  public static save = vi.fn();

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

async function createModule(batchSize = 10) {
  const module = await Test.createTestingModule({
    imports: [
      BusModule.forRoot({
        transport: {
          provider: MemoryOutbox,
          config: {
            transport: {
              provider: InnerTransport,
              config: { url: 'inner://' },
            },
            interval: 10,
            batchSize,
          },
        },
        providers: [TestHandler],
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

  it('gives the transport and the outbox their own config', async () => {
    const module = await createModule(7);

    expect(module.get(MemoryOutbox).getConfig()).toMatchObject({
      batchSize: 7,
      interval: 10,
    });
    expect(module.get(InnerTransport, { strict: false }).getConfig()).toEqual({
      url: 'inner://',
    });

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
