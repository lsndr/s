import { Inject, Injectable, Module, type OnModuleInit } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { SpanKind } from '@opentelemetry/api';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { BusModule } from '../bus.module.js';
import { EventHandler } from '../decorators/event-handler.decorator.js';
import { Dispatcher, type PublishEvent } from '../transport/dispatcher.js';
import { createEventDto, parseEventPayload } from '../transport/event-dto.js';
import { Outbox, type OutboxRecord } from '../transport/outbox/outbox.js';
import {
  type EventCallback,
  type Subscription,
} from '../transport/subscriber.js';
import {
  Transport,
  type TransportPublishOptions,
} from '../transport/transport.js';
import { type OnBusInit } from '../lifecycle.js';
import { type EventConstructor } from '../types.js';
import { createOtelFixture } from './otel/otel.fixture.js';
import {
  type BusTelemetry,
  type ContextCarrier,
  type ProcessOperation,
  type PublishOperation,
  type TelemetryDescriptor,
  type TransportTelemetry,
} from './telemetry.js';

class TestEvent {
  public constructor(public readonly value: string) {}
}

/** Delivers to the subscriptions in the process, like a broker would. */
@Injectable()
class MemoryTransport extends Transport {
  public static subscriptions: Subscription[] = [];
  public static published: TransportPublishOptions[] = [];

  public override getTelemetryDescriptor(): TelemetryDescriptor {
    return {
      system: 'memory',
      destination: (eventName, handler) =>
        handler ? `mem:${eventName}:${handler}` : `mem:${eventName}`,
    };
  }

  public assertHealthy(): Promise<void> {
    return Promise.resolve();
  }

  public async publish(
    eventOrEvents: object | PublishEvent<TransportPublishOptions>[],
    options?: TransportPublishOptions,
  ): Promise<void> {
    const events = Array.isArray(eventOrEvents)
      ? eventOrEvents
      : [{ event: eventOrEvents, options }];

    for (const { event, options } of events) {
      MemoryTransport.published.push(options ?? {});

      const dto = createEventDto(event, options);

      for (const subscription of MemoryTransport.subscriptions) {
        if (dto.name === subscription.event.name) {
          await subscription.callback(
            parseEventPayload(subscription.event, JSON.stringify(dto)),
            { attempt: 1 },
          );
        }
      }
    }
  }

  public subscribe(
    eventOrSubscriptions: EventConstructor<any> | Subscription[],
    handler?: string,
    callback?: EventCallback<any>,
  ): Promise<void> {
    MemoryTransport.subscriptions.push(
      ...(Array.isArray(eventOrSubscriptions)
        ? eventOrSubscriptions
        : [
            {
              event: eventOrSubscriptions,
              handler: handler!,
              callback: callback!,
            },
          ]),
    );

    return Promise.resolve();
  }
}

@Injectable()
class MemoryOutbox extends Outbox {
  public static pending: OutboxRecord[] = [];
  public static saved: OutboxRecord[] = [];

  protected save(records: OutboxRecord[]): Promise<void> {
    // Stored as a database would, so only what is persisted survives
    MemoryOutbox.pending.push(...structuredClone(records));
    MemoryOutbox.saved.push(...structuredClone(records));

    return Promise.resolve();
  }

  protected async relay(): Promise<number> {
    const batch = MemoryOutbox.pending.splice(0);

    if (batch.length > 0) {
      await this.dispatch(batch);
    }

    return batch.length;
  }
}

@EventHandler(TestEvent)
class TestHandler {
  public static handle = vi.fn();

  public handle(): void {
    TestHandler.handle();
  }
}

afterEach(() => {
  MemoryTransport.subscriptions = [];
  MemoryTransport.published = [];
  MemoryOutbox.pending = [];
  MemoryOutbox.saved = [];
  TestHandler.handle.mockReset();
});

describe('telemetry', () => {
  it('traces a message from publication to its handler', async () => {
    const otel = createOtelFixture();
    const module = await Test.createTestingModule({
      imports: [
        BusModule.forRoot({
          transport: { provider: MemoryTransport, config: {} },
          telemetry: otel.telemetry,
          providers: [TestHandler],
        }),
      ],
    }).compile();

    try {
      await module.init();
      await module.get<Dispatcher>(Dispatcher).publish(new TestEvent('a'), {
        id: 'id-1',
      });

      const send = otel.span('send mem:TestEvent');
      const process = otel.span('process mem:TestEvent:TestHandler');

      expect(TestHandler.handle).toHaveBeenCalledOnce();
      expect(send.kind).toBe(SpanKind.PRODUCER);
      expect(send.attributes['messaging.message.id']).toBe('id-1');
      expect(process.kind).toBe(SpanKind.CONSUMER);
      expect(process.attributes['messaging.message.id']).toBe('id-1');
      expect(process.attributes['bus.delivery.attempt']).toBe(1);
      expect(process.links.map((l) => l.context.spanId)).toEqual([
        send.spanContext().spanId,
      ]);
    } finally {
      await module.close();
      await otel.shutdown();
    }
  });

  it('keeps the creation context through the outbox and sends from the relay', async () => {
    const otel = createOtelFixture();
    const module = await Test.createTestingModule({
      imports: [
        BusModule.forRoot({
          transport: {
            provider: MemoryOutbox,
            config: {
              transport: { provider: MemoryTransport, config: {} },
              interval: 5,
            },
          },
          telemetry: otel.telemetry,
          providers: [TestHandler],
        }),
      ],
    }).compile();

    try {
      // The relay starts with the module, so handlers must be subscribed first
      await module.init();
      await module.get<Dispatcher>(Dispatcher).publish(new TestEvent('a'));
      await vi.waitFor(() => expect(TestHandler.handle).toHaveBeenCalledOnce());

      const create = otel.span('create mem:TestEvent');
      const send = otel.span('send mem:TestEvent');
      const process = otel.span('process mem:TestEvent:TestHandler');

      expect(create.kind).toBe(SpanKind.PRODUCER);
      expect(MemoryOutbox.saved[0]?.context?.['traceparent']).toContain(
        create.spanContext().spanId,
      );
      expect(send.kind).toBe(SpanKind.CLIENT);
      expect(send.links.map((l) => l.context.spanId)).toEqual([
        create.spanContext().spanId,
      ]);
      expect(process.links.map((l) => l.context.spanId)).toEqual([
        create.spanContext().spanId,
      ]);
      // The stored context was not replaced by the one of the relay
      expect(MemoryTransport.published[0]?.context?.['traceparent']).toContain(
        create.spanContext().spanId,
      );
      expect(
        otel.spans().filter((s) => s.name.startsWith('process')),
      ).toHaveLength(1);
      expect(
        (await otel.metric('bus.outbox.relayed'))[0]?.value,
      ).toBeGreaterThanOrEqual(1);
      expect(await otel.metric('bus.outbox.relay.duration')).toHaveLength(1);
    } finally {
      await module.close();
      await otel.shutdown();
    }
  });

  it('lets a message be published while the modules initialize, before it is reported', async () => {
    const otel = createOtelFixture();

    @Injectable()
    class Eager implements OnModuleInit {
      public constructor(
        @Inject(Dispatcher) private readonly bus: Dispatcher,
      ) {}

      public async onModuleInit(): Promise<void> {
        await this.bus.publish(new TestEvent('early'));
      }
    }

    @Module({ providers: [Eager] })
    class EagerModule {}

    const module = await Test.createTestingModule({
      imports: [
        BusModule.forRoot({
          transport: { provider: MemoryTransport, config: {} },
          telemetry: otel.telemetry,
        }),
        EagerModule,
      ],
    }).compile();

    try {
      await module.init();

      // Published before the bus bootstrapped, so it was not instrumented yet
      expect(MemoryTransport.published).toHaveLength(1);
      expect(otel.spans()).toEqual([]);

      await module.get<Dispatcher>(Dispatcher).publish(new TestEvent('later'));

      expect(otel.span('send mem:TestEvent')).toBeDefined();
    } finally {
      await module.close();
      await otel.shutdown();
    }
  });

  it('runs the transports hooks once, with or without telemetry', async () => {
    const otel = createOtelFixture();
    const bootstrap = vi.fn();

    @Injectable()
    class HookedTransport extends MemoryTransport {
      public onApplicationBootstrap(): void {
        bootstrap();
      }
    }

    for (const telemetry of [undefined, otel.telemetry]) {
      bootstrap.mockClear();

      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            transport: { provider: HookedTransport, config: {} },
            telemetry,
          }),
        ],
      }).compile();

      await module.init();
      await module.close();

      expect(bootstrap).toHaveBeenCalledOnce();
    }

    await otel.shutdown();
  });

  describe('custom telemetry', () => {
    class Recording implements BusTelemetry, OnBusInit {
      public readonly inited = vi.fn();
      public readonly publishes: PublishOperation[] = [];
      public readonly processes: ProcessOperation[] = [];
      public readonly descriptors: TelemetryDescriptor[] = [];
      public readonly counted = vi.fn();

      public async onBusInit(): Promise<void> {
        await Promise.resolve();
        this.inited();
      }

      public publish<T>(
        operation: PublishOperation,
        fn: (contexts: ContextCarrier[]) => Promise<T>,
      ): Promise<T> {
        this.publishes.push(operation);

        return fn(
          operation.messages.map((m) => m.context ?? { custom: 'carrier' }),
        );
      }

      public process<T>(
        operation: ProcessOperation,
        fn: () => Promise<T>,
      ): Promise<T> {
        this.processes.push(operation);

        return fn();
      }

      public transport(descriptor: TelemetryDescriptor): TransportTelemetry {
        this.descriptors.push(descriptor);

        return {
          span: (_options, fn) => fn(),
          counter: (name) => ({
            add: (value) => this.counted(name, value),
          }),
          histogram: () => ({ record: () => undefined }),
          gauge: () => undefined,
        };
      }
    }

    @Injectable()
    class ReportingTransport extends MemoryTransport {
      public override async publish(
        eventOrEvents: object | PublishEvent<TransportPublishOptions>[],
        options?: TransportPublishOptions,
      ): Promise<void> {
        this.telemetry.counter('published').add(1);

        await super.publish(eventOrEvents, options);
      }
    }

    it('is initialized first and receives what the bus and the transport report', async () => {
      const telemetry = new Recording();
      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            transport: { provider: ReportingTransport, config: {} },
            telemetry,
            providers: [TestHandler],
          }),
        ],
      }).compile();

      await module.init();
      await module.get<Dispatcher>(Dispatcher).publish(new TestEvent('a'), {
        id: 'id-1',
      });

      expect(telemetry.inited).toHaveBeenCalledOnce();
      expect(telemetry.publishes).toHaveLength(1);
      expect(telemetry.publishes[0]).toMatchObject({
        type: 'send',
        messages: [{ id: 'id-1', name: 'TestEvent' }],
      });
      expect(telemetry.descriptors.every((d) => d.system === 'memory')).toBe(
        true,
      );
      expect(telemetry.processes[0]).toMatchObject({
        eventName: 'TestEvent',
        handler: 'TestHandler',
        messageId: 'id-1',
        context: { custom: 'carrier' },
        delivery: { attempt: 1 },
      });
      expect(telemetry.counted).toHaveBeenCalledWith('published', 1);

      await module.close();
    });

    it('does not need to instrument the transports', async () => {
      const counted = vi.fn();
      const telemetry: BusTelemetry = {
        transport: () => ({
          span: (_options, fn) => fn(),
          counter: (name) => ({ add: (value) => counted(name, value) }),
          histogram: () => ({ record: () => undefined }),
          gauge: () => undefined,
        }),
      };
      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            transport: { provider: ReportingTransport, config: {} },
            telemetry,
            providers: [TestHandler],
          }),
        ],
      }).compile();

      await module.init();
      await module.get<Dispatcher>(Dispatcher).publish(new TestEvent('a'));

      expect(TestHandler.handle).toHaveBeenCalledOnce();
      expect(counted).toHaveBeenCalledWith('published', 1);

      await module.close();
    });

    it('reports an outbox as creating the events and the transport it relays to as sending them', async () => {
      const telemetry = new Recording();
      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            transport: {
              provider: MemoryOutbox,
              config: {
                transport: { provider: MemoryTransport, config: {} },
                interval: 5,
              },
            },
            telemetry,
            providers: [TestHandler],
          }),
        ],
      }).compile();

      await module.init();
      await module.get<Dispatcher>(Dispatcher).publish(new TestEvent('a'), {
        id: 'id-1',
      });
      await vi.waitFor(() => expect(TestHandler.handle).toHaveBeenCalledOnce());

      expect(telemetry.publishes.map((p) => p.type)).toEqual([
        'create',
        'send',
      ]);
      // The relay sends with the context of the creation
      expect(telemetry.publishes[1]?.messages[0]?.context).toEqual({
        custom: 'carrier',
      });
      // Only the transport the outbox relays to is subscribed through
      expect(telemetry.processes).toHaveLength(1);

      await module.close();
    });

    it('gives the transport an outbox relays to the telemetry', async () => {
      const telemetry = new Recording();
      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            transport: {
              provider: MemoryOutbox,
              config: {
                transport: { provider: ReportingTransport, config: {} },
                interval: 5,
              },
            },
            telemetry,
          }),
        ],
      }).compile();

      await module.init();
      await module.get<Dispatcher>(Dispatcher).publish(new TestEvent('a'));
      await vi.waitFor(() =>
        expect(telemetry.counted).toHaveBeenCalledWith('published', 1),
      );

      await module.close();
    });

    it('names the system after the class of a transport without a descriptor', () => {
      @Injectable()
      class Bare extends Transport {
        public assertHealthy(): Promise<void> {
          return Promise.resolve();
        }

        public publish(): Promise<void> {
          return Promise.resolve();
        }

        public subscribe(): Promise<void> {
          return Promise.resolve();
        }

        public describe(): TelemetryDescriptor {
          return this.getTelemetryDescriptor();
        }
      }

      expect(new Bare().describe()).toEqual({ system: 'Bare' });
    });
  });
});
