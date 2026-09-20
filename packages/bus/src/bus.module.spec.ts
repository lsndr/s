import { Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { BusModule } from './bus.module.js';
import { EventHandler } from './decorators/event-handler.decorator.js';
import { Transport } from './transport/transport.js';
import { Dispatcher, type PublishOptions } from './transport/dispatcher.js';
import { type EventCallback } from './transport/subscriber.js';
import { type EventConstructor, EventPayload } from './types.js';
import { type EventHandlerMiddleware } from './middleware/middleware.js';

class TestEvent {
  public value: string;

  public constructor(props: TestEvent) {
    this.value = props.value;
  }
}

@Injectable()
class MockTransport extends Transport {
  public static publish = vi.fn(
    async <E extends object>(event: E, options?: PublishOptions) => {
      const payload: EventPayload<E> = {
        id: options?.id ?? '1',
        event,
        meta: options?.meta ?? {},
        publishedAt: new Date(),
      };

      for (const sub of MockTransport.subscriptions) {
        if (event instanceof sub.event) {
          await sub.callback(payload);
        }
      }
    },
  );
  public static subscribe = vi.fn((event: any, handler: any, callback: any) => {
    MockTransport.subscriptions.push({
      event: event as EventConstructor<object>,
      callback: callback as EventCallback<object>,
    });

    return Promise.resolve();
  });
  public static assertHealthy = vi.fn().mockResolvedValue(undefined);

  private static readonly subscriptions: {
    event: EventConstructor<object>;
    callback: EventCallback<object>;
  }[] = [];

  public assertHealthy = MockTransport.assertHealthy;
  public subscribe = MockTransport.subscribe;
  public publish = MockTransport.publish;
}

@EventHandler(TestEvent)
class TestHandler {
  public handle = vi.fn<(event: EventPayload<TestEvent>) => Promise<void>>();
}

describe('BusModule', () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it('provides Dispatcher', async () => {
    const module = await Test.createTestingModule({
      imports: [
        BusModule.forRoot({
          group: 'test-app',
          transport: {
            provider: MockTransport,
            config: {},
          },
        }),
      ],
    }).compile();

    const dispatcher = module.get<Dispatcher>(Dispatcher);
    expect(dispatcher).toBeInstanceOf(MockTransport);

    await module.close();
  });

  it('registers explicit even handlers', async () => {
    const module = await Test.createTestingModule({
      imports: [
        BusModule.forRoot({
          group: 'test-app',
          handlers: [TestHandler],
          transport: {
            provider: MockTransport,
            config: {},
          },
        }),
      ],
      providers: [TestHandler],
    }).compile();

    await module.init();

    expect(MockTransport.subscribe).toHaveBeenCalledExactlyOnceWith(
      TestEvent,
      TestHandler.name,
      expect.any(Function),
    );

    await module.close();
  });

  it('does not register implicit event handlers', async () => {
    const module = await Test.createTestingModule({
      imports: [
        BusModule.forRoot({
          group: 'test-app',
          transport: {
            provider: MockTransport,
            config: {},
          },
        }),
      ],
      providers: [TestHandler],
    }).compile();

    await module.init();

    expect(MockTransport.subscribe).not.toHaveBeenCalled();

    await module.close();
  });

  describe('middlewares', () => {
    afterEach(() => {
      vi.clearAllMocks();
    });

    it('applies bus-level middleware to handler', async () => {
      @Injectable()
      class TestMiddleware implements EventHandlerMiddleware {
        public static use = vi.fn(
          async (
            event: EventPayload<object>,
            next: (event: EventPayload<object>) => Promise<void>,
          ) => {
            await next(event);
          },
        );

        public use = TestMiddleware.use;
      }

      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            group: 'test-app',
            handlers: [TestHandler],
            middlewares: [TestMiddleware],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [TestHandler],
      }).compile();

      await module.init();

      const event = new TestEvent({ value: 'test' });
      const dispatcher = module.get<Dispatcher>(Dispatcher);

      await dispatcher.publish(event);

      expect(TestMiddleware.use).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ event }),
        expect.any(Function),
      );

      const handler = module.get(TestHandler);
      expect(handler.handle).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ event }),
      );

      await module.close();
    });

    it('executes multiple bus-level middlewares in declared order', async () => {
      const executionOrder: string[] = [];

      @Injectable()
      class MiddlewareA implements EventHandlerMiddleware {
        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          executionOrder.push('A:before');
          await next(event);
          executionOrder.push('A:after');
        }
      }

      @Injectable()
      class MiddlewareB implements EventHandlerMiddleware {
        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          executionOrder.push('B:before');
          await next(event);
          executionOrder.push('B:after');
        }
      }

      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            group: 'test-app',
            handlers: [TestHandler],
            middlewares: [MiddlewareA, MiddlewareB],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [TestHandler],
      }).compile();

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      await dispatcher.publish(new TestEvent({ value: 'test' }));

      expect(executionOrder).toEqual([
        'A:before',
        'B:before',
        'B:after',
        'A:after',
      ]);

      await module.close();
    });

    it('applies handler-level middleware only to the decorated handler', async () => {
      @Injectable()
      class HandlerMiddleware implements EventHandlerMiddleware {
        public static use = vi.fn(
          async (
            event: EventPayload<object>,
            next: (event: EventPayload<object>) => Promise<void>,
          ) => {
            await next(event);
          },
        );

        public use = HandlerMiddleware.use;
      }

      class AnotherEvent {
        public value: string;

        public constructor(props: AnotherEvent) {
          this.value = props.value;
        }
      }

      @EventHandler(AnotherEvent, { middlewares: [HandlerMiddleware] })
      class HandlerWithMiddleware {
        public handle = vi.fn();
      }

      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            group: 'test-app',
            handlers: [TestHandler, HandlerWithMiddleware],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [TestHandler, HandlerWithMiddleware],
      }).compile();

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      await dispatcher.publish(new TestEvent({ value: 'test' }));
      expect(HandlerMiddleware.use).not.toHaveBeenCalled();

      await dispatcher.publish(new AnotherEvent({ value: 'test' }));
      expect(HandlerMiddleware.use).toHaveBeenCalledOnce();

      await module.close();
    });

    it('executes bus-level middlewares before handler-level middlewares', async () => {
      const executionOrder: string[] = [];

      @Injectable()
      class BusLevelMiddleware implements EventHandlerMiddleware {
        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          executionOrder.push('bus');
          await next(event);
        }
      }

      @Injectable()
      class HandlerLevelMiddleware implements EventHandlerMiddleware {
        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          executionOrder.push('handler');
          await next(event);
        }
      }

      @EventHandler(TestEvent, { middlewares: [HandlerLevelMiddleware] })
      class HandlerWithMiddleware {
        public handle = vi.fn(() => {
          executionOrder.push('handle');
        });
      }

      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            group: 'test-app',
            handlers: [HandlerWithMiddleware],
            middlewares: [BusLevelMiddleware],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [HandlerWithMiddleware],
      }).compile();

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      await dispatcher.publish(new TestEvent({ value: 'test' }));

      expect(executionOrder).toEqual(['bus', 'handler', 'handle']);

      await module.close();
    });

    it('allows middleware to modify event payload', async () => {
      @Injectable()
      class PayloadModifyingMiddleware implements EventHandlerMiddleware {
        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          await next({ ...event, meta: { ...event.meta, injected: 'value' } });
        }
      }

      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            group: 'test-app',
            handlers: [TestHandler],
            middlewares: [PayloadModifyingMiddleware],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [TestHandler],
      }).compile();

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      await dispatcher.publish(new TestEvent({ value: 'test' }));

      const handler = module.get(TestHandler);
      expect(handler.handle).toHaveBeenCalledWith(
        expect.objectContaining({ meta: { injected: 'value' } }),
      );

      await module.close();
    });

    it('allows middleware to short-circuit by not calling next', async () => {
      @Injectable()
      class ShortCircuitMiddleware implements EventHandlerMiddleware {
        public async use() {
          // intentionally not calling next
        }
      }

      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            group: 'test-app',
            handlers: [TestHandler],
            middlewares: [ShortCircuitMiddleware],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [TestHandler],
      }).compile();

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      await dispatcher.publish(new TestEvent({ value: 'test' }));

      const handler = module.get(TestHandler);
      expect(handler.handle).not.toHaveBeenCalled();

      await module.close();
    });
  });
});
