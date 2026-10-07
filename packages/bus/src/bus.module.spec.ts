import { Inject, Injectable, Scope } from '@nestjs/common';
import { REQUEST } from '@nestjs/core';
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

  it.each([
    {
      name: 'bus',
      busProviders: [TestHandler],
      parentProviders: [],
    },
    {
      name: 'parent',
      busProviders: [],
      parentProviders: [TestHandler],
    },
  ])(
    'registers handlers provided in $name module',
    async ({ busProviders, parentProviders: mainProviders }) => {
      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            group: 'test-app',
            transport: {
              provider: MockTransport,
              config: {},
            },
            providers: [...busProviders],
          }),
        ],
        providers: [...mainProviders],
      }).compile();

      await module.init();

      expect(MockTransport.subscribe).toHaveBeenCalledExactlyOnceWith(
        TestEvent,
        TestHandler.name,
        expect.any(Function),
      );

      await module.close();
    },
  );

  it('throws if a handler is provided in both BusModule and a parent module', async () => {
    const module = await Test.createTestingModule({
      imports: [
        BusModule.forRoot({
          group: 'test-app',
          transport: {
            provider: MockTransport,
            config: {},
          },
          providers: [TestHandler],
        }),
      ],
      providers: [TestHandler],
    }).compile();

    await expect(module.init()).rejects.toThrow(
      `Event handler ${TestHandler.name} is provided more than once`,
    );

    // Nest rethrows the failed initialization on close
    await module.close().catch(() => undefined);
  });

  describe('multiple events', () => {
    afterEach(() => {
      vi.clearAllMocks();
    });

    class OtherEvent {
      public value2: string;

      public constructor(props: OtherEvent) {
        this.value2 = props.value2;
      }
    }

    it('subscribes to each event', async () => {
      @EventHandler([TestEvent, OtherEvent])
      class MultiEventHandler {
        public handle = vi.fn();
      }

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
        providers: [MultiEventHandler],
      }).compile();

      await module.init();

      expect(MockTransport.subscribe).toHaveBeenCalledTimes(2);
      expect(MockTransport.subscribe).toHaveBeenCalledWith(
        TestEvent,
        MultiEventHandler.name,
        expect.any(Function),
      );
      expect(MockTransport.subscribe).toHaveBeenCalledWith(
        OtherEvent,
        MultiEventHandler.name,
        expect.any(Function),
      );

      await module.close();
    });

    it('dispatches every subscribed event to the handler', async () => {
      @EventHandler([TestEvent, OtherEvent])
      class MultiEventHandler {
        public handle = vi.fn<(event: EventPayload<object>) => void>();
      }

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
        providers: [MultiEventHandler],
      }).compile();

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);
      const testEvent = new TestEvent({ value: 'test' });
      const otherEvent = new OtherEvent({ value2: 'other' });

      await dispatcher.publish(testEvent);
      await dispatcher.publish(otherEvent);

      const handler = module.get(MultiEventHandler);
      expect(handler.handle).toHaveBeenCalledWith(
        expect.objectContaining({ event: testEvent }),
      );
      expect(handler.handle).toHaveBeenCalledWith(
        expect.objectContaining({ event: otherEvent }),
      );

      await module.close();
    });

    it('applies handler-level middleware to every subscribed event', async () => {
      @Injectable()
      class MultiEventMiddleware implements EventHandlerMiddleware {
        public static use = vi.fn(
          async (
            event: EventPayload<object>,
            next: (event: EventPayload<object>) => Promise<void>,
          ) => {
            await next(event);
          },
        );

        public use = MultiEventMiddleware.use;
      }

      @EventHandler([TestEvent, OtherEvent], {
        middlewares: [MultiEventMiddleware],
      })
      class MultiEventHandler {
        public handle = vi.fn();
      }

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
        providers: [MultiEventHandler, MultiEventMiddleware],
      }).compile();

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      await dispatcher.publish(new TestEvent({ value: 'test' }));
      await dispatcher.publish(new OtherEvent({ value2: 'other' }));

      expect(MultiEventMiddleware.use).toHaveBeenCalledTimes(2);

      await module.close();
    });

    it('throws when given an empty array of events', () => {
      expect(() => {
        @EventHandler([])
        class EmptyHandler {
          public handle = vi.fn();
        }

        return EmptyHandler;
      }).toThrow();
    });
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
            middlewares: [TestMiddleware],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [TestHandler, TestMiddleware],
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
            middlewares: [MiddlewareA, MiddlewareB],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [TestHandler, MiddlewareA, MiddlewareB],
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
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [TestHandler, HandlerWithMiddleware, HandlerMiddleware],
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
            middlewares: [BusLevelMiddleware],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [
          HandlerWithMiddleware,
          BusLevelMiddleware,
          HandlerLevelMiddleware,
        ],
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
            middlewares: [PayloadModifyingMiddleware],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [TestHandler, PayloadModifyingMiddleware],
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
            middlewares: [ShortCircuitMiddleware],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [TestHandler, ShortCircuitMiddleware],
      }).compile();

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      await dispatcher.publish(new TestEvent({ value: 'test' }));

      const handler = module.get(TestHandler);
      expect(handler.handle).not.toHaveBeenCalled();

      await module.close();
    });

    it('resolves middleware dependencies from the module providing it', async () => {
      @Injectable()
      class Dependency {
        public readonly seen = vi.fn();
      }

      @Injectable()
      class DependentMiddleware implements EventHandlerMiddleware {
        public constructor(
          @Inject(Dependency) private readonly dep: Dependency,
        ) {}

        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          this.dep.seen();
          await next(event);
        }
      }

      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            group: 'test-app',
            middlewares: [DependentMiddleware],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [TestHandler, Dependency, DependentMiddleware],
      }).compile();

      await module.init();

      await module
        .get<Dispatcher>(Dispatcher)
        .publish(new TestEvent({ value: 'test' }));

      expect(module.get(Dependency).seen).toHaveBeenCalledOnce();

      await module.close();
    });

    it('throws if a middleware is not provided in any module', async () => {
      @Injectable()
      class NotProvidedMiddleware implements EventHandlerMiddleware {
        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          await next(event);
        }
      }

      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            group: 'test-app',
            middlewares: [NotProvidedMiddleware],
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers: [TestHandler],
      }).compile();

      await expect(module.init()).rejects.toThrow(
        'Middleware NotProvidedMiddleware is not provided in any module',
      );

      await module.close().catch(() => undefined);
    });

    it('throws if a middleware is provided in more than one module', async () => {
      @Injectable()
      class DuplicatedMiddleware implements EventHandlerMiddleware {
        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          await next(event);
        }
      }

      const module = await Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            group: 'test-app',
            middlewares: [DuplicatedMiddleware],
            transport: {
              provider: MockTransport,
              config: {},
            },
            providers: [DuplicatedMiddleware],
          }),
        ],
        providers: [TestHandler, DuplicatedMiddleware],
      }).compile();

      await expect(module.init()).rejects.toThrow(
        'Middleware DuplicatedMiddleware is provided more than once',
      );

      await module.close().catch(() => undefined);
    });
  });

  describe('handler scopes', () => {
    const createModule = (providers: any[]) =>
      Test.createTestingModule({
        imports: [
          BusModule.forRoot({
            group: 'test-app',
            transport: {
              provider: MockTransport,
              config: {},
            },
          }),
        ],
        providers,
      }).compile();

    it('resolves a request-scoped handler for every event with the payload as request', async () => {
      const instances: object[] = [];
      const requests: unknown[] = [];

      @EventHandler(TestEvent, { scope: Scope.REQUEST })
      class ScopedHandler {
        public constructor(@Inject(REQUEST) request: unknown) {
          instances.push(this);
          requests.push(request);
        }

        public handle() {}
      }

      const module = await createModule([ScopedHandler]);

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);
      const first = new TestEvent({ value: 'first' });
      const second = new TestEvent({ value: 'second' });

      await dispatcher.publish(first);
      await dispatcher.publish(second);

      expect(instances).toHaveLength(2);
      expect(instances[0]).not.toBe(instances[1]);
      expect(requests).toEqual([
        expect.objectContaining({ event: first }),
        expect.objectContaining({ event: second }),
      ]);

      await module.close();
    });

    it('resolves a transient handler for every event', async () => {
      const instances: object[] = [];

      @EventHandler(TestEvent, { scope: Scope.TRANSIENT })
      class TransientHandler {
        public handle() {
          instances.push(this);
        }
      }

      const module = await createModule([TransientHandler]);

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      await dispatcher.publish(new TestEvent({ value: 'first' }));
      await dispatcher.publish(new TestEvent({ value: 'second' }));

      expect(instances).toHaveLength(2);
      expect(instances[0]).not.toBe(instances[1]);

      await module.close();
    });

    it('resolves a handler depending on a request-scoped provider for every event', async () => {
      const dependencies: object[] = [];

      @Injectable({ scope: Scope.REQUEST })
      class ScopedDependency {}

      @EventHandler(TestEvent)
      class DependentHandler {
        public constructor(
          @Inject(ScopedDependency) private readonly dep: ScopedDependency,
        ) {}

        public handle() {
          dependencies.push(this.dep);
        }
      }

      const module = await createModule([ScopedDependency, DependentHandler]);

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      await dispatcher.publish(new TestEvent({ value: 'first' }));
      await dispatcher.publish(new TestEvent({ value: 'second' }));

      expect(dependencies).toHaveLength(2);
      expect(dependencies[0]).not.toBe(dependencies[1]);

      await module.close();
    });

    it('registers a handler provided by a factory', async () => {
      @EventHandler(TestEvent)
      class FactoryHandler {
        public handle = vi.fn();
      }

      const handler = new FactoryHandler();
      const module = await createModule([
        { provide: 'FACTORY_HANDLER', useFactory: () => handler },
      ]);

      await module.init();

      await module
        .get<Dispatcher>(Dispatcher)
        .publish(new TestEvent({ value: 'test' }));

      expect(MockTransport.subscribe).toHaveBeenCalledExactlyOnceWith(
        TestEvent,
        FactoryHandler.name,
        expect.any(Function),
      );
      expect(handler.handle).toHaveBeenCalledOnce();

      await module.close();
    });
  });
});
