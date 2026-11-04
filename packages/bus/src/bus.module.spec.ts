import {
  Inject,
  Injectable,
  Logger,
  Module,
  type ModuleMetadata,
  Optional,
  Scope,
  type Type,
} from '@nestjs/common';
import { REQUEST } from '@nestjs/core';
import { Test, type TestingModule } from '@nestjs/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { BusModule } from './bus.module.js';
import { EventHandler } from './decorators/event-handler.decorator.js';
import { Transport } from './transport/transport.js';
import { Health } from './transport/health.js';
import { type BusProvider } from './provider.js';
import {
  Dispatcher,
  type PublishEvent,
  type PublishOptions,
} from './transport/dispatcher.js';
import {
  type EventCallback,
  type Subscription,
} from './transport/subscriber.js';
import { type EventConstructor, EventPayload } from './types.js';
import {
  type SerializedMessage,
  Serializer,
} from './serialization/serializer.js';
import { JsonSerializer } from './serialization/json.serializer.js';
import { type EventHandlerMiddleware } from './middleware/middleware.js';
import {
  type BusTelemetry,
  noopTransportTelemetry,
  Telemetry,
  type TransportTelemetry,
} from './telemetry/telemetry.js';
import {
  type OnBusBootstrap,
  type OnBusInit,
  type OnBusShutdown,
} from './lifecycle.js';

class TestEvent {
  public value: string;

  public constructor(props: TestEvent) {
    this.value = props.value;
  }
}

@Injectable()
class MockTransport implements Transport {
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
  public static subscribe = vi.fn((subscriptions: Subscription[]) => {
    MockTransport.subscriptions.push(...subscriptions);

    return Promise.resolve();
  });
  public static assertHealthy = vi.fn().mockResolvedValue(undefined);
  public static onBusBootstrap = vi.fn();
  public static onBusShutdown = vi.fn();

  private static readonly subscriptions: Subscription[] = [];

  public assertHealthy = MockTransport.assertHealthy;

  public onBusBootstrap(): void {
    MockTransport.onBusBootstrap();
  }

  public onBusShutdown(): void {
    MockTransport.onBusShutdown();
  }

  public async publish(
    eventOrEvents: object | PublishEvent[],
    options?: PublishOptions,
  ): Promise<void> {
    const events = Array.isArray(eventOrEvents)
      ? eventOrEvents
      : [{ event: eventOrEvents, options }];

    for (const item of events) {
      await MockTransport.publish(item.event, item.options);
    }
  }

  public async subscribe(
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

    await MockTransport.subscribe(subscriptions);
  }
}

@EventHandler(TestEvent)
class TestHandler {
  public handle = vi.fn<(event: EventPayload<TestEvent>) => Promise<void>>();
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

describe('BusModule', () => {
  afterEach(async () => {
    for (const module of modules.splice(0)) {
      // nest rethrows the failed initialization of a module on close
      await close(module).catch(() => undefined);
    }

    vi.clearAllMocks();
    vi.restoreAllMocks();
  });

  it('provides Dispatcher', async () => {
    // arrange
    const module = await compile({
      imports: [
        BusModule.forRoot({
          transport: MockTransport,
        }),
      ],
    });

    // act
    const dispatcher = module.get<Dispatcher>(Dispatcher);

    // assert
    expect(dispatcher).toBeInstanceOf(MockTransport);
  });

  it('provides Health', async () => {
    // arrange
    const module = await compile({
      imports: [
        BusModule.forRoot({
          transport: MockTransport,
        }),
      ],
    });

    // act
    const health = module.get<Health>(Health);

    // assert
    expect(health).toBe(module.get(Transport));
  });

  it('checks health through the transport', async () => {
    // arrange
    const module = await compile({
      imports: [
        BusModule.forRoot({
          transport: MockTransport,
        }),
      ],
    });

    // act
    await module.get<Health>(Health).assertHealthy();

    // assert
    expect(MockTransport.assertHealthy).toHaveBeenCalledOnce();
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
      // arrange
      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: MockTransport,
            providers: [...busProviders],
          }),
        ],
        providers: [...mainProviders],
      });

      // act
      await module.init();

      // assert
      expect(MockTransport.subscribe).toHaveBeenCalledExactlyOnceWith([
        {
          event: TestEvent,
          handler: TestHandler.name,
          callback: expect.any(Function),
        },
      ]);
    },
  );

  describe('transport providers', () => {
    afterEach(() => {
      vi.clearAllMocks();
    });

    @Injectable()
    class ConfiguredTransport extends MockTransport {
      public constructor(@Inject('CONFIG') public readonly config: string) {
        super();
      }
    }

    const providers: [string, () => BusProvider<Transport>][] = [
      ['a class', () => MockTransport],
      ['useClass', () => ({ useClass: MockTransport })],
      ['useValue', () => ({ useValue: new MockTransport() })],
      ['useFactory', () => ({ useFactory: () => new MockTransport() })],
    ];

    it.each(providers)(
      'provides a transport given as %s',
      async (_name, transport) => {
        // arrange
        const module = await compile({
          imports: [BusModule.forRoot({ transport: transport() })],
        });

        // act
        await module.init();

        // assert
        expect(module.get(Transport)).toBeInstanceOf(MockTransport);
        expect(module.get(Dispatcher)).toBe(module.get(Transport));
        expect(MockTransport.onBusBootstrap).toHaveBeenCalledOnce();
      },
    );

    it.each(providers)(
      'publishes through a transport given as %s',
      async (_name, transport) => {
        // arrange
        const module = await compile({
          imports: [BusModule.forRoot({ transport: transport() })],
        });

        await module.init();

        // act
        await module
          .get<Dispatcher>(Dispatcher)
          .publish(new TestEvent({ value: 'test' }));

        // assert
        expect(MockTransport.publish).toHaveBeenCalledOnce();
      },
    );

    it.each(providers)(
      'shuts down a transport given as %s',
      async (_name, transport) => {
        // arrange
        const module = await compile({
          imports: [BusModule.forRoot({ transport: transport() })],
        });

        await module.init();

        // act
        await close(module);

        // assert
        expect(MockTransport.onBusShutdown).toHaveBeenCalledOnce();
      },
    );

    it('injects the dependencies of a class', async () => {
      // arrange
      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: ConfiguredTransport,
            providers: [{ provide: 'CONFIG', useValue: 'value' }],
          }),
        ],
      });

      // act
      const transport = module.get<ConfiguredTransport>(Transport);

      // assert
      expect(transport.config).toBe('value');
    });

    it('registers the additional providers of the transport', async () => {
      // arrange
      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: {
              inject: ['CONFIG'],
              useFactory: (config: string) => new ConfiguredTransport(config),
              providers: [{ provide: 'CONFIG', useValue: 'extra' }],
            },
          }),
        ],
      });

      // act
      const transport = module.get<ConfiguredTransport>(Transport);

      // assert
      expect(transport.config).toBe('extra');
    });

    describe('provided in another module', () => {
      @Module({
        providers: [MockTransport],
        exports: [MockTransport],
      })
      class TransportModule {}

      async function setup() {
        const module = await compile({
          imports: [
            TransportModule,
            BusModule.forRoot({
              imports: [TransportModule],
              transport: { useExisting: MockTransport },
            }),
          ],
        });

        await module.init();

        return module;
      }

      it('publishes through it', async () => {
        // arrange
        const module = await setup();

        // act
        await module
          .get<Dispatcher>(Dispatcher)
          .publish(new TestEvent({ value: 'a' }));

        // assert
        expect(MockTransport.publish).toHaveBeenCalledOnce();
      });

      it('bootstraps it', async () => {
        // act
        await setup();

        // assert
        expect(MockTransport.onBusBootstrap).toHaveBeenCalledOnce();
      });

      it('shuts it down', async () => {
        // arrange
        const module = await setup();

        // act
        await close(module);

        // assert
        expect(MockTransport.onBusShutdown).toHaveBeenCalledOnce();
      });
    });
  });

  describe('serializer', () => {
    afterEach(() => {
      vi.clearAllMocks();
    });

    class CustomSerializer implements Serializer, OnBusInit {
      public static initialized = false;

      public onBusInit(): void {
        CustomSerializer.initialized = true;
      }

      public serialize(): SerializedMessage {
        return {
          id: '1',
          name: 'TestEvent',
          publishedAt: new Date(),
          data: '',
        };
      }

      public deserialize(): never {
        throw new Error('not implemented');
      }
    }

    it('uses JsonSerializer by default', async () => {
      // arrange
      const module = await compile({
        imports: [BusModule.forRoot({ transport: MockTransport })],
      });

      // act
      await module.init();

      // assert
      expect(module.get(Serializer)).toBeInstanceOf(JsonSerializer);
    });

    it.each<
      [string, () => Parameters<typeof BusModule.forRoot>[0]['serializer']]
    >([
      ['a class', () => CustomSerializer],
      ['useValue', () => ({ useValue: new CustomSerializer() })],
      ['useFactory', () => ({ useFactory: () => new CustomSerializer() })],
    ])('provides a serializer given as %s', async (_name, serializer) => {
      // arrange
      CustomSerializer.initialized = false;

      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: MockTransport,
            serializer: serializer(),
          }),
        ],
      });

      // act
      await module.init();

      // assert
      expect(module.get(Serializer)).toBeInstanceOf(CustomSerializer);
      expect(CustomSerializer.initialized).toBe(true);
    });

    it('initializes the serializer before the transport', async () => {
      // arrange
      const calls: string[] = [];

      class OrderedSerializer extends CustomSerializer {
        public override onBusInit(): void {
          calls.push('serializer');
        }
      }

      @Injectable()
      class OrderedTransport extends MockTransport {
        public onBusInit(): void {
          calls.push('transport');
        }
      }

      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: OrderedTransport,
            serializer: OrderedSerializer,
          }),
        ],
      });

      // act
      await module.init();

      // assert
      expect(calls).toEqual(['serializer', 'transport']);
    });

    it('injects the serializer into a transport', async () => {
      // arrange
      @Injectable()
      class SerializingTransport extends MockTransport {
        public constructor(
          @Inject(Serializer) public readonly serializer: Serializer,
        ) {
          super();
        }
      }

      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: SerializingTransport,
            serializer: CustomSerializer,
          }),
        ],
      });

      // act
      const transport = module.get<SerializingTransport>(Transport);

      // assert
      expect(transport.serializer).toBe(module.get(Serializer));
    });
  });

  describe('telemetry', () => {
    @Injectable()
    class CustomTelemetry implements BusTelemetry, OnBusInit {
      public static initialized = false;

      public constructor(@Inject('CONFIG') public readonly config: string) {}

      public onBusInit(): void {
        CustomTelemetry.initialized = true;
      }

      public transport(): TransportTelemetry {
        return noopTransportTelemetry;
      }
    }

    it.each<[string, () => BusProvider<BusTelemetry>, string?]>([
      [
        'a class',
        () => ({
          useClass: CustomTelemetry,
          providers: [{ provide: 'CONFIG', useValue: 'class' }],
        }),
        'class',
      ],
      ['useValue', () => ({ useValue: new CustomTelemetry('value') }), 'value'],
      [
        'useFactory',
        () => ({
          useFactory: (config: string) => new CustomTelemetry(config),
          inject: ['CONFIG'],
          providers: [{ provide: 'CONFIG', useValue: 'factory' }],
        }),
        'factory',
      ],
    ])(
      'provides a telemetry given as %s',
      async (_name, telemetry, expectedConfig) => {
        // arrange
        CustomTelemetry.initialized = false;

        const module = await compile({
          imports: [
            BusModule.forRoot({
              transport: MockTransport,
              telemetry: telemetry(),
            }),
          ],
        });

        // act
        await module.init();

        // assert
        const resolved = module.get<CustomTelemetry>(Telemetry);

        expect(resolved).toBeInstanceOf(CustomTelemetry);
        expect(resolved.config).toBe(expectedConfig);
        expect(CustomTelemetry.initialized).toBe(true);
      },
    );

    it('injects the telemetry into a transport', async () => {
      // arrange
      @Injectable()
      class ReportingTransport extends MockTransport {
        public constructor(
          @Optional()
          @Inject(Telemetry)
          public readonly telemetry?: BusTelemetry,
        ) {
          super();
        }
      }

      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: ReportingTransport,
            telemetry: { useValue: new CustomTelemetry('value') },
          }),
        ],
      });

      // act
      const transport = module.get<ReportingTransport>(Transport);

      // assert
      expect(transport.telemetry).toBe(module.get(Telemetry));
    });

    it('initializes the telemetry before the transport', async () => {
      // arrange
      const calls: string[] = [];

      class OrderedTelemetry extends CustomTelemetry {
        public override onBusInit(): void {
          calls.push('telemetry');
        }
      }

      @Injectable()
      class OrderedTransport extends MockTransport {
        public onBusInit(): void {
          calls.push('transport');
        }
      }

      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: OrderedTransport,
            telemetry: { useValue: new OrderedTelemetry('value') },
          }),
        ],
      });

      // act
      await module.init();

      // assert
      expect(calls).toEqual(['telemetry', 'transport']);
    });
  });

  describe('lifecycle', () => {
    it('bootstraps the transport once all handlers are subscribed', async () => {
      // arrange
      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: MockTransport,
            providers: [TestHandler],
          }),
        ],
      });

      // act
      await module.init();

      // assert
      expect(MockTransport.onBusBootstrap).toHaveBeenCalledOnce();
      expect(MockTransport.subscribe).calledBefore(
        MockTransport.onBusBootstrap,
      );
    });

    it('does not shut the transport down before the module closes', async () => {
      // arrange
      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: MockTransport,
          }),
        ],
      });

      // act
      await module.init();

      // assert
      expect(MockTransport.onBusShutdown).not.toHaveBeenCalled();
    });

    it('shuts the transport down when the module closes', async () => {
      // arrange
      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: MockTransport,
          }),
        ],
      });

      await module.init();

      // act
      await close(module);

      // assert
      expect(MockTransport.onBusShutdown).toHaveBeenCalledOnce();
    });

    describe('of the providers', () => {
      const calls: string[] = [];

      /** Records its hooks, and can be told to fail on shutdown. */
      abstract class Recorder
        implements OnBusInit, OnBusBootstrap, OnBusShutdown
      {
        public static failOnShutdown = false;

        public onBusInit(): void {
          calls.push(`${this.constructor.name}.init`);
        }

        public onBusBootstrap(): void {
          calls.push(`${this.constructor.name}.bootstrap`);
        }

        public onBusShutdown(): void {
          calls.push(`${this.constructor.name}.shutdown`);

          if (Recorder.failOnShutdown && this.constructor.name === 'First') {
            throw new Error('boom');
          }
        }
      }

      @Injectable()
      class First extends Recorder {}

      @Injectable()
      class Second extends Recorder {}

      @Injectable({ scope: Scope.REQUEST })
      class Scoped extends Recorder {}

      @Injectable()
      class Outside extends Recorder {}

      afterEach(() => {
        calls.length = 0;
        Recorder.failOnShutdown = false;
      });

      it('calls them on every provider of the bus module, around subscribing the handlers', async () => {
        // arrange
        MockTransport.subscribe.mockImplementationOnce(() => {
          calls.push('subscribe');

          return Promise.resolve();
        });

        const module = await compile({
          imports: [
            BusModule.forRoot({
              transport: MockTransport,
              providers: [First, Second, TestHandler],
            }),
          ],
        });

        // act
        await module.init();

        // assert
        expect(calls).toEqual([
          'First.init',
          'Second.init',
          'subscribe',
          'First.bootstrap',
          'Second.bootstrap',
        ]);
      });

      it('shuts them down in reverse, so that what is registered first is shut down last', async () => {
        // arrange
        const module = await compile({
          imports: [
            BusModule.forRoot({
              transport: MockTransport,
              providers: [First, Second, TestHandler],
            }),
          ],
        });

        await module.init();
        calls.length = 0;

        // act
        await close(module);

        // assert
        expect(calls).toEqual(['Second.shutdown', 'First.shutdown']);
      });

      it('leaves out the providers of other modules and the ones that are scoped', async () => {
        // arrange
        const module = await compile({
          imports: [
            BusModule.forRoot({
              transport: MockTransport,
              providers: [First, Scoped],
            }),
          ],
          providers: [Outside],
        });

        await module.init();

        // act
        await close(module);

        // assert
        expect(new Set(calls)).toEqual(
          new Set(['First.init', 'First.bootstrap', 'First.shutdown']),
        );
      });

      describe('when a provider fails to shut down', () => {
        async function setup() {
          Recorder.failOnShutdown = true;

          const module = await compile({
            imports: [
              BusModule.forRoot({
                transport: MockTransport,
                providers: [First, Second],
              }),
            ],
          });

          await module.init();
          calls.length = 0;

          // nest logs what a shutdown hook throws
          const logged = vi.spyOn(Logger, 'error').mockReturnValue();

          return { module, logged };
        }

        it('shuts the others down', async () => {
          // arrange
          const { module } = await setup();

          // act
          await close(module);

          // assert
          expect(calls).toEqual(['Second.shutdown', 'First.shutdown']);
          expect(MockTransport.onBusShutdown).toHaveBeenCalledOnce();
        });

        it('logs the failure', async () => {
          // arrange
          const { module, logged } = await setup();

          // act
          await close(module);

          // assert
          expect(logged).toHaveBeenCalledWith(
            expect.objectContaining({ message: 'boom' }),
            expect.any(String),
          );
        });
      });
    });

    describe('with a transport without the hooks', () => {
      async function setup() {
        class PlainTransport extends MockTransport {}

        Object.defineProperty(PlainTransport.prototype, 'onBusBootstrap', {
          value: undefined,
        });
        Object.defineProperty(PlainTransport.prototype, 'onBusShutdown', {
          value: undefined,
        });

        const module = await compile({
          imports: [
            BusModule.forRoot({
              transport: PlainTransport,
            }),
          ],
        });

        return module;
      }

      it('works when the module is initialized', async () => {
        // arrange
        const module = await setup();

        // act
        await module.init();

        // assert
        expect(MockTransport.onBusBootstrap).not.toHaveBeenCalled();
      });

      it('works when the module is closed', async () => {
        // arrange
        const module = await setup();

        await module.init();

        // act
        await close(module);

        // assert
        expect(MockTransport.onBusShutdown).not.toHaveBeenCalled();
      });
    });
  });

  it('throws if a handler is provided in both BusModule and a parent module', async () => {
    // arrange
    const module = await compile({
      imports: [
        BusModule.forRoot({
          transport: MockTransport,
          providers: [TestHandler],
        }),
      ],
      providers: [TestHandler],
    });

    // act
    const initializing = module.init();

    // assert
    await expect(initializing).rejects.toThrow(
      `Event handler ${TestHandler.name} is provided more than once`,
    );
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
      // arrange
      @EventHandler([TestEvent, OtherEvent])
      class MultiEventHandler {
        public handle = vi.fn();
      }

      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: MockTransport,
          }),
        ],
        providers: [MultiEventHandler],
      });

      // act
      await module.init();

      // assert
      expect(MockTransport.subscribe).toHaveBeenCalledExactlyOnceWith([
        {
          event: TestEvent,
          handler: MultiEventHandler.name,
          callback: expect.any(Function),
        },
        {
          event: OtherEvent,
          handler: MultiEventHandler.name,
          callback: expect.any(Function),
        },
      ]);
    });

    it('dispatches every subscribed event to the handler', async () => {
      // arrange
      @EventHandler([TestEvent, OtherEvent])
      class MultiEventHandler {
        public handle = vi.fn<(event: EventPayload<object>) => void>();
      }

      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: MockTransport,
          }),
        ],
        providers: [MultiEventHandler],
      });

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);
      const testEvent = new TestEvent({ value: 'test' });
      const otherEvent = new OtherEvent({ value2: 'other' });

      // act
      await dispatcher.publish(testEvent);
      await dispatcher.publish(otherEvent);

      // assert
      const handler = module.get(MultiEventHandler);
      expect(handler.handle).toHaveBeenCalledWith(
        expect.objectContaining({ event: testEvent }),
      );
      expect(handler.handle).toHaveBeenCalledWith(
        expect.objectContaining({ event: otherEvent }),
      );
    });

    it('applies handler-level middleware to every subscribed event', async () => {
      // arrange
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

      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: MockTransport,
          }),
        ],
        providers: [MultiEventHandler, MultiEventMiddleware],
      });

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      // act
      await dispatcher.publish(new TestEvent({ value: 'test' }));
      await dispatcher.publish(new OtherEvent({ value2: 'other' }));

      // assert
      expect(MultiEventMiddleware.use).toHaveBeenCalledTimes(2);
    });

    it('throws when given an empty array of events', () => {
      // act
      const declare = () => {
        @EventHandler([])
        class EmptyHandler {
          public handle = vi.fn();
        }

        return EmptyHandler;
      };

      // assert
      expect(declare).toThrow();
    });
  });

  describe('middlewares', () => {
    afterEach(() => {
      vi.clearAllMocks();
    });

    it('applies bus-level middleware to handler', async () => {
      // arrange
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

      const module = await compile({
        imports: [
          BusModule.forRoot({
            middlewares: [TestMiddleware],
            transport: MockTransport,
          }),
        ],
        providers: [TestHandler, TestMiddleware],
      });

      await module.init();

      const event = new TestEvent({ value: 'test' });
      const dispatcher = module.get<Dispatcher>(Dispatcher);

      // act
      await dispatcher.publish(event);

      // assert
      expect(TestMiddleware.use).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ event }),
        expect.any(Function),
      );

      const handler = module.get(TestHandler);
      expect(handler.handle).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ event }),
      );
    });

    it('executes multiple bus-level middlewares in declared order', async () => {
      // arrange
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

      const module = await compile({
        imports: [
          BusModule.forRoot({
            middlewares: [MiddlewareA, MiddlewareB],
            transport: MockTransport,
          }),
        ],
        providers: [TestHandler, MiddlewareA, MiddlewareB],
      });

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      // act
      await dispatcher.publish(new TestEvent({ value: 'test' }));

      // assert
      expect(executionOrder).toEqual([
        'A:before',
        'B:before',
        'B:after',
        'A:after',
      ]);
    });

    it('applies handler-level middleware only to the decorated handler', async () => {
      // arrange
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

      const module = await compile({
        imports: [
          BusModule.forRoot({
            transport: MockTransport,
          }),
        ],
        providers: [TestHandler, HandlerWithMiddleware, HandlerMiddleware],
      });

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      // act
      await dispatcher.publish(new TestEvent({ value: 'test' }));

      // assert
      expect(HandlerMiddleware.use).not.toHaveBeenCalled();

      await dispatcher.publish(new AnotherEvent({ value: 'test' }));
      expect(HandlerMiddleware.use).toHaveBeenCalledOnce();
    });

    it('executes bus-level middlewares before handler-level middlewares', async () => {
      // arrange
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

      const module = await compile({
        imports: [
          BusModule.forRoot({
            middlewares: [BusLevelMiddleware],
            transport: MockTransport,
          }),
        ],
        providers: [
          HandlerWithMiddleware,
          BusLevelMiddleware,
          HandlerLevelMiddleware,
        ],
      });

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      // act
      await dispatcher.publish(new TestEvent({ value: 'test' }));

      // assert
      expect(executionOrder).toEqual(['bus', 'handler', 'handle']);
    });

    it('allows middleware to modify event payload', async () => {
      // arrange
      @Injectable()
      class PayloadModifyingMiddleware implements EventHandlerMiddleware {
        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          await next({ ...event, meta: { ...event.meta, injected: 'value' } });
        }
      }

      const module = await compile({
        imports: [
          BusModule.forRoot({
            middlewares: [PayloadModifyingMiddleware],
            transport: MockTransport,
          }),
        ],
        providers: [TestHandler, PayloadModifyingMiddleware],
      });

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      // act
      await dispatcher.publish(new TestEvent({ value: 'test' }));

      // assert
      const handler = module.get(TestHandler);
      expect(handler.handle).toHaveBeenCalledWith(
        expect.objectContaining({ meta: { injected: 'value' } }),
      );
    });

    it('allows middleware to short-circuit by not calling next', async () => {
      // arrange
      @Injectable()
      class ShortCircuitMiddleware implements EventHandlerMiddleware {
        public async use() {
          // intentionally not calling next
        }
      }

      const module = await compile({
        imports: [
          BusModule.forRoot({
            middlewares: [ShortCircuitMiddleware],
            transport: MockTransport,
          }),
        ],
        providers: [TestHandler, ShortCircuitMiddleware],
      });

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);

      // act
      await dispatcher.publish(new TestEvent({ value: 'test' }));

      // assert
      const handler = module.get(TestHandler);
      expect(handler.handle).not.toHaveBeenCalled();
    });

    it('resolves middleware dependencies from the module providing it', async () => {
      // arrange
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

      const module = await compile({
        imports: [
          BusModule.forRoot({
            middlewares: [DependentMiddleware],
            transport: MockTransport,
          }),
        ],
        providers: [TestHandler, Dependency, DependentMiddleware],
      });

      await module.init();

      // act
      await module
        .get<Dispatcher>(Dispatcher)
        .publish(new TestEvent({ value: 'test' }));

      // assert
      expect(module.get(Dependency).seen).toHaveBeenCalledOnce();
    });

    describe('scopes and provider kinds', () => {
      const createModule = (
        middleware: Type<EventHandlerMiddleware>,
        providers: any[],
      ) =>
        compile({
          imports: [
            BusModule.forRoot({
              middlewares: [middleware],
              transport: MockTransport,
            }),
          ],
          providers,
        });

      it('resolves a request-scoped middleware for every event with the payload as request', async () => {
        // arrange
        const instances: object[] = [];
        const requests: unknown[] = [];

        @Injectable({ scope: Scope.REQUEST })
        class ScopedMiddleware implements EventHandlerMiddleware {
          public constructor(@Inject(REQUEST) request: unknown) {
            instances.push(this);
            requests.push(request);
          }

          public async use(
            event: EventPayload<object>,
            next: (event: EventPayload<object>) => Promise<void>,
          ) {
            await next(event);
          }
        }

        const module = await createModule(ScopedMiddleware, [
          TestHandler,
          ScopedMiddleware,
        ]);

        await module.init();

        const dispatcher = module.get<Dispatcher>(Dispatcher);
        const first = new TestEvent({ value: 'first' });
        const second = new TestEvent({ value: 'second' });

        // act
        await dispatcher.publish(first);
        await dispatcher.publish(second);

        // assert
        expect(instances).toHaveLength(2);
        expect(instances[0]).not.toBe(instances[1]);
        expect(requests).toEqual([
          expect.objectContaining({ event: first }),
          expect.objectContaining({ event: second }),
        ]);
        expect(module.get(TestHandler).handle).toHaveBeenCalledTimes(2);
      });

      it('resolves a transient middleware for every event', async () => {
        // arrange
        const instances: object[] = [];

        @Injectable({ scope: Scope.TRANSIENT })
        class TransientMiddleware implements EventHandlerMiddleware {
          public async use(
            event: EventPayload<object>,
            next: (event: EventPayload<object>) => Promise<void>,
          ) {
            instances.push(this);
            await next(event);
          }
        }

        const module = await createModule(TransientMiddleware, [
          TestHandler,
          TransientMiddleware,
        ]);

        await module.init();

        const dispatcher = module.get<Dispatcher>(Dispatcher);

        // act
        await dispatcher.publish(new TestEvent({ value: 'first' }));
        await dispatcher.publish(new TestEvent({ value: 'second' }));

        // assert
        expect(instances).toHaveLength(2);
        expect(instances[0]).not.toBe(instances[1]);
      });

      it('shares request-scoped dependencies between middleware and handler within an event', async () => {
        // arrange
        const seen: [object, object][] = [];

        @Injectable({ scope: Scope.REQUEST })
        class ScopedDependency {}

        @Injectable()
        class DependentMiddleware implements EventHandlerMiddleware {
          public constructor(
            @Inject(ScopedDependency) public readonly dep: ScopedDependency,
          ) {}

          public async use(
            event: EventPayload<object>,
            next: (event: EventPayload<object>) => Promise<void>,
          ) {
            await next(event);
          }
        }

        @EventHandler(TestEvent, { middlewares: [DependentMiddleware] })
        class DependentHandler {
          public constructor(
            @Inject(ScopedDependency) private readonly dep: ScopedDependency,
            @Inject(DependentMiddleware)
            private readonly middleware: DependentMiddleware,
          ) {}

          public handle() {
            seen.push([this.dep, this.middleware.dep]);
          }
        }

        const module = await createModule(DependentMiddleware, [
          ScopedDependency,
          DependentMiddleware,
          DependentHandler,
        ]);

        await module.init();

        const dispatcher = module.get<Dispatcher>(Dispatcher);

        // act
        await dispatcher.publish(new TestEvent({ value: 'first' }));
        await dispatcher.publish(new TestEvent({ value: 'second' }));

        // assert
        expect(seen).toHaveLength(2);
        expect(seen[0]![0]).toBe(seen[0]![1]);
        expect(seen[1]![0]).toBe(seen[1]![1]);
        expect(seen[0]![0]).not.toBe(seen[1]![0]);
      });

      it.each([
        {
          name: 'useValue',
          provide: (token: Type, instance: object) => ({
            provide: token,
            useValue: instance,
          }),
        },
        {
          name: 'useFactory',
          provide: (token: Type, instance: object) => ({
            provide: token,
            useFactory: () => instance,
          }),
        },
      ])('applies a middleware provided with $name', async ({ provide }) => {
        // arrange
        @Injectable()
        class CustomProvidedMiddleware implements EventHandlerMiddleware {
          public use = vi.fn(
            async (
              event: EventPayload<object>,
              next: (event: EventPayload<object>) => Promise<void>,
            ) => next(event),
          );
        }

        const middleware = new CustomProvidedMiddleware();
        const module = await createModule(CustomProvidedMiddleware, [
          TestHandler,
          provide(CustomProvidedMiddleware, middleware),
        ]);

        await module.init();

        // act
        await module
          .get<Dispatcher>(Dispatcher)
          .publish(new TestEvent({ value: 'test' }));

        // assert
        expect(middleware.use).toHaveBeenCalledOnce();
      });

      it('applies a middleware once when it is also provided with useExisting', async () => {
        // arrange
        @Injectable()
        class AliasedMiddleware implements EventHandlerMiddleware {
          public static use = vi.fn();

          public async use(
            event: EventPayload<object>,
            next: (event: EventPayload<object>) => Promise<void>,
          ) {
            AliasedMiddleware.use();
            await next(event);
          }
        }

        const module = await createModule(AliasedMiddleware, [
          TestHandler,
          AliasedMiddleware,
          { provide: 'MIDDLEWARE_ALIAS', useExisting: AliasedMiddleware },
        ]);

        await module.init();

        // act
        await module
          .get<Dispatcher>(Dispatcher)
          .publish(new TestEvent({ value: 'test' }));

        // assert
        expect(AliasedMiddleware.use).toHaveBeenCalledOnce();
      });
    });

    it('throws if a middleware is not provided in any module', async () => {
      // arrange
      @Injectable()
      class NotProvidedMiddleware implements EventHandlerMiddleware {
        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          await next(event);
        }
      }

      const module = await compile({
        imports: [
          BusModule.forRoot({
            middlewares: [NotProvidedMiddleware],
            transport: MockTransport,
          }),
        ],
        providers: [TestHandler],
      });

      // act
      const initializing = module.init();

      // assert
      await expect(initializing).rejects.toThrow(
        'Middleware NotProvidedMiddleware is not provided in any module',
      );
    });

    it('throws if a handler middleware is not provided in any module', async () => {
      // arrange
      @Injectable()
      class NotProvidedMiddleware implements EventHandlerMiddleware {
        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          await next(event);
        }
      }

      @EventHandler(TestEvent, { middlewares: [NotProvidedMiddleware] })
      class HandlerWithMiddleware {
        public handle = vi.fn();
      }

      const module = await compile({
        imports: [BusModule.forRoot({ transport: MockTransport })],
        providers: [HandlerWithMiddleware],
      });

      // act
      const initializing = module.init();

      // assert
      await expect(initializing).rejects.toThrow(
        'Middleware NotProvidedMiddleware is not provided in any module',
      );
    });

    it('resolves a middleware provided in the providers of the bus module', async () => {
      // arrange
      @Injectable()
      class BusProvidedMiddleware implements EventHandlerMiddleware {
        public static use = vi.fn();

        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          BusProvidedMiddleware.use();
          await next(event);
        }
      }

      const module = await compile({
        imports: [
          BusModule.forRoot({
            middlewares: [BusProvidedMiddleware],
            transport: MockTransport,
            providers: [BusProvidedMiddleware],
          }),
        ],
        providers: [TestHandler],
      });

      await module.init();

      // act
      await module
        .get<Dispatcher>(Dispatcher)
        .publish(new TestEvent({ value: 'test' }));

      // assert
      expect(BusProvidedMiddleware.use).toHaveBeenCalledOnce();
    });

    it('resolves a request-scoped handler middleware for every event with the payload as request', async () => {
      // arrange
      const instances: object[] = [];
      const requests: unknown[] = [];

      @Injectable({ scope: Scope.REQUEST })
      class ScopedMiddleware implements EventHandlerMiddleware {
        public constructor(@Inject(REQUEST) request: unknown) {
          instances.push(this);
          requests.push(request);
        }

        public async use(
          event: EventPayload<object>,
          next: (event: EventPayload<object>) => Promise<void>,
        ) {
          await next(event);
        }
      }

      @EventHandler(TestEvent, { middlewares: [ScopedMiddleware] })
      class HandlerWithMiddleware {
        public handle = vi.fn();
      }

      const module = await compile({
        imports: [BusModule.forRoot({ transport: MockTransport })],
        providers: [HandlerWithMiddleware, ScopedMiddleware],
      });

      await module.init();

      const dispatcher = module.get<Dispatcher>(Dispatcher);
      const first = new TestEvent({ value: 'first' });
      const second = new TestEvent({ value: 'second' });

      // act
      await dispatcher.publish(first);
      await dispatcher.publish(second);

      // assert
      expect(instances).toHaveLength(2);
      expect(instances[0]).not.toBe(instances[1]);
      expect(requests).toEqual([
        expect.objectContaining({ event: first }),
        expect.objectContaining({ event: second }),
      ]);
    });
  });

  describe('handler scopes', () => {
    const createModule = (providers: any[]) =>
      compile({
        imports: [
          BusModule.forRoot({
            transport: MockTransport,
          }),
        ],
        providers,
      });

    it('resolves a request-scoped handler for every event with the payload as request', async () => {
      // arrange
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

      // act
      await dispatcher.publish(first);
      await dispatcher.publish(second);

      // assert
      expect(instances).toHaveLength(2);
      expect(instances[0]).not.toBe(instances[1]);
      expect(requests).toEqual([
        expect.objectContaining({ event: first }),
        expect.objectContaining({ event: second }),
      ]);
    });

    it('resolves a transient handler for every event', async () => {
      // arrange
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

      // act
      await dispatcher.publish(new TestEvent({ value: 'first' }));
      await dispatcher.publish(new TestEvent({ value: 'second' }));

      // assert
      expect(instances).toHaveLength(2);
      expect(instances[0]).not.toBe(instances[1]);
    });

    it('resolves a handler depending on a request-scoped provider for every event', async () => {
      // arrange
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

      // act
      await dispatcher.publish(new TestEvent({ value: 'first' }));
      await dispatcher.publish(new TestEvent({ value: 'second' }));

      // assert
      expect(dependencies).toHaveLength(2);
      expect(dependencies[0]).not.toBe(dependencies[1]);
    });

    it('registers a handler once when it is also provided with useExisting', async () => {
      // arrange
      const module = await createModule([
        TestHandler,
        { provide: 'HANDLER_ALIAS', useExisting: TestHandler },
      ]);

      // act
      await module.init();

      // assert
      expect(MockTransport.subscribe).toHaveBeenCalledExactlyOnceWith([
        {
          event: TestEvent,
          handler: TestHandler.name,
          callback: expect.any(Function),
        },
      ]);
    });

    it('registers a handler provided with useValue', async () => {
      // arrange
      @EventHandler(TestEvent)
      class ValueHandler {
        public handle = vi.fn();
      }

      const handler = new ValueHandler();
      const module = await createModule([
        { provide: 'VALUE_HANDLER', useValue: handler },
      ]);

      await module.init();

      // act
      await module
        .get<Dispatcher>(Dispatcher)
        .publish(new TestEvent({ value: 'test' }));

      // assert
      expect(MockTransport.subscribe).toHaveBeenCalledExactlyOnceWith([
        {
          event: TestEvent,
          handler: ValueHandler.name,
          callback: expect.any(Function),
        },
      ]);
      expect(handler.handle).toHaveBeenCalledOnce();
    });

    it('registers a handler provided by a factory', async () => {
      // arrange
      @EventHandler(TestEvent)
      class FactoryHandler {
        public handle = vi.fn();
      }

      const handler = new FactoryHandler();
      const module = await createModule([
        { provide: 'FACTORY_HANDLER', useFactory: () => handler },
      ]);

      await module.init();

      // act
      await module
        .get<Dispatcher>(Dispatcher)
        .publish(new TestEvent({ value: 'test' }));

      // assert
      expect(MockTransport.subscribe).toHaveBeenCalledExactlyOnceWith([
        {
          event: TestEvent,
          handler: FactoryHandler.name,
          callback: expect.any(Function),
        },
      ]);
      expect(handler.handle).toHaveBeenCalledOnce();
    });
  });
});
