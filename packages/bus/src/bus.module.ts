import {
  type BeforeApplicationShutdown,
  type DynamicModule,
  Inject,
  Module,
  type OnApplicationBootstrap,
  type Provider,
} from '@nestjs/common';
import { DiscoveryModule, DiscoveryService } from '@nestjs/core';
import { HandlerDiscoveryService } from './discovery/handler-discovery.service.js';
import { HandlerInvoker } from './discovery/handler-invoker.js';
import {
  type BusTransport,
  Transport,
  TransportConfig,
} from './transport/transport.js';
import {
  Outbox,
  type OutboxConfig,
  OutboxConfigToken,
  OutboxTransport,
} from './transport/outbox/outbox.js';
import { Dispatcher } from './transport/dispatcher.js';
import { Subscriber } from './transport/subscriber.js';
import { Health } from './transport/health.js';
import {
  hasOnBusBootstrap,
  hasOnBusInit,
  hasOnBusShutdown,
} from './lifecycle.js';
import { GlobalEventHandlerMiddlewares } from './middleware/middleware.js';

export { type BusTransport };

export interface BusModuleOptions<T extends Transport> {
  middlewares?: GlobalEventHandlerMiddlewares;
  transport: BusTransport<T>;
  imports?: any[];
  providers?: Provider[];
}

@Module({})
export class BusModule
  implements OnApplicationBootstrap, BeforeApplicationShutdown
{
  public constructor(
    @Inject(HandlerDiscoveryService)
    private readonly discovery: HandlerDiscoveryService,
    @Inject(DiscoveryService)
    private readonly discoveryService: DiscoveryService,
  ) {}

  public static forRoot<T extends Transport>(
    options: BusModuleOptions<T>,
  ): DynamicModule {
    const middlewares = options.middlewares ?? [];
    return {
      global: true,
      module: BusModule,
      imports: [DiscoveryModule, ...(options.imports ?? [])],
      providers: [
        {
          provide: GlobalEventHandlerMiddlewares,
          useValue: middlewares,
        },
        ...this.transportProviders(options.transport),
        options.transport.provider,
        {
          provide: Dispatcher,
          useExisting: options.transport.provider,
        },
        {
          provide: Subscriber,
          useExisting: options.transport.provider,
        },
        {
          provide: Health,
          useExisting: options.transport.provider,
        },
        {
          provide: Transport,
          useExisting: options.transport.provider,
        },
        HandlerInvoker,
        HandlerDiscoveryService,
        ...(options.providers ?? []),
      ],
      exports: [Dispatcher, Health],
    };
  }

  /**
   * Provides the config of the transport, and, for an outbox, the transport it
   * relays to. Both read their config from `TransportConfig`, so the outbox
   * reads its own from `OutboxConfigToken` instead.
   */
  private static transportProviders(
    transport: BusTransport<Transport>,
  ): Provider[] {
    if (!this.isOutbox(transport)) {
      return [{ provide: TransportConfig, useValue: transport.config }];
    }

    const inner = transport.config.transport;

    return [
      { provide: OutboxConfigToken, useValue: transport.config },
      { provide: TransportConfig, useValue: inner.config },
      inner.provider,
      { provide: OutboxTransport, useExisting: inner.provider },
    ];
  }

  private static isOutbox(
    transport: BusTransport<Transport>,
  ): transport is BusTransport<Outbox<OutboxConfig>> {
    return transport.provider.prototype instanceof Outbox;
  }

  public async onApplicationBootstrap(): Promise<void> {
    const providers = this.findProviders();

    for (const provider of providers) {
      if (hasOnBusInit(provider)) {
        await provider.onBusInit();
      }
    }

    await this.discovery.subscribeHandlers();

    for (const provider of providers) {
      if (hasOnBusBootstrap(provider)) {
        await provider.onBusBootstrap();
      }
    }
  }

  public async beforeApplicationShutdown(): Promise<void> {
    const errors: unknown[] = [];

    // Reverse order, so that what was registered first is shut down last
    for (const provider of this.findProviders().reverse()) {
      if (!hasOnBusShutdown(provider)) {
        continue;
      }

      // One failing does not keep the others from shutting down
      try {
        await provider.onBusShutdown();
      } catch (error) {
        errors.push(error);
      }
    }

    if (errors.length > 0) {
      throw errors.length === 1
        ? errors[0]
        : new AggregateError(errors, 'Bus shutdown hooks failed');
    }
  }

  /**
   * The providers of the bus module, in the order they are registered. The
   * ones with a request or transient scope have no single instance, and are
   * left out, like the aliases (`useExisting`), which stand for another one.
   */
  private findProviders(): object[] {
    const found = new Set<object>();

    for (const wrapper of this.discoveryService.getProviders({
      include: [BusModule],
    })) {
      const { instance } = wrapper;

      if (
        wrapper.isAlias ||
        wrapper.isTransient ||
        !wrapper.isDependencyTreeStatic() ||
        typeof instance !== 'object' ||
        instance === null ||
        instance === this
      ) {
        continue;
      }

      found.add(instance);
    }

    return [...found];
  }
}
