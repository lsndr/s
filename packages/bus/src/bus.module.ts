import {
  type BeforeApplicationShutdown,
  type DynamicModule,
  Inject,
  Module,
  type OnApplicationBootstrap,
  type Provider,
} from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
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
import { hasOnBusBootstrap, hasOnBusShutdown } from './lifecycle.js';
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
    @Inject(Transport) private readonly transport: Transport,
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
    await this.discovery.subscribeHandlers();

    if (hasOnBusBootstrap(this.transport)) {
      await this.transport.onBusBootstrap();
    }
  }

  public async beforeApplicationShutdown(): Promise<void> {
    if (hasOnBusShutdown(this.transport)) {
      await this.transport.onBusShutdown();
    }
  }
}
