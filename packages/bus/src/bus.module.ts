import {
  type DynamicModule,
  Module,
  type Provider,
  type Type,
} from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { HandlerDiscoveryService } from './discovery/handler-discovery.service.js';
import { HandlerInvoker } from './discovery/handler-invoker.js';
import { Transport, TransportConfig } from './transport/transport.js';
import { Dispatcher } from './transport/dispatcher.js';
import { BusConfig, BusRetriesConfig } from './types.js';
import { Subscriber } from './transport/subscriber.js';
import { Health } from './transport/health.js';
import { GlobalEventHandlerMiddlewares } from './middleware/middleware.js';

type InferTransportConfig<T> = T extends Transport<infer C> ? C : unknown;

export interface BusTransport<T extends Transport> {
  provider: Type<T>;
  config: InferTransportConfig<T>;
}

export interface BusModuleOptions<T extends Transport> {
  group: string;
  middlewares?: GlobalEventHandlerMiddlewares;
  transport: BusTransport<T>;
  imports?: any[];
  providers?: Provider[];
  retries?: Partial<BusRetriesConfig>;
}

@Module({})
export class BusModule {
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
        {
          provide: TransportConfig,
          useValue: options.transport.config,
        },
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
          provide: BusConfig,
          useValue: {
            retries: {
              max: options.retries?.max ?? 5,
              delay: options.retries?.delay ?? 5000,
            },
            group: options.group,
          } satisfies BusConfig,
        },
        HandlerInvoker,
        HandlerDiscoveryService,
        ...(options.providers ?? []),
      ],
      exports: [Dispatcher, Health],
    };
  }
}
