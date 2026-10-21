import {
  type BeforeApplicationShutdown,
  type DynamicModule,
  Inject,
  Module,
  type OnApplicationBootstrap,
  Optional,
  type Provider,
} from '@nestjs/common';
import { DiscoveryModule, DiscoveryService } from '@nestjs/core';
import { HandlerDiscoveryService } from './discovery/handler-discovery.service.js';
import { HandlerInvoker } from './discovery/handler-invoker.js';
import { Transport } from './transport/transport.js';
import { type BusProvider, toProviders } from './provider.js';
import { Serializer } from './serialization/serializer.js';
import { JsonSerializer } from './serialization/json.serializer.js';
import { Dispatcher } from './transport/dispatcher.js';
import { Subscriber } from './transport/subscriber.js';
import { Health } from './transport/health.js';
import {
  hasOnBusBootstrap,
  hasOnBusInit,
  hasOnBusShutdown,
} from './lifecycle.js';
import { GlobalEventHandlerMiddlewares } from './middleware/middleware.js';
import { type BusTelemetry, Telemetry } from './telemetry/telemetry.js';
import { instrumentTransport } from './telemetry/instrument.js';

export interface BusModuleOptions {
  middlewares?: GlobalEventHandlerMiddlewares;
  transport: BusProvider<Transport>;
  serializer?: BusProvider<Serializer>;
  /**
   * Reports spans and metrics of the bus, e.g. `new BusOtel()`. Nothing is
   * reported by default.
   */
  telemetry?: BusTelemetry;
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
    @Inject(DiscoveryService)
    private readonly discoveryService: DiscoveryService,
    @Optional()
    @Optional()
    @Inject(Telemetry)
    private readonly telemetry?: BusTelemetry,
  ) {}

  public static forRoot(options: BusModuleOptions): DynamicModule {
    const middlewares = options.middlewares ?? [];
    const { telemetry } = options;

    return {
      global: true,
      module: BusModule,
      imports: [DiscoveryModule, ...(options.imports ?? [])],
      providers: [
        {
          provide: GlobalEventHandlerMiddlewares,
          useValue: middlewares,
        },
        ...(telemetry ? [{ provide: Telemetry, useValue: telemetry }] : []),
        ...toProviders(Serializer, options.serializer ?? JsonSerializer),
        ...toProviders(Transport, options.transport),
        { provide: Dispatcher, useExisting: Transport },
        { provide: Subscriber, useExisting: Transport },
        { provide: Health, useExisting: Transport },
        HandlerInvoker,
        HandlerDiscoveryService,
        ...(options.providers ?? []),
      ],
      exports: [Dispatcher, Health],
    };
  }

  public async onApplicationBootstrap(): Promise<void> {
    const providers = this.findProviders();

    for (const provider of providers) {
      if (hasOnBusInit(provider)) {
        await provider.onBusInit();
      }
    }

    if (this.telemetry) {
      instrumentTransport(this.transport, this.telemetry);
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

  private findProviders(): object[] {
    const found = new Set<object>();

    for (const wrapper of this.discoveryService.getProviders({
      include: [BusModule],
    })) {
      const { instance } = wrapper;

      if (
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
