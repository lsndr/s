import {
  Inject,
  Injectable,
  Logger,
  Type,
  type OnApplicationBootstrap,
} from '@nestjs/common';
import { DiscoveryService, ModuleRef, Reflector } from '@nestjs/core';
import {
  BUS_EVENT_HANDLER_METADATA,
  type EventHandlerMetadata,
} from '../decorators/event-handler.decorator.js';
import { type EventCallback, Subscriber } from '../transport/subscriber.js';
import {
  type EventHandler,
  EventHandlers,
  type EventPayload,
} from '../types.js';
import {
  type EventHandlerMiddleware,
  GlobalEventHandlerMiddlewares,
} from '../middleware/middleware.js';

@Injectable()
export class HandlerDiscoveryService implements OnApplicationBootstrap {
  private readonly logger = new Logger(HandlerDiscoveryService.name);

  public constructor(
    @Inject(DiscoveryService)
    private readonly discoveryService: DiscoveryService,
    @Inject(Reflector) private readonly reflector: Reflector,
    @Inject(Subscriber) private readonly subscriber: Subscriber,
    @Inject(EventHandlers) private readonly eventHandlers: EventHandlers,
    @Inject(GlobalEventHandlerMiddlewares)
    private readonly globalEventHandlerMiddlewares: Type<EventHandlerMiddleware>[],
    @Inject(ModuleRef) private readonly moduleRef: ModuleRef,
  ) {}

  public async onApplicationBootstrap(): Promise<void> {
    const globalEventHandlerMiddlewares = await this.createMiddlewares(
      this.globalEventHandlerMiddlewares,
    );
    const providers = this.discoveryService.getProviders();

    for (const provider of providers) {
      if (!this.isConstructor(provider.metatype)) {
        continue;
      }

      const metadata = this.reflector.get<EventHandlerMetadata | undefined>(
        BUS_EVENT_HANDLER_METADATA,
        provider.metatype,
      );

      if (!metadata) {
        continue;
      }

      const metatype = provider.metatype as Type<EventHandler<object>>;

      if (!this.isRegistered(metatype)) {
        this.logger.warn(
          `Class ${provider.name} is not registered as event handler in BusModule configuration`,
        );

        continue;
      }

      const instance = provider.instance as EventHandler<object>;
      const eventHandlerMiddlewares = await this.createMiddlewares(
        metadata.middlewares,
      );

      const callback = this.buildMiddlewareChain(
        globalEventHandlerMiddlewares,
        eventHandlerMiddlewares,
        instance,
      );

      for (const event of metadata.events) {
        await this.subscriber.subscribe(event, metatype.name, callback);
      }
    }
  }

  private async createMiddlewares(
    types: Type<EventHandlerMiddleware>[],
  ): Promise<EventHandlerMiddleware[]> {
    return Promise.all(types.map((type) => this.moduleRef.create(type)));
  }

  private buildMiddlewareChain(
    globalMiddlewares: EventHandlerMiddleware[],
    localMiddlewares: EventHandlerMiddleware[],
    handler: EventHandler<object>,
  ): EventCallback<object> {
    const allMiddlewares = [...globalMiddlewares, ...localMiddlewares];

    return allMiddlewares.reduceRight<
      (event: EventPayload<object>) => Promise<void>
    >(
      (next, middleware) => {
        return (event) => middleware.use(event, next);
      },
      (event) => Promise.resolve(handler.handle(event)).then(() => undefined),
    );
  }

  private isRegistered(handler: Type<EventHandler<object>>): boolean {
    return this.eventHandlers.some(
      (registeredHandler) => registeredHandler === handler,
    );
  }

  private isConstructor(value: unknown): value is object {
    return typeof value === 'function';
  }
}
