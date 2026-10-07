import {
  Inject,
  Injectable,
  Type,
  type OnApplicationBootstrap,
} from '@nestjs/common';
import {
  ContextIdFactory,
  DiscoveryService,
  ModuleRef,
  Reflector,
} from '@nestjs/core';
import {
  BUS_EVENT_HANDLER_METADATA,
  type EventHandlerMetadata,
} from '../decorators/event-handler.decorator.js';
import { type InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import { type EventCallback, Subscriber } from '../transport/subscriber.js';
import { type EventHandler, type EventPayload } from '../types.js';
import {
  type EventHandlerMiddleware,
  GlobalEventHandlerMiddlewares,
} from '../middleware/middleware.js';

@Injectable()
export class HandlerDiscoveryService implements OnApplicationBootstrap {
  public constructor(
    @Inject(DiscoveryService)
    private readonly discoveryService: DiscoveryService,
    @Inject(Reflector) private readonly reflector: Reflector,
    @Inject(Subscriber) private readonly subscriber: Subscriber,
    @Inject(GlobalEventHandlerMiddlewares)
    private readonly globalEventHandlerMiddlewares: Type<EventHandlerMiddleware>[],
    @Inject(ModuleRef) private readonly moduleRef: ModuleRef,
  ) {}

  public async onApplicationBootstrap(): Promise<void> {
    const providers = this.discoveryService.getProviders();
    const globalEventHandlerMiddlewares = this.findMiddlewares(
      providers,
      this.globalEventHandlerMiddlewares,
    );
    const subscribed = new Set<unknown>();

    for (const provider of providers) {
      // Factory providers have a factory as metatype, the class is the type of
      // the instance they produce
      const metatype = (
        provider.inject ? provider.instance?.constructor : provider.metatype
      ) as Type<EventHandler<object>> | undefined;

      if (!this.isConstructor(metatype)) {
        continue;
      }

      const metadata = this.reflector.get<EventHandlerMetadata | undefined>(
        BUS_EVENT_HANDLER_METADATA,
        metatype,
      );

      if (!metadata) {
        continue;
      }

      if (subscribed.has(metatype)) {
        throw new Error(
          `Event handler ${metatype.name} is provided more than once. Provide it in a single module`,
        );
      }

      subscribed.add(metatype);

      const eventHandlerMiddlewares = this.findMiddlewares(
        providers,
        metadata.middlewares,
      );

      const callback = this.buildMiddlewareChain(
        globalEventHandlerMiddlewares,
        eventHandlerMiddlewares,
        this.createHandle(provider, metatype),
      );

      for (const event of metadata.events) {
        await this.subscriber.subscribe(event, metatype.name, callback);
      }
    }
  }

  private findMiddlewares(
    providers: InstanceWrapper[],
    types: Type<EventHandlerMiddleware>[],
  ): EventHandlerMiddleware[] {
    return types.map((type) => {
      const matches = providers.filter(
        (provider) => provider.metatype === type,
      );

      if (matches.length === 0) {
        throw new Error(
          `Middleware ${type.name} is not provided in any module. Add it to the providers of a module`,
        );
      }

      if (matches.length > 1) {
        throw new Error(
          `Middleware ${type.name} is provided more than once. Provide it in a single module`,
        );
      }

      const instance = matches[0]?.instance as
        EventHandlerMiddleware | undefined;

      if (!instance) {
        throw new Error(
          `Middleware ${type.name} must be a provider with the default scope`,
        );
      }

      return instance;
    });
  }

  private createHandle(
    provider: InstanceWrapper,
    metatype: Type<EventHandler<object>>,
  ): (event: EventPayload<object>) => Promise<void> | void {
    if (provider.isDependencyTreeStatic() && !provider.isTransient) {
      const instance = provider.instance as EventHandler<object> | undefined;

      if (!instance) {
        throw new Error(`Event handler ${metatype.name} has no instance`);
      }

      return (event) => instance.handle(event);
    }

    return async (event) => {
      const contextId = ContextIdFactory.create();

      this.moduleRef.registerRequestByContextId(event, contextId);

      const instance = await this.moduleRef.resolve<EventHandler<object>>(
        provider.token,
        contextId,
        { strict: false },
      );

      await instance.handle(event);
    };
  }

  private buildMiddlewareChain(
    globalMiddlewares: EventHandlerMiddleware[],
    localMiddlewares: EventHandlerMiddleware[],
    handle: (event: EventPayload<object>) => Promise<void> | void,
  ): EventCallback<object> {
    const allMiddlewares = [...globalMiddlewares, ...localMiddlewares];

    return allMiddlewares.reduceRight<
      (event: EventPayload<object>) => Promise<void>
    >(
      (next, middleware) => {
        return (event) => middleware.use(event, next);
      },
      (event) => Promise.resolve(handle(event)).then(() => undefined),
    );
  }

  private isConstructor(value: unknown): value is object {
    return typeof value === 'function';
  }
}
