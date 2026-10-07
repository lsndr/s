import {
  Inject,
  Injectable,
  Type,
  type OnApplicationBootstrap,
} from '@nestjs/common';
import { DiscoveryService, Reflector } from '@nestjs/core';
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
  ) {}

  public async onApplicationBootstrap(): Promise<void> {
    const providers = this.discoveryService.getProviders();
    const globalEventHandlerMiddlewares = this.findMiddlewares(
      providers,
      this.globalEventHandlerMiddlewares,
    );
    const subscribed = new Set<unknown>();

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

      if (subscribed.has(metatype)) {
        throw new Error(
          `Event handler ${provider.name} is provided more than once. Provide it in a single module`,
        );
      }

      subscribed.add(metatype);

      const instance = provider.instance as EventHandler<object>;
      const eventHandlerMiddlewares = this.findMiddlewares(
        providers,
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

  /**
   * Looks up middleware instances among the providers of the application, so
   * that the application decides in which module a middleware lives and how
   * its dependencies are resolved.
   */
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

  private isConstructor(value: unknown): value is object {
    return typeof value === 'function';
  }
}
