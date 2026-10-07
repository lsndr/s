import {
  Inject,
  Injectable,
  Type,
  type OnApplicationBootstrap,
} from '@nestjs/common';
import {
  type ContextId,
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

/**
 * A provider of the application. Default-scoped ones have a single instance,
 * the others are resolved anew for every event.
 */
interface Resolvable<T> {
  wrapper: InstanceWrapper;
  name: string;
  /** Set when the provider has the default scope. */
  instance: T | undefined;
}

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
      // An alias (`useExisting`) points at a provider that is discovered on
      // its own
      if (provider.isAlias) {
        continue;
      }

      const metatype = this.getClass(provider) as
        Type<EventHandler<object>> | undefined;

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

      const callback = this.createCallback(
        [...globalEventHandlerMiddlewares, ...eventHandlerMiddlewares],
        this.toResolvable<EventHandler<object>>(provider, metatype.name),
      );

      for (const event of metadata.events) {
        await this.subscriber.subscribe(event, metatype.name, callback);
      }
    }
  }

  /**
   * Looks up middleware providers of the application, so that the application
   * decides in which module a middleware lives and how its dependencies are
   * resolved.
   */
  private findMiddlewares(
    providers: InstanceWrapper[],
    types: Type<EventHandlerMiddleware>[],
  ): Resolvable<EventHandlerMiddleware>[] {
    return types.map((type) => {
      const matches = providers.filter(
        (provider) => !provider.isAlias && this.getClass(provider) === type,
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

      return this.toResolvable<EventHandlerMiddleware>(matches[0]!, type.name);
    });
  }

  /**
   * The class of a provider is the class of its instance, so that providers
   * created with `useValue` or `useFactory` are found too. A factory provider
   * has the factory as its metatype.
   */
  private getClass(provider: InstanceWrapper): unknown {
    return provider.instance?.constructor ?? provider.metatype;
  }

  private toResolvable<T>(
    wrapper: InstanceWrapper,
    name: string,
  ): Resolvable<T> {
    // Transient providers have a static dependency tree, but still must not
    // be shared
    if (!wrapper.isDependencyTreeStatic() || wrapper.isTransient) {
      return { wrapper, name, instance: undefined };
    }

    if (!wrapper.instance) {
      throw new Error(`${name} has no instance`);
    }

    return { wrapper, name, instance: wrapper.instance as T };
  }

  /**
   * Builds the callback of a handler. When everything in the chain is a
   * singleton the chain is built once. Otherwise, the scoped providers are
   * resolved for every event, all in the same context so that they share
   * request-scoped dependencies, and the event payload is available as
   * `REQUEST`.
   */
  private createCallback(
    middlewares: Resolvable<EventHandlerMiddleware>[],
    handler: Resolvable<EventHandler<object>>,
  ): EventCallback<object> {
    if (handler.instance && middlewares.every((m) => m.instance)) {
      const instance = handler.instance;

      return this.buildMiddlewareChain(
        middlewares.map((m) => m.instance!),
        (event) => instance.handle(event),
      );
    }

    return async (event) => {
      const contextId = ContextIdFactory.create();

      this.moduleRef.registerRequestByContextId(event, contextId);

      const resolvedMiddlewares: EventHandlerMiddleware[] = [];

      for (const middleware of middlewares) {
        resolvedMiddlewares.push(await this.resolve(middleware, contextId));
      }

      const resolvedHandler = await this.resolve(handler, contextId);

      await this.buildMiddlewareChain(resolvedMiddlewares, (e) =>
        resolvedHandler.handle(e),
      )(event);
    };
  }

  private async resolve<T>(
    resolvable: Resolvable<T>,
    contextId: ContextId,
  ): Promise<T> {
    return (
      resolvable.instance ??
      (await this.moduleRef.resolve<T>(resolvable.wrapper.token, contextId, {
        strict: false,
      }))
    );
  }

  private buildMiddlewareChain(
    allMiddlewares: EventHandlerMiddleware[],
    handle: (event: EventPayload<object>) => Promise<void> | void,
  ): EventCallback<object> {
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
