import { Inject, Injectable, Type } from '@nestjs/common';
import { DiscoveryService, Reflector } from '@nestjs/core';
import { type InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import {
  BUS_EVENT_HANDLER_METADATA,
  type EventHandlerMetadata,
} from '../decorators/event-handler.decorator.js';
import { Subscriber, type Subscription } from '../transport/subscriber.js';
import { type EventHandler } from '../types.js';
import {
  type EventHandlerMiddleware,
  GlobalEventHandlerMiddlewares,
} from '../middleware/middleware.js';
import { HandlerInvoker, type Resolvable } from './handler-invoker.js';

interface DiscoveredHandler {
  metatype: Type<EventHandler<object>>;
  metadata: EventHandlerMetadata;
  handler: Resolvable<EventHandler<object>>;
  middlewares: Resolvable<EventHandlerMiddleware>[];
}

@Injectable()
export class HandlerDiscoveryService {
  public constructor(
    @Inject(DiscoveryService)
    private readonly discoveryService: DiscoveryService,
    @Inject(Reflector) private readonly reflector: Reflector,
    @Inject(Subscriber) private readonly subscriber: Subscriber,
    @Inject(GlobalEventHandlerMiddlewares)
    private readonly globalEventHandlerMiddlewares: Type<EventHandlerMiddleware>[],
    @Inject(HandlerInvoker) private readonly invoker: HandlerInvoker,
  ) {}

  public async subscribeHandlers(): Promise<void> {
    const subscriptions: Subscription[] = [];

    for (const {
      metatype,
      metadata,
      handler,
      middlewares,
    } of this.findHandlers()) {
      const callback = this.invoker.createCallback(middlewares, handler);

      for (const event of metadata.events) {
        subscriptions.push({ event, handler: metatype.name, callback });
      }
    }

    await this.subscriber.subscribe(subscriptions);
  }

  private findHandlers(): DiscoveredHandler[] {
    const providers = this.discoveryService.getProviders();
    const globalMiddlewares = this.findMiddlewares(
      providers,
      this.globalEventHandlerMiddlewares,
    );
    const handlers: DiscoveredHandler[] = [];
    const found = new Set<unknown>();

    for (const provider of providers) {
      // An alias (`useExisting`) points at a provider that is discovered on
      // its own
      if (provider.isAlias) {
        continue;
      }

      const metatype = this.getClass(provider) as
        Type<EventHandler<object>> | undefined;

      if (typeof metatype !== 'function') {
        continue;
      }

      const metadata = this.reflector.get<EventHandlerMetadata | undefined>(
        BUS_EVENT_HANDLER_METADATA,
        metatype,
      );

      if (!metadata) {
        continue;
      }

      if (found.has(metatype)) {
        throw new Error(
          `Event handler ${metatype.name} is provided more than once. Provide it in a single module`,
        );
      }

      found.add(metatype);

      handlers.push({
        metatype,
        metadata,
        handler: this.toResolvable<EventHandler<object>>(
          provider,
          metatype.name,
        ),
        middlewares: [
          ...globalMiddlewares,
          ...this.findMiddlewares(providers, metadata.middlewares),
        ],
      });
    }

    return handlers;
  }

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
}
