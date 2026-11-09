import { Inject, Injectable, Scope, Type } from '@nestjs/common';
import { DiscoveryService, ModuleRef, Reflector } from '@nestjs/core';
import { type InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import {
  BUS_EVENT_HANDLER_METADATA,
  type EventHandlerMetadata,
} from '../decorators/event-handler.decorator.js';
import { Subscriber } from '../transport/subscriber.js';
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
    @Inject(ModuleRef) private readonly moduleRef: ModuleRef,
    @Inject(Subscriber) private readonly subscriber: Subscriber,
    @Inject(GlobalEventHandlerMiddlewares)
    private readonly globalEventHandlerMiddlewares: Type<EventHandlerMiddleware>[],
    @Inject(HandlerInvoker) private readonly invoker: HandlerInvoker,
  ) {}

  public async subscribeHandlers(): Promise<void> {
    for (const {
      metatype,
      metadata,
      handler,
      middlewares,
    } of this.findHandlers()) {
      const callback = this.invoker.createCallback(middlewares, handler);

      for (const event of metadata.events) {
        await this.subscriber.subscribe(event, metatype.name, callback);
      }
    }
  }

  private findHandlers(): DiscoveredHandler[] {
    const providers = this.discoveryService.getProviders();
    const globalMiddlewares = this.resolveMiddlewares(
      this.globalEventHandlerMiddlewares,
    );
    const handlers: DiscoveredHandler[] = [];
    const found = new Set<unknown>();

    for (const provider of providers) {
      // an alias (`useExisting`) points at a provider that is discovered on
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
          ...this.resolveMiddlewares(metadata.middlewares),
        ],
      });
    }

    return handlers;
  }

  private resolveMiddlewares(
    types: Type<EventHandlerMiddleware>[],
  ): Resolvable<EventHandlerMiddleware>[] {
    return types.map((type) => {
      let scope: Scope;

      try {
        ({ scope } = this.moduleRef.introspect(type));
      } catch {
        throw new Error(
          `Middleware ${type.name} is not provided in any module. Add it to the providers of a module`,
        );
      }

      // request-scoped, transient, and the ones depending on them
      if (scope !== Scope.DEFAULT) {
        return { token: type, name: type.name, instance: undefined };
      }

      return {
        token: type,
        name: type.name,
        instance: this.moduleRef.get(type, { strict: false }),
      };
    });
  }

  private getClass(provider: InstanceWrapper): unknown {
    return provider.instance?.constructor ?? provider.metatype;
  }

  private toResolvable<T>(
    wrapper: InstanceWrapper,
    name: string,
  ): Resolvable<T> {
    // a transient provider with a static dependency tree still has an
    // instance, which must not be shared, so it is resolved again every time
    if (!wrapper.isDependencyTreeStatic() || wrapper.isTransient) {
      return { token: wrapper.token, name, instance: undefined };
    }

    if (!wrapper.instance) {
      throw new Error(`${name} has no instance`);
    }

    return { token: wrapper.token, name, instance: wrapper.instance as T };
  }
}
