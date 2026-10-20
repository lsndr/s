import { Inject, Injectable, type InjectionToken } from '@nestjs/common';
import { type ContextId, ContextIdFactory, ModuleRef } from '@nestjs/core';
import { type EventCallback } from '../transport/subscriber.js';
import { type EventHandler, type EventPayload } from '../types.js';
import { type EventHandlerMiddleware } from '../middleware/middleware.js';

export interface Resolvable<T> {
  token: InjectionToken;
  name: string;
  /** Set when the provider has the default scope. */
  instance: T | undefined;
}

@Injectable()
export class HandlerInvoker {
  public constructor(
    @Inject(ModuleRef) private readonly moduleRef: ModuleRef,
  ) {}

  public createCallback(
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
      (await this.moduleRef.resolve<T>(resolvable.token, contextId, {
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
}
