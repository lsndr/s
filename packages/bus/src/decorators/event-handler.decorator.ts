import { Injectable, type Type } from '@nestjs/common';
import { type EventConstructor, type EventHandler } from '../types.js';
import { type EventHandlerMiddleware } from '../middleware/middleware.js';

export const BUS_EVENT_HANDLER_METADATA = Symbol('BUS_EVENT_HANDLER_METADATA');

export interface EventHandlerMetadata {
  event: EventConstructor<object>;
  middlewares: Type<EventHandlerMiddleware>[];
}

export interface EventHandlerOptions {
  middlewares?: Type<EventHandlerMiddleware>[];
}

export function EventHandler<E extends object>(
  event: EventConstructor<E>,
  options?: EventHandlerOptions,
) {
  return <T extends new (...args: any[]) => EventHandler<E>>(
    target: T,
  ): void => {
    Reflect.defineMetadata(
      BUS_EVENT_HANDLER_METADATA,
      {
        event,
        middlewares: options?.middlewares ?? [],
      } satisfies EventHandlerMetadata,
      target,
    );
    Injectable()(target as unknown as new (...args: any[]) => any);
  };
}
