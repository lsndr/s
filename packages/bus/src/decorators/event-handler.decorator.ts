import { Injectable, type Scope, type Type } from '@nestjs/common';
import {
  type EventPayload,
  type EventConstructor,
  type EventHandler,
} from '../types.js';
import { type EventHandlerMiddleware } from '../middleware/middleware.js';

export const BUS_EVENT_HANDLER_METADATA = Symbol('BUS_EVENT_HANDLER_METADATA');

export interface EventHandlerMetadata {
  events: EventConstructor<object>[];
  middlewares: Type<EventHandlerMiddleware>[];
}

export interface EventHandlerOptions {
  middlewares?: Type<EventHandlerMiddleware>[];
  scope?: Scope;
}

type EventPayloadOf<C> =
  C extends EventConstructor<infer E> ? EventPayload<E> : never;

export function EventHandler<E extends object>(
  event: EventConstructor<E>,
  options?: EventHandlerOptions,
): <T extends new (...args: any[]) => EventHandler<E>>(target: T) => void;
export function EventHandler<
  const C extends readonly EventConstructor<object>[],
>(
  events: C,
  options?: EventHandlerOptions,
): <
  T extends new (...args: any[]) => {
    // keep this a property, not a method: TypeScript only rejects a handler
    // that accepts too few events when the type is declared as a property
    handle: (event: EventPayloadOf<C[number]>) => Promise<void> | void;
  },
>(
  target: T,
) => void;
export function EventHandler(
  event: EventConstructor<object> | readonly EventConstructor<object>[],
  options?: EventHandlerOptions,
) {
  const events = [...new Set(Array.isArray(event) ? event : [event])];

  if (events.length === 0) {
    throw new Error('@EventHandler() requires at least one event');
  }

  return (target: new (...args: any[]) => any): void => {
    Reflect.defineMetadata(
      BUS_EVENT_HANDLER_METADATA,
      {
        events,
        middlewares: options?.middlewares ?? [],
      } satisfies EventHandlerMetadata,
      target,
    );
    Injectable({ scope: options?.scope })(target);
  };
}
