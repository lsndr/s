import { type Type } from '@nestjs/common';
import { type EventPayload } from '../types.js';

export interface EventHandlerMiddleware {
  use(
    event: EventPayload<object>,
    next: (event: EventPayload<object>) => Promise<void>,
  ): Promise<void>;
}

export type GlobalEventHandlerMiddlewares = Type<EventHandlerMiddleware>[];

export const GlobalEventHandlerMiddlewares = Symbol(
  'GlobalEventHandlerMiddlewares',
);
