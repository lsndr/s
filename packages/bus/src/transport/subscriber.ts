import { type EventPayload, type EventConstructor } from '../types.js';

export type EventCallback<E extends object> = (
  event: EventPayload<E>,
) => void | Promise<void>;

export interface Subscription<
  C extends EventConstructor<any> = EventConstructor<any>,
> {
  event: C;
  handler: string;
  callback: EventCallback<InstanceType<C>>;
}

export interface Subscriber {
  subscribe<
    C extends EventConstructor<any>,
    E extends InstanceType<C> = InstanceType<C>,
  >(
    event: C,
    handler: string,
    callback: EventCallback<E>,
  ): Promise<void>;
}

export const Subscriber = Symbol('Subscriber');
