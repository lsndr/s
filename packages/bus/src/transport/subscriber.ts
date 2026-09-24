import { type EventPayload, type EventConstructor } from '../types.js';

export type EventCallback<E extends object> = (
  event: EventPayload<E>,
) => void | Promise<void>;

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
