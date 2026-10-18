import { type EventPayload, type EventConstructor } from '../types.js';
import { type Delivery } from '../telemetry/telemetry.js';

export type EventCallback<E extends object> = (
  event: EventPayload<E>,
  /** What the transport knows about the delivery, for the telemetry. */
  delivery?: Delivery,
) => void | Promise<void>;

export interface Subscription<
  C extends EventConstructor<any> = EventConstructor<any>,
> {
  event: C;
  handler: string;
  callback: EventCallback<InstanceType<C>>;
}

export interface Subscriber {
  subscribe(subscriptions: Subscription[]): Promise<void>;
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
