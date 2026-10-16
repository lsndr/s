import {
  type PublishEvent,
  type PublishOptions,
  type Dispatcher,
} from './dispatcher.js';
import { type EventConstructor } from '../types.js';
import {
  type EventCallback,
  type Subscriber,
  type Subscription,
} from './subscriber.js';
import { type Health } from './health.js';
import { Inject, type Type } from '@nestjs/common';

export const TransportConfig = Symbol('TransportConfig');

export interface TransportPublishOptions extends PublishOptions {
  /** Event name. Defaults to the class name of the event. */
  name?: string;
}

export abstract class Transport<TransportConfig = unknown>
  implements Dispatcher, Subscriber, Health
{
  @Inject(TransportConfig)
  public config!: TransportConfig;

  public abstract assertHealthy(): Promise<void>;

  public abstract publish(
    events: PublishEvent<TransportPublishOptions>[],
  ): Promise<void>;
  public abstract publish(
    event: object,
    options?: TransportPublishOptions,
  ): Promise<void>;

  public abstract subscribe(subscriptions: Subscription[]): Promise<void>;
  public abstract subscribe<
    C extends EventConstructor<any>,
    E extends InstanceType<C> = InstanceType<C>,
  >(event: C, handler: string, callback: EventCallback<E>): Promise<void>;
}

export type InferTransportConfig<T> =
  T extends Transport<infer C> ? C : unknown;

export interface BusTransport<T extends Transport> {
  provider: Type<T>;
  config: InferTransportConfig<T>;
}
