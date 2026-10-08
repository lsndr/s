import { type PublishOptions, type Dispatcher } from './dispatcher.js';
import { type EventConstructor } from '../types.js';
import { type EventCallback, type Subscriber } from './subscriber.js';
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
  protected config!: TransportConfig;

  public abstract publish<E extends object>(
    event: E,
    options?: TransportPublishOptions,
  ): Promise<void>;

  public abstract subscribe<
    C extends EventConstructor<any>,
    E extends InstanceType<C> = InstanceType<C>,
  >(event: C, handler: string, callback: EventCallback<E>): Promise<void>;

  public abstract assertHealthy(): Promise<void>;
}

export type InferTransportConfig<T> =
  T extends Transport<infer C> ? C : unknown;

export interface BusTransport<T extends Transport> {
  provider: Type<T>;
  config: InferTransportConfig<T>;
}
