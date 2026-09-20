import { type PublishOptions, type Dispatcher } from './dispatcher.js';
import { type EventConstructor } from '../types.js';
import { type EventCallback, type Subscriber } from './subscriber.js';
import { type Health } from './health.js';
import { Inject } from '@nestjs/common';

export const TransportConfig = Symbol('TransportConfig');

export abstract class Transport<TransportConfig = unknown>
  implements Dispatcher, Subscriber, Health
{
  @Inject(TransportConfig)
  protected config!: TransportConfig;

  public abstract publish<E extends object>(
    event: E,
    options?: PublishOptions,
  ): Promise<void>;

  public abstract subscribe<
    C extends EventConstructor<any>,
    E extends InstanceType<C> = InstanceType<C>,
  >(event: C, handler: string, callback: EventCallback<E>): Promise<void>;

  public abstract assertHealthy(): Promise<void>;
}
