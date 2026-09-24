import { AggregateEvents } from './aggregate-events.js';
import { DomainObject } from './domain-object.js';

export abstract class AggregateRoot<S> extends DomainObject<S> {
  public readonly events = new AggregateEvents();
}
