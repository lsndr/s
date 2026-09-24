import { DomainObject } from './domain-object.js';

export abstract class ValueObject<S, T> extends DomainObject<S> {
  // @ts-expect-error -- nominal typing hack
  private nominalTypeName: T;
}
