import { DomainObject } from './domain-object.js';

export abstract class ValueObject<S, T> extends DomainObject<S> {
  // typed and non-private so the brand survives .d.ts emit, which drops the type of private members
  declare protected readonly nominalTypeName: T;
}
