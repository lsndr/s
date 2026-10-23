import { describe, expect, it } from 'vitest';
import { DomainError } from './domain-error.js';
import { DomainObject } from './domain-object.js';

interface TestState {
  name?: string;
}

class TestObject extends DomainObject<TestState> {
  public static named(name?: string): string {
    const object = new TestObject({ name });

    TestObject.assert(object.state.name !== undefined, 'name-must-be-set');

    // compiles only if the assertion narrows `name` to a string
    return object.state.name.toUpperCase();
  }

  public shout(): string {
    this.assert(this.state.name !== undefined, 'name-must-be-set');

    // compiles only if the assertion narrows `name` to a string
    return this.state.name.toUpperCase();
  }
}

describe('DomainObject', () => {
  it('lets execution continue when the expression holds', () => {
    expect(new TestObject({ name: 'a' }).shout()).toBe('A');
    expect(TestObject.named('a')).toBe('A');
  });

  it('throws a DomainError naming the owner and the key when it does not', () => {
    expect(() => new TestObject({}).shout()).toThrow(
      new DomainError('TestObject', 'name-must-be-set'),
    );
    expect(() => TestObject.named()).toThrow(
      new DomainError('TestObject', 'name-must-be-set'),
    );
  });
});
