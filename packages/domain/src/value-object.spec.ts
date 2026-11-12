import { describe, expect, it } from 'vitest';
import { ValueObject } from './value-object.js';

class Email extends ValueObject<string, 'Email'> {}
class Username extends ValueObject<string, 'Username'> {}

describe('Value Object', () => {
  it('should not be assignable to a value object with another name', () => {
    // arrange
    const email = new Email('john@example.com');

    // act
    // @ts-expect-error -- different brand
    const username: Username = email;

    // assert
    expect(username).toBe(email);
  });
});
