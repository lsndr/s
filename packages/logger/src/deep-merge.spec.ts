import { describe, expect, it } from 'vitest';
import { deepMerge } from './deep-merge.js';

describe('deepMerge', () => {
  it('merges nested objects key by key', () => {
    // arrange
    const a = { user: { id: '1' } };
    const b = { user: { role: 'admin' } };

    // act
    const result = deepMerge(a, b);

    // assert
    expect(result).toEqual({ user: { id: '1', role: 'admin' } });
  });

  it('lets later sources win', () => {
    // arrange
    const a = { requestId: 'a', nested: { x: 1 } };
    const b = { requestId: 'b', nested: { x: 2 } };

    // act
    const result = deepMerge(a, b);

    // assert
    expect(result).toEqual({ requestId: 'b', nested: { x: 2 } });
  });

  it('skips undefined values', () => {
    // arrange
    const a = { requestId: 'a' };
    const b = { requestId: undefined };

    // act
    const result = deepMerge(a, b);

    // assert
    expect(result).toEqual({ requestId: 'a' });
  });

  it('lets null override an earlier value', () => {
    // arrange
    const a = { user: { id: '1' } };
    const b = { user: null };

    // act
    const result = deepMerge(a, b);

    // assert
    expect(result).toEqual({ user: null });
  });

  it('lets a primitive replace an object', () => {
    // arrange
    const a = { user: { id: '1' } };
    const b = { user: 'anonymous' };

    // act
    const result = deepMerge(a, b);

    // assert
    expect(result).toEqual({ user: 'anonymous' });
  });

  it('lets an object replace a primitive', () => {
    // arrange
    const a = { user: 'anonymous' };
    const b = { user: { id: '1' } };

    // act
    const result = deepMerge(a, b);

    // assert
    expect(result).toEqual({ user: { id: '1' } });
  });

  it('ignores sources that are not objects', () => {
    // arrange
    const a = { requestId: 'a' };

    // act
    const result = deepMerge(undefined, a, null, 42, 'text');

    // assert
    expect(result).toEqual({ requestId: 'a' });
  });

  it('replaces arrays instead of merging them by index', () => {
    // arrange
    const a = { tags: ['a', 'b'] };
    const b = { tags: ['c'] };

    // act
    const result = deepMerge(a, b);

    // assert
    expect(result).toEqual({ tags: ['c'] });
  });

  it('does not share nested objects or arrays with its sources', () => {
    // arrange
    const a = { user: { id: '1' }, tags: [{ name: 'a' }] };

    // act
    const result = deepMerge(a) as typeof a;

    // assert
    expect(result.user).not.toBe(a.user);
    expect(result.tags).not.toBe(a.tags);
    expect(result.tags[0]).not.toBe(a.tags[0]);
  });

  it('does not modify its sources', () => {
    // arrange
    const a = { user: { id: '1' } };
    const b = { user: { role: 'admin' } };

    // act
    deepMerge(a, b);

    // assert
    expect(a).toEqual({ user: { id: '1' } });
    expect(b).toEqual({ user: { role: 'admin' } });
  });

  it('keeps Error instances by reference', () => {
    // arrange
    const error = new Error('boom');

    // act
    const result = deepMerge({ cause: error });

    // assert
    expect(result['cause']).toBe(error);
  });

  it('keeps Date instances by reference', () => {
    // arrange
    const date = new Date();

    // act
    const result = deepMerge({ at: date });

    // assert
    expect(result['at']).toBe(date);
  });

  it('merges objects without a prototype', () => {
    // arrange
    const a = Object.assign(Object.create(null) as object, { x: 1 });
    const b = { y: 2 };

    // act
    const result = deepMerge(a, b);

    // assert
    expect(result).toEqual({ x: 1, y: 2 });
  });

  it('does not pollute prototypes through a __proto__ key', () => {
    // arrange
    const malicious: unknown = JSON.parse('{"__proto__":{"polluted":true}}');

    // act
    const result = deepMerge({}, malicious);

    // assert
    expect(Object.getPrototypeOf(result)).toBe(Object.prototype);
    expect(result['polluted']).toBeUndefined();
  });
});
