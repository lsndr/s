import { describe, expect, it, vi } from 'vitest';
import { setTimeout } from 'node:timers/promises';
import { Handler } from './handler.js';
import { type EventPayload } from '../types.js';

const event = { id: '1' } as EventPayload<object>;

describe('Handler', () => {
  it('runs every callback with the event', async () => {
    // arrange
    const first = vi.fn();
    const second = vi.fn();
    const handler = new Handler('h', first);

    handler.merge(new Handler('h', second));

    // act
    await handler.handle(event);

    // assert
    expect(first).toHaveBeenCalledExactlyOnceWith(event);
    expect(second).toHaveBeenCalledExactlyOnceWith(event);
  });

  it('runs the other callbacks when one throws', async () => {
    // arrange
    const after = vi.fn();
    const handler = new Handler('h', () => {
      throw new Error('boom');
    });

    handler.merge(new Handler('h', after));

    // act
    const run = handler.handle(event).catch(() => undefined);
    await run;

    // assert
    expect(after).toHaveBeenCalledOnce();
  });

  it('throws the error of a single failed callback as it is', async () => {
    // arrange
    const error = new TypeError('boom');
    const handler = new Handler('h', () => Promise.reject(error));

    handler.merge(new Handler('h', vi.fn()));

    // act
    const run = handler.handle(event);

    // assert
    await expect(run).rejects.toBe(error);
  });

  it('throws an AggregateError when several callbacks fail', async () => {
    // arrange
    const first = new Error('first');
    const second = new Error('second');
    const handler = new Handler('h', () => Promise.reject(first));

    handler.merge(new Handler('h', vi.fn()));
    handler.merge(new Handler('h', () => Promise.reject(second)));

    // act
    const run = handler.handle(event);

    // assert
    await expect(run).rejects.toMatchObject({
      name: 'AggregateError',
      message: '2 of 3 handlers failed',
      errors: [first, second],
    });
  });

  it('waits for slow callbacks before it rejects', async () => {
    // arrange
    const slow = vi.fn();
    const handler = new Handler('h', () => Promise.reject(new Error('boom')));

    handler.merge(
      new Handler('h', async () => {
        await setTimeout(20);
        slow();
      }),
    );

    // act
    await handler.handle(event).catch(() => undefined);

    // assert
    expect(slow).toHaveBeenCalledOnce();
  });
});
