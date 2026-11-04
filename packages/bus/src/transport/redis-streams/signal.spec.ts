import { describe, expect, it, vi } from 'vitest';
import { setTimeout } from 'node:timers/promises';
import { Signal } from './signal.js';

describe('Signal', () => {
  it('keeps a wait pending if it is not notified', async () => {
    // arrange
    const signal = new Signal();
    const woken = vi.fn();

    void signal.wait().then(woken);

    // act
    await setTimeout(0);

    // assert
    expect(woken).toHaveBeenCalledTimes(0);
  });

  it('drops a notification that arrives while nobody waits', async () => {
    // arrange
    const signal = new Signal();
    const woken = vi.fn();

    signal.notify();

    // act
    void signal.wait().then(woken);
    await setTimeout(0);

    // assert
    expect(woken).not.toHaveBeenCalled();
  });

  it('wakes a waiter once per wait', async () => {
    // arrange
    const signal = new Signal();
    const wait = vi.fn();

    void signal.wait().then(wait);

    // act
    signal.notify();
    signal.notify();
    await setTimeout(0);

    // assert
    expect(wait).toHaveBeenCalledTimes(1);
  });

  it('wakes every waiter', async () => {
    // arrange
    const signal = new Signal();
    const first = vi.fn();
    const second = vi.fn();

    void signal.wait().then(first);
    void signal.wait().then(second);

    // act
    signal.notify();
    await setTimeout(0);

    // assert
    expect(first).toHaveBeenCalledTimes(1);
    expect(second).toHaveBeenCalledTimes(1);
  });

  it('can be waited on again after a notification', async () => {
    // arrange
    const signal = new Signal();
    const first = signal.wait();

    signal.notify();
    await first;

    // act
    const second = signal.wait();

    signal.notify();

    // assert
    await expect(second).resolves.toBeUndefined();
  });
});
