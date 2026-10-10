import { describe, expect, it, vi } from 'vitest';
import { setTimeout } from 'node:timers/promises';
import { Signal } from './signal.js';

describe('Signal', () => {
  it('keeps a wait pending if it is not notified', async () => {
    const signal = new Signal();
    const woken = vi.fn();

    void signal.wait().then(woken);
    await setTimeout(0);

    expect(woken).toHaveBeenCalledTimes(0);
  });

  it('drops a notification that arrives while nobody waits', async () => {
    const signal = new Signal();
    const woken = vi.fn();

    signal.notify();
    void signal.wait().then(woken);
    await setTimeout(0);

    expect(woken).not.toHaveBeenCalled();
  });

  it('wakes a waiter once per wait', async () => {
    const signal = new Signal();
    const wait = vi.fn();
    void signal.wait().then(wait);

    signal.notify();
    signal.notify();

    await setTimeout(0);

    expect(wait).toHaveBeenCalledTimes(1);
  });

  it('can be waited on again after a notification', async () => {
    const signal = new Signal();

    for (let i = 0; i < 3; i++) {
      const waiting = signal.wait().then(() => i);

      signal.notify();

      await expect(waiting).resolves.toBe(i);
    }
  });
});
