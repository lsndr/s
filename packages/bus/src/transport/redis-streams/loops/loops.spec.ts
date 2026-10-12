import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { setTimeout } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { type Loop } from './loop.js';
import { Loops } from './loops.js';

// Fake timers don't cover `node:timers/promises`, so it is routed to the global
// (faked) setTimeout, keeping the abort behaviour of the real one.
vi.mock('node:timers/promises', () => ({
  setTimeout: (
    ms: number,
    value?: unknown,
    options?: { signal?: AbortSignal },
  ) =>
    new Promise((resolve, reject) => {
      const signal = options?.signal;

      if (signal?.aborted) {
        reject(new Error('aborted'));
        return;
      }

      const onAbort = () => {
        globalThis.clearTimeout(timer);
        reject(new Error('aborted'));
      };
      const timer = globalThis.setTimeout(() => {
        signal?.removeEventListener('abort', onAbort);
        resolve(value);
      }, ms);

      signal?.addEventListener('abort', onAbort, { once: true });
    }),
}));

/** Iterates right away, yielding so the event loop is not starved. */
class CountingLoop implements Loop {
  public readonly next = vi.fn(async (_signal: AbortSignal) => {
    await setTimeout(0);
  });
}

/** Waits for the abort, like a read that blocks until there is something to do. */
class BlockingLoop implements Loop {
  public readonly next = vi.fn(async (signal: AbortSignal) => {
    if (signal.aborted) {
      return;
    }

    await new Promise<void>((resolve) => {
      signal.addEventListener('abort', () => resolve(), { once: true });
    });
  });
}

class FailingLoop implements Loop {
  public readonly next = vi.fn(async (_signal: AbortSignal) => {
    await Promise.resolve();

    throw new Error('boom');
  });
}

class IntervalLoop implements Loop {
  public readonly interval = vi.fn(() => 20);
  public readonly next = vi.fn(async (_signal: AbortSignal) => {
    await Promise.resolve();
  });
}

describe('Loops', () => {
  const started: Loops[] = [];

  function create(loops: Loop[]): Loops {
    const instance = new Loops(loops);

    started.push(instance);

    return instance;
  }

  function mockLogger() {
    return vi.spyOn(Logger.prototype, 'error').mockImplementation(() => {});
  }

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(async () => {
    for (const loops of started.splice(0)) {
      loops.abort();
      await Promise.all([loops.wait(), vi.runAllTimersAsync()]);
    }

    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('resolves wait right away when it was never started', async () => {
    const loops = create([new CountingLoop()]);

    await expect(loops.wait()).resolves.toBeUndefined();
  });

  it('runs every loop again and again', async () => {
    const a = new CountingLoop();
    const b = new CountingLoop();
    const loops = create([a, b]);

    loops.start();
    await vi.advanceTimersByTimeAsync(30);

    expect(a.next.mock.calls.length).toBeGreaterThan(1);
    expect(b.next.mock.calls.length).toBeGreaterThan(1);
  });

  it('passes a signal that is aborted when the loops are aborted', async () => {
    const loop = new BlockingLoop();
    const loops = create([loop]);

    loops.start();
    await vi.advanceTimersByTimeAsync(0);

    const signal = loop.next.mock.calls[0]?.[0];

    expect(signal).toBeInstanceOf(AbortSignal);
    expect(signal?.aborted).toBe(false);

    loops.abort();

    expect(signal?.aborted).toBe(true);
  });

  it('stops iterating once aborted', async () => {
    const loop = new CountingLoop();
    const loops = create([loop]);

    loops.start();
    await vi.advanceTimersByTimeAsync(10);
    loops.abort();
    // The running iteration is not interrupted, it needs its timer to fire
    await Promise.all([loops.wait(), vi.runAllTimersAsync()]);

    const calls = loop.next.mock.calls.length;

    await vi.advanceTimersByTimeAsync(20);

    expect(loop.next).toHaveBeenCalledTimes(calls);
  });

  it('does not start the loops twice', async () => {
    const loop = new BlockingLoop();
    const loops = create([loop]);

    loops.start();
    loops.start();
    await vi.advanceTimersByTimeAsync(0);

    expect(loop.next).toHaveBeenCalledTimes(1);
  });

  describe('when aborted during an iteration', () => {
    function start() {
      const { promise, resolve } = Promise.withResolvers<void>();
      const loop: Loop = { next: vi.fn(() => promise) };
      const loops = create([loop]);

      loops.start();

      return { loop, loops, resolve };
    }

    it('does not interrupt the iteration', async () => {
      const { loops, resolve } = start();
      const ended = vi.fn();

      await vi.advanceTimersByTimeAsync(0);
      loops.abort();
      void loops.wait().then(ended);
      await vi.advanceTimersByTimeAsync(10);

      expect(ended).not.toHaveBeenCalled();

      resolve();
      await loops.wait();

      expect(ended).toHaveBeenCalledTimes(1);
    });

    it('does not start another iteration once it is done', async () => {
      const { loop, loops, resolve } = start();

      await vi.advanceTimersByTimeAsync(0);
      loops.abort();
      resolve();
      await loops.wait();

      expect(loop.next).toHaveBeenCalledTimes(1);
    });
  });

  describe('with an interval', () => {
    it.each([1, 2, 3, 4, 5])('waits before every iteration (%i)', async (i) => {
      const loop = new IntervalLoop();
      const loops = create([loop]);

      loops.start();
      await vi.advanceTimersByTimeAsync(i * 20 + 5);

      expect(loop.next).toHaveBeenCalledTimes(i);
    });

    it('ends without iterating when aborted during the wait', async () => {
      const loop = new IntervalLoop();
      const loops = create([loop]);

      loop.interval.mockReturnValue(10_000);
      loops.start();
      await vi.advanceTimersByTimeAsync(0);
      loops.abort();
      await loops.wait();

      expect(loop.next).not.toHaveBeenCalled();
    });
  });

  describe('when an iteration fails', () => {
    it('logs the failure', async () => {
      const error = mockLogger();
      const loops = create([new FailingLoop()]);

      loops.start();
      await vi.advanceTimersByTimeAsync(10);

      expect(error).toHaveBeenCalledExactlyOnceWith(
        expect.stringContaining('FailingLoop failed: Error: boom'),
        expect.stringContaining('boom'),
      );
    });

    it('waits 1 second before retrying', async () => {
      mockLogger();

      const loop = new FailingLoop();
      const loops = create([loop]);

      loops.start();
      await vi.advanceTimersByTimeAsync(500);

      expect(loop.next).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(500);

      expect(loop.next).toHaveBeenCalledTimes(2);
    });

    it('keeps the other loops running', async () => {
      mockLogger();

      const healthy = new CountingLoop();
      const loops = create([new FailingLoop(), healthy]);

      loops.start();
      await vi.advanceTimersByTimeAsync(30);

      expect(healthy.next.mock.calls.length).toBeGreaterThan(1);
    });
  });
});
