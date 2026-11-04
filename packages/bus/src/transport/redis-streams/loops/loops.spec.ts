import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { setTimeout } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { type Loop } from './loop.js';
import { Loops } from './loops.js';

// fake timers don't cover `node:timers/promises`, so it is routed to the global
// (faked) setTimeout, keeping the abort behaviour of the real one
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

class CountingLoop implements Loop {
  public readonly next = vi.fn(async (_signal: AbortSignal) => {
    await setTimeout(0);
  });
}

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

class ShuttingDownLoop extends CountingLoop {
  public onTransportShutdown = vi.fn<() => void | Promise<void>>();
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
  // resolvers of promises that a test leaves pending
  const holds: (() => void)[] = [];

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
    holds.splice(0).forEach((resolve) => resolve());

    for (const loops of started.splice(0)) {
      await Promise.all([loops.onTransportShutdown(), vi.runAllTimersAsync()]);
    }

    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('resolves shutdown right away when it was never started', async () => {
    // arrange
    const loops = create([new CountingLoop()]);

    // act
    const shutdown = loops.onTransportShutdown();

    // assert
    await expect(shutdown).resolves.toBeUndefined();
  });

  it('runs every loop again and again', async () => {
    // arrange
    const a = new CountingLoop();
    const b = new CountingLoop();
    const loops = create([a, b]);

    // act
    loops.start();
    await vi.advanceTimersByTimeAsync(30);

    // assert
    expect(a.next.mock.calls.length).toBeGreaterThan(1);
    expect(b.next.mock.calls.length).toBeGreaterThan(1);
  });

  it('passes a signal that is not aborted while the loops run', async () => {
    // arrange
    const loop = new BlockingLoop();
    const loops = create([loop]);

    // act
    loops.start();
    await vi.advanceTimersByTimeAsync(0);

    // assert
    expect(loop.next.mock.calls[0]?.[0].aborted).toBe(false);
  });

  it('passes a signal that is aborted when the loops are aborted', async () => {
    // arrange
    const loop = new BlockingLoop();
    const loops = create([loop]);

    loops.start();
    await vi.advanceTimersByTimeAsync(0);

    // act
    await loops.onTransportShutdown();

    // assert
    expect(loop.next.mock.calls[0]?.[0].aborted).toBe(true);
  });

  it('stops iterating once aborted', async () => {
    // arrange
    const loop = new CountingLoop();
    const loops = create([loop]);

    loops.start();
    await vi.advanceTimersByTimeAsync(0);
    // the running iteration is not interrupted, it needs its timer to fire
    await Promise.all([loops.onTransportShutdown(), vi.runAllTimersAsync()]);

    const calls = loop.next.mock.calls.length;

    // act
    await vi.advanceTimersByTimeAsync(0);

    // assert
    expect(loop.next).toHaveBeenCalledTimes(calls);
  });

  it('does not start the loops twice', async () => {
    // arrange
    const loop = new BlockingLoop();
    const loops = create([loop]);

    // act
    loops.start();
    loops.start();
    await vi.advanceTimersByTimeAsync(0);

    // assert
    expect(loop.next).toHaveBeenCalledTimes(1);
  });

  describe('when aborted during an iteration', () => {
    function start() {
      const { promise, resolve } = Promise.withResolvers<void>();
      const loop: Loop = { next: vi.fn(() => promise) };
      const loops = create([loop]);

      holds.push(resolve);
      loops.start();

      return { loop, loops, resolve };
    }

    it('does not interrupt the iteration', async () => {
      // arrange
      const { loops } = start();
      const ended = vi.fn();

      await vi.advanceTimersByTimeAsync(0);

      // act
      void loops.onTransportShutdown().then(ended);
      await vi.advanceTimersByTimeAsync(0);

      // assert
      expect(ended).not.toHaveBeenCalled();
    });

    it('ends once the iteration is done', async () => {
      // arrange
      const { loops, resolve } = start();
      const ended = vi.fn();

      await vi.advanceTimersByTimeAsync(0);
      void loops.onTransportShutdown().then(ended);
      await vi.advanceTimersByTimeAsync(0);

      // act
      resolve();
      await vi.advanceTimersByTimeAsync(0);

      // assert
      expect(ended).toHaveBeenCalledTimes(1);
    });

    it('does not start another iteration once it is done', async () => {
      // arrange
      const { loop, loops, resolve } = start();

      await vi.advanceTimersByTimeAsync(0);
      const shutdown = loops.onTransportShutdown();

      // act
      resolve();
      await shutdown;

      // assert
      expect(loop.next).toHaveBeenCalledTimes(1);
    });
  });

  describe('with an interval', () => {
    it.each([1, 2, 3, 4, 5])('waits before every iteration (%i)', async (i) => {
      // arrange
      const loop = new IntervalLoop();
      const loops = create([loop]);

      // act
      loops.start();
      await vi.advanceTimersByTimeAsync(i * 20 + 5);

      // assert
      expect(loop.next).toHaveBeenCalledTimes(i);
    });

    it('ends without iterating when aborted during the wait', async () => {
      // arrange
      const loop = new IntervalLoop();
      const loops = create([loop]);

      loop.interval.mockReturnValue(10_000);
      loops.start();
      await vi.advanceTimersByTimeAsync(0);

      // act
      await loops.onTransportShutdown();

      // assert
      expect(loop.next).not.toHaveBeenCalled();
    });
  });

  describe('when an iteration fails', () => {
    it('logs the failure', async () => {
      // arrange
      const error = mockLogger();
      const loops = create([new FailingLoop()]);

      // act
      loops.start();
      await vi.advanceTimersByTimeAsync(10);

      // assert
      expect(error).toHaveBeenCalledExactlyOnceWith(
        expect.stringContaining('FailingLoop failed: Error: boom'),
        expect.stringContaining('boom'),
      );
    });

    it('waits 1 second before retrying', async () => {
      // arrange
      mockLogger();

      const loop = new FailingLoop();
      const loops = create([loop]);

      // act
      loops.start();
      await vi.advanceTimersByTimeAsync(500);

      // assert
      expect(loop.next).toHaveBeenCalledTimes(1);
    });

    it('retries after 1 second', async () => {
      // arrange
      mockLogger();

      const loop = new FailingLoop();
      const loops = create([loop]);

      loops.start();
      await vi.advanceTimersByTimeAsync(500);

      // act
      await vi.advanceTimersByTimeAsync(500);

      // assert
      expect(loop.next).toHaveBeenCalledTimes(2);
    });

    it('keeps the other loops running', async () => {
      // arrange
      mockLogger();

      const healthy = new CountingLoop();
      const loops = create([new FailingLoop(), healthy]);

      // act
      loops.start();
      await vi.advanceTimersByTimeAsync(30);

      // assert
      expect(healthy.next.mock.calls.length).toBeGreaterThan(1);
    });
  });

  describe('onTransportShutdown', () => {
    it('shuts all the loops down at once, skipping those without a hook', async () => {
      // arrange
      const first = new ShuttingDownLoop();
      const second = new ShuttingDownLoop();
      const release = Promise.withResolvers<void>();

      first.onTransportShutdown.mockReturnValue(release.promise);
      holds.push(release.resolve);

      const loops = create([first, new BlockingLoop(), second]);

      // act
      void loops.onTransportShutdown();
      await vi.advanceTimersByTimeAsync(0);

      // assert
      // the second loop doesn't wait for the first
      expect(second.onTransportShutdown).toHaveBeenCalledOnce();
      expect(first.onTransportShutdown).toHaveBeenCalledOnce();
    });

    it('calls the hooks while the loops are still running', async () => {
      // arrange
      const loop = new ShuttingDownLoop();
      const iterating = Promise.withResolvers<void>();

      loop.next.mockImplementation(() => iterating.promise);
      holds.push(iterating.resolve);

      const loops = create([loop]);
      const ended = vi.fn();

      loops.start();
      await vi.advanceTimersByTimeAsync(0);

      // act
      void loops.onTransportShutdown().then(ended);
      await vi.advanceTimersByTimeAsync(0);

      // assert
      expect(loop.onTransportShutdown).toHaveBeenCalledOnce();
      expect(ended).not.toHaveBeenCalled();
    });

    it('ends once the running iteration is done', async () => {
      // arrange
      const loop = new ShuttingDownLoop();
      const iterating = Promise.withResolvers<void>();

      loop.next.mockImplementation(() => iterating.promise);

      const loops = create([loop]);
      const ended = vi.fn();

      loops.start();
      await vi.advanceTimersByTimeAsync(0);
      void loops.onTransportShutdown().then(ended);
      await vi.advanceTimersByTimeAsync(0);

      // act
      iterating.resolve();
      await vi.advanceTimersByTimeAsync(0);

      // assert
      expect(ended).toHaveBeenCalledOnce();
    });

    it('lets a hook end its loop', async () => {
      // arrange
      const loop = new BlockingLoop();
      const release = Promise.withResolvers<void>();
      const hooked = Object.assign(loop, {
        onTransportShutdown: vi.fn(() => release.resolve()),
      });

      loop.next.mockImplementation(() => release.promise);

      const loops = create([hooked]);

      loops.start();
      await vi.advanceTimersByTimeAsync(0);

      // act
      const shutdown = loops.onTransportShutdown();

      // assert
      await expect(shutdown).resolves.toBeUndefined();
    });

    it('logs a failure and shuts the other loops down', async () => {
      // arrange
      const error = mockLogger();
      const failing = new ShuttingDownLoop();
      const other = new ShuttingDownLoop();

      failing.onTransportShutdown = vi.fn(() =>
        Promise.reject(new Error('boom')),
      );
      other.onTransportShutdown = vi.fn();

      const loops = create([failing, other]);

      // act
      const shutdown = loops.onTransportShutdown();

      // assert
      await expect(shutdown).resolves.toBeUndefined();
      expect(error).toHaveBeenCalledExactlyOnceWith(
        expect.stringContaining(
          'ShuttingDownLoop failed to shut down: Error: boom',
        ),
        expect.stringContaining('boom'),
      );
      expect(other.onTransportShutdown).toHaveBeenCalledOnce();
    });
  });
});
