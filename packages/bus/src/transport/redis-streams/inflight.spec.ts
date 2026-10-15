import { describe, expect, it, vi } from 'vitest';
import { setTimeout } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { Inflight } from './inflight.js';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });

  return { promise, resolve };
}

describe('Inflight', () => {
  const never = new AbortController().signal;

  describe('readable', () => {
    it('makes a registered stream readable right away', async () => {
      const inflight = new Inflight(2);
      inflight.register('a');

      const readable = await inflight.readable(never);

      expect(readable).toEqual(['a']);
    });

    it('lists the streams in the order they were registered', async () => {
      const inflight = new Inflight(1);
      inflight.register('b');
      inflight.register('a');
      inflight.register('c');

      const readable = await inflight.readable(never);

      expect(readable).toEqual(['b', 'a', 'c']);
    });

    it('keeps a stream readable until all its slots are taken', async () => {
      const inflight = new Inflight(2);
      inflight.register('a');
      inflight.register('b');
      inflight.run('a', '1', () => deferred().promise);
      inflight.run('a', '2', () => deferred().promise);

      const readable = await inflight.readable(never);

      expect(readable).toEqual(['b']);
    });

    describe('with more entries than slots', () => {
      // 2 slots, 3 entries run, e.g. a read and a reclaim
      function setup() {
        const inflight = new Inflight(2);
        const held = [deferred(), deferred(), deferred()];
        inflight.register('a');
        inflight.register('b');
        held.forEach(({ promise }, i) =>
          inflight.run('a', String(i), () => promise),
        );

        return { inflight, held };
      }

      it('is not readable while the entries exceed the slots', async () => {
        const { inflight } = setup();

        const readable = await inflight.readable(never);

        expect(readable).toEqual(['b']);
      });

      it('is not readable when the overflow is finished but no slot is free', async () => {
        const { inflight, held } = setup();

        held[0]?.resolve();
        await setTimeout(0);
        const readable = await inflight.readable(never);

        expect(readable).toEqual(['b']);
      });

      it('is readable again once a slot is free', async () => {
        const { inflight, held } = setup();

        held[0]?.resolve();
        held[1]?.resolve();
        await setTimeout(0);
        const readable = await inflight.readable(never);

        expect(readable).toEqual(['a', 'b']);
      });
    });

    describe('when no stream is readable', () => {
      it('waits until an entry settles', async () => {
        const inflight = new Inflight(1);
        const held = deferred();
        const onReadable = vi.fn();
        inflight.register('a');
        inflight.run('a', '1', () => held.promise);
        const waiting = inflight.readable(never).then(onReadable);
        await setTimeout(0);
        expect(onReadable).not.toHaveBeenCalled();

        held.resolve();
        await waiting;

        expect(onReadable).toHaveBeenCalledExactlyOnceWith(['a']);
      });

      it('waits until a stream is registered', async () => {
        const inflight = new Inflight(1);
        const onReadable = vi.fn();
        const waiting = inflight.readable(never).then(onReadable);
        await setTimeout(10);
        expect(onReadable).not.toHaveBeenCalled();

        inflight.register('a');
        await waiting;

        expect(onReadable).toHaveBeenCalledExactlyOnceWith(['a']);
      });

      it('returns an empty list when the signal is aborted', async () => {
        const inflight = new Inflight(1);
        const controller = new AbortController();
        inflight.register('a');
        inflight.run('a', '1', () => deferred().promise);
        const waiting = inflight.readable(controller.signal);
        await setTimeout(0);

        controller.abort();
        const readable = await waiting;

        expect(readable).toEqual([]);
      });
    });

    it('returns an empty list right away when the signal is already aborted', async () => {
      const inflight = new Inflight(1);
      inflight.register('a');

      const readable = await inflight.readable(AbortSignal.abort());

      expect(readable).toEqual([]);
    });
  });

  describe('run', () => {
    it('throws for a stream that is not registered', () => {
      const inflight = new Inflight(1);

      const act = () => inflight.run('a', '1', async () => {});

      expect(act).toThrow('Stream a is not registered');
    });

    it('lists the entry as running', () => {
      const inflight = new Inflight(2);
      inflight.register('a');

      inflight.run('a', '1', () => deferred().promise);

      expect(inflight.ids('a')).toEqual(['1']);
      expect(inflight.has('a', '1')).toBe(true);
      expect(inflight.has('a', '2')).toBe(false);
    });

    it('removes the entry once it settles', async () => {
      const inflight = new Inflight(2);
      const held = deferred();
      inflight.register('a');
      inflight.run('a', '1', () => held.promise);

      held.resolve();
      await setTimeout(0);

      expect(inflight.ids('a')).toEqual([]);
      expect(inflight.has('a', '1')).toBe(false);
    });

    describe('when the work fails', () => {
      it('logs a rejection and a synchronous throw', async () => {
        const inflight = new Inflight(1);
        const error = vi
          .spyOn(Logger.prototype, 'error')
          .mockImplementation(() => undefined);
        inflight.register('a');

        inflight.run('a', '1', () => Promise.reject(new Error('boom')));
        inflight.run('a', '2', () => {
          throw new Error('sync boom');
        });
        await setTimeout(0);

        expect(error).toHaveBeenCalledTimes(2);
        error.mockRestore();
      });

      it('frees the slots and lets drain finish', async () => {
        const inflight = new Inflight(1);
        const error = vi
          .spyOn(Logger.prototype, 'error')
          .mockImplementation(() => undefined);
        inflight.register('a');
        inflight.run('a', '1', () => Promise.reject(new Error('boom')));
        inflight.run('a', '2', () => {
          throw new Error('sync boom');
        });

        const drained = await inflight.drain();

        expect(drained).toBe(true);
        expect(inflight.ids('a')).toEqual([]);
        error.mockRestore();
      });
    });

    describe('when the entry is running already', () => {
      it('does not start the work again', () => {
        const inflight = new Inflight(2);
        const work = vi.fn(async () => {});
        inflight.register('a');
        inflight.run('a', '1', () => deferred().promise);

        inflight.run('a', '1', work);

        expect(work).not.toHaveBeenCalled();
      });

      it('holds a single slot', async () => {
        const inflight = new Inflight(2);
        inflight.register('a');
        inflight.run('a', '1', () => deferred().promise);

        inflight.run('a', '1', async () => {});
        const readable = await inflight.readable(never);

        expect(readable).toEqual(['a']);
      });
    });
  });

  describe('drain', () => {
    it('resolves right away when nothing is running', async () => {
      const inflight = new Inflight(1);
      inflight.register('a');

      const drained = await inflight.drain();

      expect(drained).toBe(true);
    });

    it('waits for the entries of all streams', async () => {
      const inflight = new Inflight(1);
      const a = deferred();
      const b = deferred();
      const onDrained = vi.fn();
      inflight.register('a');
      inflight.register('b');
      inflight.run('a', '1', () => a.promise);
      inflight.run('b', '1', () => b.promise);
      const waiting = inflight.drain().then(onDrained);

      a.resolve();
      await setTimeout(0);

      expect(onDrained).not.toHaveBeenCalled();

      b.resolve();
      await waiting;

      expect(onDrained).toHaveBeenCalledOnce();
    });

    it('gives up after the timeout', async () => {
      const inflight = new Inflight(1);
      inflight.register('a');
      inflight.run('a', '1', () => deferred().promise);

      const drained = await inflight.drain(10);

      expect(drained).toBe(false);
    });
  });
});
