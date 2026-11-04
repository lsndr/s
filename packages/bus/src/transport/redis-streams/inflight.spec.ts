import { afterEach, describe, expect, it, vi } from 'vitest';
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

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('readable', () => {
    it('makes a registered stream readable right away', async () => {
      // arrange
      const inflight = new Inflight(2);
      inflight.register('a');

      // act
      const readable = await inflight.readable(never);

      // assert
      expect(readable).toEqual(['a']);
    });

    it('lists the streams in the order they were registered', async () => {
      // arrange
      const inflight = new Inflight(1);
      inflight.register('b');
      inflight.register('a');
      inflight.register('c');

      // act
      const readable = await inflight.readable(never);

      // assert
      expect(readable).toEqual(['b', 'a', 'c']);
    });

    it('keeps a stream readable until all its slots are taken', async () => {
      // arrange
      const inflight = new Inflight(2);
      inflight.register('a');
      inflight.register('b');
      inflight.run('a', '1', () => deferred().promise);
      inflight.run('a', '2', () => deferred().promise);

      // act
      const readable = await inflight.readable(never);

      // assert
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
        // arrange
        const { inflight } = setup();

        // act
        const readable = await inflight.readable(never);

        // assert
        expect(readable).toEqual(['b']);
      });

      it('is not readable when the overflow is finished but no slot is free', async () => {
        // arrange
        const { inflight, held } = setup();

        held[0]?.resolve();
        await setTimeout(0);

        // act
        const readable = await inflight.readable(never);

        // assert
        expect(readable).toEqual(['b']);
      });

      it('is readable again once a slot is free', async () => {
        // arrange
        const { inflight, held } = setup();

        held[0]?.resolve();
        held[1]?.resolve();
        await setTimeout(0);

        // act
        const readable = await inflight.readable(never);

        // assert
        expect(readable).toEqual(['a', 'b']);
      });
    });

    describe('when no stream is readable', () => {
      it('keeps waiting while no entry settles', async () => {
        // arrange
        const inflight = new Inflight(1);
        const onReadable = vi.fn();
        inflight.register('a');
        inflight.run('a', '1', () => deferred().promise);

        // act
        void inflight.readable(never).then(onReadable);
        await setTimeout(0);

        // assert
        expect(onReadable).not.toHaveBeenCalled();
      });

      it('waits until an entry settles', async () => {
        // arrange
        const inflight = new Inflight(1);
        const held = deferred();
        const onReadable = vi.fn();
        inflight.register('a');
        inflight.run('a', '1', () => held.promise);
        const waiting = inflight.readable(never).then(onReadable);
        await setTimeout(0);

        // act
        held.resolve();
        await waiting;

        // assert
        expect(onReadable).toHaveBeenCalledExactlyOnceWith(['a']);
      });

      it('keeps waiting while no stream is registered', async () => {
        // arrange
        const inflight = new Inflight(1);
        const onReadable = vi.fn();

        // act
        void inflight.readable(never).then(onReadable);
        await setTimeout(10);

        // assert
        expect(onReadable).not.toHaveBeenCalled();
      });

      it('waits until a stream is registered', async () => {
        // arrange
        const inflight = new Inflight(1);
        const onReadable = vi.fn();
        const waiting = inflight.readable(never).then(onReadable);
        await setTimeout(10);

        // act
        inflight.register('a');
        await waiting;

        // assert
        expect(onReadable).toHaveBeenCalledExactlyOnceWith(['a']);
      });

      it('returns an empty list when the signal is aborted', async () => {
        // arrange
        const inflight = new Inflight(1);
        const controller = new AbortController();
        inflight.register('a');
        inflight.run('a', '1', () => deferred().promise);
        const waiting = inflight.readable(controller.signal);
        await setTimeout(0);

        // act
        controller.abort();
        const readable = await waiting;

        // assert
        expect(readable).toEqual([]);
      });
    });

    describe('with excluded streams', () => {
      it('skips the excluded streams that have a free slot', async () => {
        // arrange
        const inflight = new Inflight(1);
        inflight.register('a');
        inflight.register('b');

        // act
        const readable = await inflight.readable(never, { exclude: ['a'] });

        // assert
        expect(readable).toEqual(['b']);
      });

      it('keeps waiting while only the excluded streams have a free slot', async () => {
        // arrange
        const inflight = new Inflight(1);
        const onReadable = vi.fn();
        inflight.register('a');

        // act
        void inflight.readable(never, { exclude: ['a'] }).then(onReadable);
        await setTimeout(10);

        // assert
        expect(onReadable).not.toHaveBeenCalled();
      });

      it('wakes when a stream that is not excluded is registered', async () => {
        // arrange
        const inflight = new Inflight(1);
        const onReadable = vi.fn();
        inflight.register('a');
        const waiting = inflight
          .readable(never, { exclude: ['a'] })
          .then(onReadable);
        await setTimeout(10);

        // act
        inflight.register('b');
        await waiting;

        // assert
        expect(onReadable).toHaveBeenCalledExactlyOnceWith(['b']);
      });

      it('wakes when a slot of a stream that is not excluded is released', async () => {
        // arrange
        const inflight = new Inflight(1);
        const held = deferred();
        const onReadable = vi.fn();
        inflight.register('a');
        inflight.register('b');
        inflight.run('b', '1', () => held.promise);
        const waiting = inflight
          .readable(never, { exclude: ['a'] })
          .then(onReadable);
        await setTimeout(0);

        // act
        held.resolve();
        await waiting;

        // assert
        expect(onReadable).toHaveBeenCalledExactlyOnceWith(['b']);
      });

      it('keeps waiting when an excluded stream takes or releases a slot', async () => {
        // arrange
        const inflight = new Inflight(2);
        const held = deferred();
        const onReadable = vi.fn();
        inflight.register('a');
        void inflight.readable(never, { exclude: ['a'] }).then(onReadable);
        await setTimeout(0);

        // act
        inflight.run('a', '1', () => held.promise);
        held.resolve();
        await setTimeout(10);

        // assert
        expect(onReadable).not.toHaveBeenCalled();
      });
    });

    it('returns an empty list right away when the signal is already aborted', async () => {
      // arrange
      const inflight = new Inflight(1);
      inflight.register('a');

      // act
      const readable = await inflight.readable(AbortSignal.abort());

      // assert
      expect(readable).toEqual([]);
    });
  });

  describe('run', () => {
    it('throws for a stream that is not registered', () => {
      // arrange
      const inflight = new Inflight(1);

      // act
      const act = () => inflight.run('a', '1', async () => {});

      // assert
      expect(act).toThrow('Stream a is not registered');
    });

    it('lists the entry as running', () => {
      // arrange
      const inflight = new Inflight(2);
      inflight.register('a');

      // act
      inflight.run('a', '1', () => deferred().promise);

      // assert
      expect(inflight.ids('a')).toEqual(['1']);
      expect(inflight.has('a', '1')).toBe(true);
      expect(inflight.has('a', '2')).toBe(false);
    });

    it('removes the entry once it settles', async () => {
      // arrange
      const inflight = new Inflight(2);
      const held = deferred();
      inflight.register('a');
      inflight.run('a', '1', () => held.promise);

      // act
      held.resolve();
      await setTimeout(0);

      // assert
      expect(inflight.ids('a')).toEqual([]);
      expect(inflight.has('a', '1')).toBe(false);
    });

    describe('when the work fails', () => {
      it('logs a rejection and a synchronous throw', async () => {
        // arrange
        const inflight = new Inflight(1);
        const error = vi
          .spyOn(Logger.prototype, 'error')
          .mockImplementation(() => undefined);
        inflight.register('a');

        // act
        inflight.run('a', '1', () => Promise.reject(new Error('boom')));
        inflight.run('a', '2', () => {
          throw new Error('sync boom');
        });
        await setTimeout(0);

        // assert
        expect(error).toHaveBeenCalledTimes(2);
      });

      it('frees the slots and lets drain finish', async () => {
        // arrange
        const inflight = new Inflight(1);
        vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
        inflight.register('a');
        inflight.run('a', '1', () => Promise.reject(new Error('boom')));
        inflight.run('a', '2', () => {
          throw new Error('sync boom');
        });

        // act
        const drained = await inflight.drain();

        // assert
        expect(drained).toBe(true);
        expect(inflight.ids('a')).toEqual([]);
      });
    });

    describe('when the entry is running already', () => {
      it('does not start the work again', () => {
        // arrange
        const inflight = new Inflight(2);
        const work = vi.fn(async () => {});
        inflight.register('a');
        inflight.run('a', '1', () => deferred().promise);

        // act
        inflight.run('a', '1', work);

        // assert
        expect(work).not.toHaveBeenCalled();
      });

      it('holds a single slot', async () => {
        // arrange
        const inflight = new Inflight(2);
        inflight.register('a');
        inflight.run('a', '1', () => deferred().promise);
        inflight.run('a', '1', async () => {});

        // act
        const readable = await inflight.readable(never);

        // assert
        expect(readable).toEqual(['a']);
      });
    });
  });

  describe('drain', () => {
    it('resolves right away when nothing is running', async () => {
      // arrange
      const inflight = new Inflight(1);
      inflight.register('a');

      // act
      const drained = await inflight.drain();

      // assert
      expect(drained).toBe(true);
    });

    it('keeps waiting while an entry of a stream is running', async () => {
      // arrange
      const inflight = new Inflight(1);
      const a = deferred();
      const b = deferred();
      const onDrained = vi.fn();
      inflight.register('a');
      inflight.register('b');
      inflight.run('a', '1', () => a.promise);
      inflight.run('b', '1', () => b.promise);
      void inflight.drain().then(onDrained);

      // act
      a.resolve();
      await setTimeout(0);

      // assert
      expect(onDrained).not.toHaveBeenCalled();
    });

    it('waits for the entries of all streams', async () => {
      // arrange
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

      // act
      b.resolve();
      await waiting;

      // assert
      expect(onDrained).toHaveBeenCalledOnce();
    });

    it('gives up after the timeout', async () => {
      // arrange
      const inflight = new Inflight(1);
      inflight.register('a');
      inflight.run('a', '1', () => deferred().promise);

      // act
      const drained = await inflight.drain(10);

      // assert
      expect(drained).toBe(false);
    });
  });
});
