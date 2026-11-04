import { afterEach, describe, expect, it, vi } from 'vitest';
import { setTimeout } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { Handler } from './handler.js';
import { Streams } from './streams.js';

class TestEvent {}
class OtherEvent {}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });

  return { promise, resolve };
}

describe('Streams', () => {
  const never = new AbortController().signal;

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('has', () => {
    it('is false for a stream that is not registered', () => {
      // arrange
      const streams = new Streams(1);

      // act
      const has = streams.has('a');

      // assert
      expect(has).toBe(false);
    });

    it('is true once a handler is registered for the stream', () => {
      // arrange
      const streams = new Streams(1);
      streams.register('a', TestEvent, new Handler('h', () => {}));

      // act
      const has = streams.has('a');

      // assert
      expect(has).toBe(true);
    });
  });

  describe('register', () => {
    it('adds a stream with its event and handler', () => {
      // arrange
      const streams = new Streams(1);
      const handler = new Handler('h', vi.fn());

      // act
      streams.register('a', TestEvent, handler);
      const [stream] = streams.all();

      // assert
      expect(streams.all()).toHaveLength(1);
      expect(stream?.name).toBe('a');
      expect(stream?.event).toBe(TestEvent);
      expect(stream?.handlers).toEqual([handler]);
    });

    it('adds another handler to the same stream', () => {
      // arrange
      const streams = new Streams(1);
      const first = new Handler('h1', vi.fn());
      const second = new Handler('h2', vi.fn());
      streams.register('a', TestEvent, first);

      // act
      streams.register('a', TestEvent, second);
      const [stream] = streams.all();

      // assert
      expect(streams.all()).toHaveLength(1);
      expect(stream?.handlers).toEqual([first, second]);
    });

    it('throws if the stream is registered for another event', () => {
      // arrange
      const streams = new Streams(1);
      streams.register('a', TestEvent, new Handler('h1', () => {}));

      // act
      const act = () =>
        streams.register('a', OtherEvent, new Handler('h2', () => {}));

      // assert
      expect(act).toThrow(
        'Stream a is registered for TestEvent, not OtherEvent',
      );
      const [stream] = streams.all();
      expect(stream?.event).toBe(TestEvent);
      expect(stream?.handlers.map(({ name }) => name)).toEqual(['h1']);
    });

    it('throws if the handler is already registered for the stream', () => {
      // arrange
      const streams = new Streams(1);
      streams.register('a', TestEvent, new Handler('h', () => {}));

      // act
      const act = () =>
        streams.register('a', TestEvent, new Handler('h', () => {}));

      // assert
      expect(act).toThrow('Handler h is already subscribed to a');
    });
  });

  describe('all', () => {
    it('is empty without streams', () => {
      // arrange
      const streams = new Streams(1);

      // act
      const all = streams.all();

      // assert
      expect(all).toEqual([]);
    });

    it('lists the streams in the order they were registered', () => {
      // arrange
      const streams = new Streams(1);
      streams.register('b', TestEvent, new Handler('h', () => {}));
      streams.register('a', TestEvent, new Handler('h', () => {}));
      streams.register('c', TestEvent, new Handler('h', () => {}));

      // act
      const names = streams.all().map((stream) => stream.name);

      // assert
      expect(names).toEqual(['b', 'a', 'c']);
    });
  });

  describe('readable', () => {
    it('lists a registered stream right away', async () => {
      // arrange
      const streams = new Streams(1);
      streams.register('a', TestEvent, new Handler('h', () => {}));

      // act
      const readable = await streams.readable(never);

      // assert
      expect(readable).toEqual(['a']);
    });

    it('returns an empty list when the signal is aborted', async () => {
      // arrange
      const streams = new Streams(1);
      streams.register('a', TestEvent, new Handler('h', () => {}));

      // act
      const readable = await streams.readable(AbortSignal.abort());

      // assert
      expect(readable).toEqual([]);
    });

    it('does not list a stream once all its slots are taken', async () => {
      // arrange
      const streams = new Streams(1);
      streams.register('a', TestEvent, new Handler('h', () => {}));
      streams.register('b', TestEvent, new Handler('h', () => {}));
      streams.run('a', '1', () => deferred().promise);

      // act
      const readable = await streams.readable(never);

      // assert
      expect(readable).toEqual(['b']);
    });
  });

  describe('run', () => {
    it('throws for a stream that is not registered', () => {
      // arrange
      const streams = new Streams(1);

      // act
      const act = () => streams.run('a', '1', async () => {});

      // assert
      expect(act).toThrow('Stream a is not registered');
    });

    it('passes the registered stream to the work', () => {
      // arrange
      const streams = new Streams(1);
      const work = vi.fn(async () => {});
      streams.register('a', TestEvent, new Handler('h', () => {}));

      // act
      streams.run('a', '1', work);

      // assert
      expect(work).toHaveBeenCalledExactlyOnceWith(streams.all()[0]);
    });

    it('lists the entry as in flight while the work is pending', () => {
      // arrange
      const streams = new Streams(2);
      streams.register('a', TestEvent, new Handler('h', () => {}));

      // act
      streams.run('a', '1', () => deferred().promise);

      // assert
      expect(streams.inflight('a')).toEqual(['1']);
      expect(streams.isInflight('a', '1')).toBe(true);
      expect(streams.isInflight('a', '2')).toBe(false);
    });

    it('removes the entry once the work settles', async () => {
      // arrange
      const streams = new Streams(2);
      const held = deferred();
      streams.register('a', TestEvent, new Handler('h', () => {}));
      streams.run('a', '1', () => held.promise);

      // act
      held.resolve();
      await setTimeout(0);

      // assert
      expect(streams.inflight('a')).toEqual([]);
      expect(streams.isInflight('a', '1')).toBe(false);
    });

    it('does not start the work again for an entry in flight', () => {
      // arrange
      const streams = new Streams(2);
      const work = vi.fn(async () => {});
      streams.register('a', TestEvent, new Handler('h', () => {}));
      streams.run('a', '1', () => deferred().promise);

      // act
      streams.run('a', '1', work);

      // assert
      expect(work).not.toHaveBeenCalled();
    });

    it('logs a failing work instead of throwing', async () => {
      // arrange
      const streams = new Streams(1);
      const error = vi
        .spyOn(Logger.prototype, 'error')
        .mockImplementation(() => undefined);
      streams.register('a', TestEvent, new Handler('h', () => {}));

      // act
      streams.run('a', '1', () => Promise.reject(new Error('boom')));
      await setTimeout(0);

      // assert
      expect(error).toHaveBeenCalledOnce();
    });
  });

  describe('inflight', () => {
    it('is empty for a stream that is not registered', () => {
      // arrange
      const streams = new Streams(1);

      // act
      const ids = streams.inflight('a');

      // assert
      expect(ids).toEqual([]);
    });
  });

  describe('isInflight', () => {
    it('is false for a stream that is not registered', () => {
      // arrange
      const streams = new Streams(1);

      // act
      const inflight = streams.isInflight('a', '1');

      // assert
      expect(inflight).toBe(false);
    });
  });

  describe('drain', () => {
    it('resolves once the entries in flight settle', async () => {
      // arrange
      const streams = new Streams(1);
      const held = deferred();
      streams.register('a', TestEvent, new Handler('h', () => {}));
      streams.run('a', '1', () => held.promise);
      const draining = streams.drain();

      // act
      held.resolve();
      const drained = await draining;

      // assert
      expect(drained).toBe(true);
    });

    it('gives up after the timeout', async () => {
      // arrange
      const streams = new Streams(1);
      streams.register('a', TestEvent, new Handler('h', () => {}));
      streams.run('a', '1', () => deferred().promise);

      // act
      const drained = await streams.drain(10);

      // assert
      expect(drained).toBe(false);
    });
  });
});
