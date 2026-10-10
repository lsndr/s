import { describe, expect, it, vi } from 'vitest';
import { setTimeout } from 'node:timers/promises';
import { Slots } from './slots.js';

describe('Slots', () => {
  const never = new AbortController().signal;

  it('makes a registered stream readable right away', async () => {
    const slots = new Slots(2);

    slots.register('a');

    await expect(slots.readable(never)).resolves.toEqual(['a']);
  });

  it('lists the streams in the order they were registered', async () => {
    const slots = new Slots(1);

    slots.register('b');
    slots.register('a');
    slots.register('c');

    await expect(slots.readable(never)).resolves.toEqual(['b', 'a', 'c']);
  });

  it('keeps a stream readable until all its slots are taken', async () => {
    const slots = new Slots(2);

    slots.register('a');
    slots.register('b');
    slots.take('a');
    slots.take('a');

    await expect(slots.readable(never)).resolves.toEqual(['b']);
  });

  it('accepts overflow and needs it released before the stream is readable', async () => {
    const slots = new Slots(2);

    slots.register('a');
    slots.register('b');
    // 2 slots, 3 entries are taken, e.g. a read and a reclaim
    slots.take('a');
    slots.take('a');
    slots.take('a');

    await expect(slots.readable(never)).resolves.toEqual(['b']);

    slots.release('a');

    // Back to zero free slots, not readable yet
    await expect(slots.readable(never)).resolves.toEqual(['b']);

    slots.release('a');

    await expect(slots.readable(never)).resolves.toEqual(['a', 'b']);
  });

  describe('when no stream is readable', () => {
    it('waits until a slot is released', async () => {
      const slots = new Slots(1);
      const readable = vi.fn();

      slots.register('a');
      slots.take('a');

      const waiting = slots.readable(never).then(readable);

      await setTimeout(0);
      expect(readable).not.toHaveBeenCalled();

      slots.release('a');
      await waiting;

      expect(readable).toHaveBeenCalledExactlyOnceWith(['a']);
    });

    it('waits until a stream is registered', async () => {
      const slots = new Slots(1);
      const readable = vi.fn();
      const waiting = slots.readable(never).then(readable);

      await setTimeout(10);
      expect(readable).not.toHaveBeenCalled();

      slots.register('a');
      await waiting;

      expect(readable).toHaveBeenCalledExactlyOnceWith(['a']);
    });

    it('returns an empty list when the signal is aborted', async () => {
      const slots = new Slots(1);
      const controller = new AbortController();

      slots.register('a');
      slots.take('a');

      const waiting = slots.readable(controller.signal);

      await setTimeout(0);
      controller.abort();

      await expect(waiting).resolves.toEqual([]);
    });
  });

  it('returns an empty list right away when the signal is already aborted', async () => {
    const slots = new Slots(1);

    slots.register('a');

    await expect(slots.readable(AbortSignal.abort())).resolves.toEqual([]);
  });
});
