import { Signal } from './signal.js';

/**
 * Credits of the streams. Each stream has `size` slots and takes one for every
 * entry it runs. A stream with a free slot may be read.
 *
 * Taking is not limited by the free count: entries that are read or reclaimed
 * run immediately, so the free count can drop below zero.
 */
export class Slots {
  private readonly free = new Map<string, number>();
  private readonly changed = new Signal();

  public constructor(private readonly size: number) {}

  public register(stream: string): void {
    this.free.set(stream, this.size);
    this.changed.notify();
  }

  public take(stream: string): void {
    this.free.set(stream, this.getFree(stream) - 1);
  }

  public release(stream: string): void {
    this.free.set(stream, this.getFree(stream) + 1);
    this.changed.notify();
  }

  /** Streams that have at least one free slot. */
  public readable(): string[] {
    return [...this.free]
      .filter(([, free]) => free > 0)
      .map(([stream]) => stream);
  }

  /**
   * Resolves when a stream is added or a slot is released. Check
   * {@link Slots.readable} right before waiting.
   */
  public wait(): Promise<void> {
    return this.changed.wait();
  }

  /** Wakes the waiter without a change, e.g. to let it see a shutdown. */
  public wake(): void {
    this.changed.notify();
  }

  private getFree(stream: string): number {
    return this.free.get(stream) ?? this.size;
  }
}
