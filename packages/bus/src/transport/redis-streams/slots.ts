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

  /**
   * Streams that have at least one free slot. When there are none, waits
   * until a slot is released or a stream is added.
   *
   * @param signal - Stops the wait.
   * @returns The streams, or an empty list once `signal` is aborted.
   */
  public async readable(signal: AbortSignal): Promise<string[]> {
    while (!signal.aborted) {
      const streams = [...this.free]
        .filter(([, free]) => free > 0)
        .map(([stream]) => stream);

      if (streams.length > 0) {
        return streams;
      }

      await this.waitForChange(signal);
    }

    return [];
  }

  private async waitForChange(signal: AbortSignal): Promise<void> {
    const wake = () => this.changed.notify();

    signal.addEventListener('abort', wake, { once: true });

    try {
      await this.changed.wait();
    } finally {
      signal.removeEventListener('abort', wake);
    }
  }

  private getFree(stream: string): number {
    return this.free.get(stream) ?? this.size;
  }
}
