import { setTimeout as sleep } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { Signal } from './signal.js';

/**
 * The entries in flight of the streams. Each stream has `size` slots and every
 * entry it runs holds one, so a stream with a free slot may be read.
 *
 * Running is not limited by the free count: entries that are read or reclaimed
 * run immediately, so a stream can hold more entries than it has slots.
 */
export class Inflight {
  /** Entries that are running, by stream and id. */
  private readonly inflight = new Map<string, Map<string, Promise<void>>>();
  private readonly changes = new Signal();
  private readonly logger = new Logger(Inflight.name);

  public constructor(private readonly size: number) {}

  public register(stream: string): void {
    if (!this.inflight.has(stream)) {
      this.inflight.set(stream, new Map());
      this.changes.notify();
    }
  }

  /**
   * Starts an entry of a stream without waiting for it. It holds a slot until
   * `work` settles. Does nothing if the entry is running already. A failure of
   * `work` is logged, it doesn't reject.
   *
   * @throws If the stream is not registered.
   */
  public run(stream: string, id: string, work: () => Promise<void>): void {
    const entries = this.inflight.get(stream);

    if (!entries) {
      throw new Error(`Stream ${stream} is not registered`);
    }

    if (entries.has(id)) {
      return;
    }

    const promise = (async () => work())()
      .catch((err) =>
        this.logger.error(
          `Entry ${id} of ${stream} failed: ${String(err)}`,
          err instanceof Error ? err.stack : undefined,
        ),
      )
      .finally(() => {
        entries.delete(id);
        this.changes.notify();
      });

    entries.set(id, promise);
    // A slot is taken, which whoever waits for a change has to know
    this.changes.notify();
  }

  /** Ids of the entries of a stream that are running. */
  public ids(stream: string): string[] {
    return [...(this.inflight.get(stream)?.keys() ?? [])];
  }

  public has(stream: string, id: string): boolean {
    return this.inflight.get(stream)?.has(id) ?? false;
  }

  /**
   * Streams that have at least one free slot. When there are none, waits
   * until a slot is released or a stream is added.
   *
   * @param signal - Stops the wait.
   * @param options.exclude - Streams that don't count, so it waits for a
   * stream that isn't one of them.
   * @returns The streams, or an empty list once `signal` is aborted.
   */
  public async readable(
    signal: AbortSignal,
    options: { exclude?: readonly string[] } = {},
  ): Promise<string[]> {
    const exclude = new Set(options.exclude);

    while (!signal.aborted) {
      const streams = this.free().filter((stream) => !exclude.has(stream));

      if (streams.length > 0) {
        return streams;
      }

      await this.changed(signal);
    }

    return [];
  }

  /**
   * Waits for the entries that are running now, of all streams, to finish.
   *
   * @param timeout - Gives up after this many milliseconds. Without it, waits
   * for as long as it takes.
   * @returns `false` if the timeout was reached with entries still running.
   */
  public async drain(timeout?: number): Promise<boolean> {
    const pending = [...this.inflight.values()].flatMap((entries) => [
      ...entries.values(),
    ]);

    if (pending.length === 0) {
      return true;
    }

    const drained = Promise.allSettled(pending).then(() => true);

    if (timeout === undefined) {
      return drained;
    }

    const timer = new AbortController();
    const timedOut = sleep(timeout, undefined, { signal: timer.signal }).then(
      () => false,
      () => false,
    );

    try {
      return await Promise.race([drained, timedOut]);
    } finally {
      // Don't keep the process alive for a timeout that is not needed
      timer.abort();
    }
  }

  /** Streams that have at least one free slot, if any. */
  private free(): string[] {
    return [...this.inflight]
      .filter(([, entries]) => entries.size < this.size)
      .map(([stream]) => stream);
  }

  /**
   * Waits for the next change: an entry that takes or releases a slot, or a
   * stream that is added. A change that happens while nobody waits is not
   * remembered, so check the state right before waiting.
   *
   * @param signal - Stops the wait.
   */
  private async changed(signal: AbortSignal): Promise<void> {
    if (signal.aborted) {
      return;
    }

    const wake = () => this.changes.notify();

    signal.addEventListener('abort', wake, { once: true });

    try {
      await this.changes.wait();
    } finally {
      signal.removeEventListener('abort', wake);
    }
  }
}
