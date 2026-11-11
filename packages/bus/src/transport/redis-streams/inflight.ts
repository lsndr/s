import { setTimeout as sleep } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { Signal } from './signal.js';

interface Saturation {
  /** Milliseconds the stream was full before the current stretch. */
  total: number;
  /** When the stream became full, unset while it has a free slot. */
  since?: number;
}

/**
 * Each stream has `size` slots and every entry it runs holds one. Running is
 * not limited by the free count: entries that are read or reclaimed run
 * immediately, so a stream can hold more entries than it has slots.
 */
export class Inflight {
  private readonly inflight = new Map<string, Map<string, Promise<void>>>();
  private readonly saturation = new Map<string, Saturation>();
  private readonly changes = new Signal();
  private readonly logger = new Logger(Inflight.name);

  /**
   * @param now - Clock in milliseconds. A parameter so that tests control how
   * long a stream stays full.
   */
  public constructor(
    private readonly size: number,
    private readonly now: () => number = () => performance.now(),
  ) {}

  /** Number of slots of every stream. */
  public get slots(): number {
    return this.size;
  }

  public register(stream: string): void {
    if (!this.inflight.has(stream)) {
      this.inflight.set(stream, new Map());
      this.saturation.set(stream, { total: 0 });
      this.changes.notify();
    }
  }

  /**
   * Starts the entry without waiting for it, and does nothing if it is running
   * already. A failure of `work` is logged, it doesn't reject.
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
        this.track(stream, entries.size);
        this.changes.notify();
      });

    entries.set(id, promise);
    this.track(stream, entries.size);
    // a slot is taken, which whoever waits for a change has to know
    this.changes.notify();
  }

  public ids(stream: string): string[] {
    return [...(this.inflight.get(stream)?.keys() ?? [])];
  }

  public has(stream: string, id: string): boolean {
    return this.inflight.get(stream)?.has(id) ?? false;
  }

  /**
   * Seconds the stream has had no free slot since it was registered,
   * including the time it is full right now.
   */
  public fullDuration(stream: string): number {
    const state = this.saturation.get(stream);

    if (!state) {
      return 0;
    }

    const running = state.since === undefined ? 0 : this.now() - state.since;

    return (state.total + running) / 1000;
  }

  /**
   * Streams that have at least one free slot. When there are none, waits
   * until a slot is released or a stream is added.
   *
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
      // don't keep the process alive for a timeout that is not needed
      timer.abort();
    }
  }

  /** Called after every change of the entries, to time how long it is full. */
  private track(stream: string, running: number): void {
    const state = this.saturation.get(stream);

    if (!state) {
      return;
    }

    if (running >= this.size && state.since === undefined) {
      state.since = this.now();
    } else if (running < this.size && state.since !== undefined) {
      state.total += this.now() - state.since;
      state.since = undefined;
    }
  }

  private free(): string[] {
    return [...this.inflight]
      .filter(([, entries]) => entries.size < this.size)
      .map(([stream]) => stream);
  }

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
