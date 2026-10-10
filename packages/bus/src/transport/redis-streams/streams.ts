import { setTimeout as sleep } from 'node:timers/promises';
import { type EventConstructor, type EventPayload } from '../../types.js';
import { Slots } from './slots.js';

export type Callback = (event: EventPayload<any>) => void | Promise<void>;

export interface Stream {
  name: string;
  event: EventConstructor<any>;
  handlers: Map<string, Callback>;
  /** Entries that are running, by id. */
  inflight: Map<string, Promise<void>>;
}

/**
 * The streams an instance consumes, with their handlers, the entries in
 * flight and the slots that decide which streams can be read.
 */
export class Streams {
  private readonly streams = new Map<string, Stream>();
  private readonly slots: Slots;

  public constructor(slotsPerStream: number) {
    this.slots = new Slots(slotsPerStream);
  }

  public has(name: string): boolean {
    return this.streams.has(name);
  }

  public all(): Stream[] {
    return [...this.streams.values()];
  }

  /**
   * Adds a handler to a stream, which is added too if it is new.
   *
   * @throws If the handler is already registered for the stream.
   */
  public register(
    name: string,
    event: EventConstructor<any>,
    handler: string,
    callback: Callback,
  ): void {
    let stream = this.streams.get(name);

    if (stream?.handlers.has(handler)) {
      throw new Error(`Handler ${handler} is already subscribed to ${name}`);
    }

    if (!stream) {
      stream = { name, event, handlers: new Map(), inflight: new Map() };
      this.streams.set(name, stream);
      this.slots.register(name);
    }

    stream.handlers.set(handler, callback);
  }

  /**
   * Waits for the entries that are running now, of all streams, to finish.
   *
   * @param timeout - Gives up after this many milliseconds. Without it, waits
   * for as long as it takes.
   * @returns `false` if the timeout was reached with entries still running.
   */
  public async drain(timeout?: number): Promise<boolean> {
    const pending = this.all().flatMap(({ inflight }) => [
      ...inflight.values(),
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

  /**
   * Names of the streams that have a free slot. Waits while there are none,
   * so nothing is read from a full stream.
   *
   * @param signal - Stops the wait.
   * @returns The streams, or an empty list once `signal` is aborted.
   */
  public readable(signal: AbortSignal): Promise<string[]> {
    return this.slots.readable(signal);
  }

  /**
   * Starts an entry of a stream without waiting for it. It holds a slot of the
   * stream and is listed as in flight until `work` settles, see
   * {@link Streams.drain}. `work` must not reject.
   *
   * @throws If the stream is not registered.
   */
  public run(
    name: string,
    id: string,
    work: (stream: Stream) => Promise<void>,
  ): void {
    const stream = this.streams.get(name);

    if (!stream) {
      throw new Error(`Stream ${name} is not registered`);
    }

    this.slots.take(name);

    const promise = work(stream).finally(() => {
      stream.inflight.delete(id);
      this.slots.release(name);
    });

    stream.inflight.set(id, promise);
  }
}
