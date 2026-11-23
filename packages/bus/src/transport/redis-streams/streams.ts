import { type EventConstructor } from '../../types.js';
import { type Handler } from '../handler.js';
import { Inflight } from './inflight.js';

export interface Stream {
  readonly name: string;
  readonly event: EventConstructor<any>;
  readonly handlers: readonly Handler[];
}

interface StreamState extends Stream {
  readonly handlers: Handler[];
}

export class Streams {
  private readonly streams = new Map<string, StreamState>();
  private readonly entries: Inflight;

  public constructor(slotsPerStream: number) {
    this.entries = new Inflight(slotsPerStream);
  }

  public has(name: string): boolean {
    return this.streams.has(name);
  }

  public all(): Stream[] {
    return [...this.streams.values()];
  }

  /**
   * A handler with the name of a registered one joins it, so its callbacks run
   * together.
   *
   * @throws If the stream is registered for another event.
   */
  public register(
    name: string,
    event: EventConstructor<any>,
    handler: Handler,
  ): void {
    let stream = this.streams.get(name);

    if (stream && stream.event !== event) {
      throw new Error(
        `Stream ${name} is registered for ${stream.event.name}, not ${event.name}`,
      );
    }

    if (!stream) {
      stream = { name, event, handlers: [] };
      this.streams.set(name, stream);
      this.entries.register(name);
    }

    const existing = stream.handlers.find(({ name }) => name === handler.name);

    if (existing) {
      existing.merge(handler);
    } else {
      stream.handlers.push(handler);
    }
  }

  /**
   * Names of the streams that have a free slot. Waits while there are none,
   * so nothing is read from a full stream.
   *
   * @param options.exclude - Streams that don't count, so it waits for a
   * stream that isn't one of them.
   * @returns The streams, or an empty list once `signal` is aborted.
   */
  public readable(
    signal: AbortSignal,
    options?: { exclude?: readonly string[] },
  ): Promise<string[]> {
    return this.entries.readable(signal, options);
  }

  /**
   * Starts the entry without waiting for it, and does nothing if it is in
   * flight already. A failure of `work` is logged.
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

    this.entries.run(name, id, () => work(stream));
  }

  public inflight(name: string): string[] {
    return this.entries.ids(name);
  }

  /** Number of slots of every stream. */
  public slots(): number {
    return this.entries.slots;
  }

  /** Seconds the stream has had no free slot, the current stretch included. */
  public fullDuration(name: string): number {
    return this.entries.fullDuration(name);
  }

  public isInflight(name: string, id: string): boolean {
    return this.entries.has(name, id);
  }

  /**
   * @param timeout - Gives up after this many milliseconds. Without it, waits
   * for as long as it takes.
   * @returns `false` if the timeout was reached with entries still running.
   */
  public drain(timeout?: number): Promise<boolean> {
    return this.entries.drain(timeout);
  }
}
