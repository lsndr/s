import { type EventConstructor, type EventPayload } from '../../types.js';
import { Inflight } from './inflight.js';

export type Callback = (event: EventPayload<any>) => void | Promise<void>;

export interface Stream {
  readonly name: string;
  readonly event: EventConstructor<any>;
  readonly handlers: ReadonlyMap<string, Callback>;
}

interface StreamState extends Stream {
  readonly handlers: Map<string, Callback>;
}

/**
 * The streams an instance consumes, with their handlers. The entries in flight,
 * and the slots they hold, are kept by {@link Inflight}.
 */
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
      stream = { name, event, handlers: new Map() };
      this.streams.set(name, stream);
      this.entries.register(name);
    }

    stream.handlers.set(handler, callback);
  }

  /**
   * Names of the streams that have a free slot. Waits while there are none,
   * so nothing is read from a full stream.
   *
   * @param signal - Stops the wait.
   * @returns The streams, or an empty list once `signal` is aborted.
   */
  public readable(signal: AbortSignal): Promise<string[]> {
    return this.entries.readable(signal);
  }

  /**
   * Starts an entry of a stream without waiting for it. It is in flight until
   * `work` settles, see {@link Streams.drain}. Does nothing if the entry is in
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

  /** Ids of the entries of a stream that are in flight. */
  public inflight(name: string): string[] {
    return this.entries.ids(name);
  }

  public isInflight(name: string, id: string): boolean {
    return this.entries.has(name, id);
  }

  /**
   * Waits for the entries that are in flight now, of all streams, to finish.
   *
   * @param timeout - Gives up after this many milliseconds. Without it, waits
   * for as long as it takes.
   * @returns `false` if the timeout was reached with entries still running.
   */
  public drain(timeout?: number): Promise<boolean> {
    return this.entries.drain(timeout);
  }
}
