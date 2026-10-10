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

  public get(name: string): Stream | undefined {
    return this.streams.get(name);
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

  /** Entries of all streams that are running. */
  public inflight(): Promise<void>[] {
    return this.all().flatMap(({ inflight }) => [...inflight.values()]);
  }

  /** Names of the streams that have a free slot. */
  public readable(): string[] {
    return this.slots.readable();
  }

  /** Resolves when a stream becomes readable or after {@link Streams.wake}. */
  public wait(): Promise<void> {
    return this.slots.wait();
  }

  public wake(): void {
    this.slots.wake();
  }

  /**
   * Runs an entry of a stream. It holds a slot of the stream and is listed as
   * in flight until `work` settles.
   */
  public run(
    stream: Stream,
    id: string,
    work: () => Promise<void>,
  ): Promise<void> {
    this.slots.take(stream.name);

    const promise = work().finally(() => {
      stream.inflight.delete(id);
      this.slots.release(stream.name);
    });

    stream.inflight.set(id, promise);

    return promise;
  }
}
