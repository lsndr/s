import { setTimeout as sleep } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { type Clients } from '../clients.js';
import { type Processor } from '../processor.js';
import { type Streams } from '../streams.js';
import { type OnTransportShutdown } from '../lifecycle.js';
import { type Loop } from './loop.js';

/** How long to wait before unblocking the read again, in milliseconds. */
const UNBLOCK_RETRY_DELAY = 50;

export interface ReadLoopOptions {
  group: string;
  consumer: string;
  /** Maximum number of entries read from a stream at once. */
  batchSize: number;
  /** How long a read blocks waiting for new entries, in milliseconds. */
  blockTimeout: number;
}

/**
 * Reads the streams that have a free slot, and starts the entries.
 *
 * A blocked read only covers the streams that were readable when it started,
 * so while it blocks, the loop watches the streams that are not part of it.
 * When one of them gets a free slot, or is registered, the read is
 * interrupted with CLIENT UNBLOCK, instead of waiting for the block timeout.
 * The same happens on shutdown, so that the loop doesn't wait for the block
 * timeout to end.

 */
export class ReadLoop implements Loop, OnTransportShutdown {
  private readonly logger = new Logger(ReadLoop.name);
  /** Aborted once the current read is over. Unset while nothing is read. */
  private readDone: AbortController | undefined;

  public constructor(
    private readonly clients: Clients<'read' | 'command'>,
    private readonly streams: Streams,
    private readonly processor: Processor,
    private readonly options: ReadLoopOptions,
  ) {}

  public async next(signal: AbortSignal): Promise<void> {
    const { streams, processor } = this;
    const reply = await this.read(signal);

    for (const { name, messages } of reply) {
      for (const entry of messages) {
        streams.run(String(name), String(entry.id), (stream) =>
          processor.process(stream, { ...entry, id: String(entry.id) }, 1),
        );
      }
    }
  }

  /** Interrupts the read that is blocked, so that the loop can end. */
  public async onTransportShutdown(): Promise<void> {
    const done = this.readDone?.signal;

    if (!done) {
      return;
    }

    try {
      await this.unblock(done);
    } catch (err) {
      this.logger.warn(`Failed to unblock the read: ${String(err)}`);
    }
  }

  /**
   * Reads the streams that have a free slot, and interrupts the read while it
   * blocks if a stream outside of it becomes readable.
   *
   * @returns The entries, none once `signal` is aborted.
   */
  private async read(signal: AbortSignal) {
    const { clients, streams } = this;
    const { group, consumer, batchSize, blockTimeout } = this.options;

    // Connected before the streams are known, so that nothing is awaited
    // between taking them and starting the read
    const client = await clients.getBinary('read');

    // Waits for a free slot instead of calling Redis when all are full
    const keys = await streams.readable(signal);

    if (keys.length === 0) {
      return [];
    }

    const done = new AbortController();

    this.readDone = done;

    // Both start in the same tick, so the watcher can only unblock a read
    // that was sent
    const reading = client.xReadGroup(
      group,
      consumer,
      keys.map((key) => ({ key, id: '>' })),
      { COUNT: batchSize, BLOCK: blockTimeout },
    );
    const unblocking = this.unblockOnReadable(
      keys,
      AbortSignal.any([signal, done.signal]),
    );

    try {
      return (await reading) ?? [];
    } finally {
      done.abort();
      this.readDone = undefined;
      // Settles before the next read, so an unblock can't hit it
      await unblocking;
    }
  }

  /**
   * Unblocks the read once a stream that it doesn't cover has a free slot, and
   * keeps trying until that worked. Gives up when `signal` is aborted, which
   * is also when the read is over. It never rejects, a read that couldn't be
   * interrupted is only slower.
   */
  private async unblockOnReadable(
    keys: string[],
    stop: AbortSignal,
  ): Promise<void> {
    try {
      // Waits for a stream that the read doesn't cover
      const [stream] = await this.streams.readable(stop, { exclude: keys });

      if (stream !== undefined) {
        await this.unblock(stop);
      }
    } catch (err) {
      this.logger.warn(`Failed to unblock the read: ${String(err)}`);
    }
  }

  /**
   * Interrupts the blocked read with CLIENT UNBLOCK, retried until it worked:
   * the command can reach Redis before the read blocks on it, and then it
   * does nothing. Gives up when `stop` is aborted.
   */
  private async unblock(stop: AbortSignal): Promise<void> {
    const { clients } = this;
    const client = await clients.get('command');

    while (!stop.aborted) {
      const id = clients.id('read');

      if (id === undefined) {
        this.logger.warn(
          'Cannot unblock the read: the id of the read client is unknown',
        );
      } else if ((await client.clientUnblock(id, 'TIMEOUT')) > 0) {
        // Makes the blocked read return as if it timed out
        return;
      }

      await sleep(UNBLOCK_RETRY_DELAY, undefined, { signal: stop }).catch(
        () => undefined,
      );
    }
  }
}
