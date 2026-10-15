import { Logger } from '@nestjs/common';
import { type Clients } from '../clients.js';
import { type Streams } from '../streams.js';
import { type Loop } from './loop.js';

/**
 * Interrupts the blocked read when a stream becomes readable, so the stream
 * is read right away instead of after the block timeout.
 *
 * It sleeps until the slots change, and compares the readable streams with
 * the ones it saw before, so it only approximates what the read loop is
 * reading: an interruption can be wasted when nothing is blocked or the next
 * read would include the stream anyway.
 */
export class UnblockLoop implements Loop {
  private previous = new Set<string>();
  private readonly logger = new Logger(UnblockLoop.name);

  public constructor(
    private readonly clients: Clients<'read' | 'command'>,
    private readonly streams: Streams,
  ) {}

  public async next(signal: AbortSignal): Promise<void> {
    const { clients, streams } = this;
    const free = streams.free();
    const added = free.some((stream) => !this.previous.has(stream));

    this.previous = new Set(free);

    if (!added) {
      await streams.changed(signal);

      return;
    }

    const id = clients.id('read');

    if (id === undefined) {
      this.logger.warn(
        'Cannot unblock the read: the id of the read client is unknown',
      );

      return;
    }

    const client = await clients.get('command');

    // Makes the blocked read return as if it timed out
    await client.clientUnblock(id, 'TIMEOUT');
  }
}
