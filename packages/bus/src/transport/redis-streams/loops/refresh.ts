import { Logger } from '@nestjs/common';
import { type Clients } from '../clients.js';
import { type Streams } from '../streams.js';
import { type Loop } from './loop.js';

export interface RefreshLoopOptions {
  group: string;
  consumer: string;
  /** How long an entry has to be pending before it is claimed again. */
  retriesDelay: number;
}

/** Keeps the entries in flight from looking abandoned. */
export class RefreshLoop implements Loop {
  private readonly logger = new Logger(RefreshLoop.name);

  public constructor(
    private readonly clients: Clients<'command'>,
    private readonly streams: Streams,
    private readonly options: RefreshLoopOptions,
  ) {}

  public interval(): number {
    return Math.max(this.options.retriesDelay / 2, 10);
  }

  public async next(): Promise<void> {
    const { group, consumer } = this.options;

    for (const { name: stream, inflight } of this.streams.all()) {
      if (inflight.size === 0) {
        continue;
      }

      try {
        const client = await this.clients.get('command');

        await client.xClaimJustId(stream, group, consumer, 0, [
          ...inflight.keys(),
        ]);
      } catch (err) {
        this.logger.warn(
          `Failed to refresh entries of ${stream}: ${String(err)}`,
        );
      }
    }
  }
}
