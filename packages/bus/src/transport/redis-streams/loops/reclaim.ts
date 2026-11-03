import { Logger } from '@nestjs/common';
import { type Clients } from '../clients.js';
import { type Processor } from '../processor.js';
import { type Streams } from '../streams.js';
import { type Loop } from './loop.js';

export interface ReclaimLoopOptions {
  group: string;
  consumer: string;
  batchSize: number;
  /** How long an entry has to be pending before it is claimed. */
  retriesDelay: number;
  /** How often pending entries are checked, in milliseconds. */
  reclaimInterval: number;
}

export class ReclaimLoop implements Loop {
  private readonly logger = new Logger(ReclaimLoop.name);

  public constructor(
    private readonly clients: Clients<'command'>,
    private readonly streams: Streams,
    private readonly processor: Processor,
    private readonly options: ReclaimLoopOptions,
  ) {}

  public interval(): number {
    // jitter keeps instances from polling at the same moment
    return this.options.reclaimInterval * (0.9 + Math.random() * 0.2);
  }

  public async next(signal: AbortSignal): Promise<void> {
    const { clients, streams, processor } = this;
    const { group, consumer, batchSize, retriesDelay } = this.options;

    for (const { name: stream } of streams.all()) {
      if (signal.aborted) {
        return;
      }

      try {
        const client = await clients.get('command');
        const pending = await client.xPendingRange(
          stream,
          group,
          '-',
          '+',
          batchSize,
          { IDLE: retriesDelay },
        );
        const deliveries = new Map(
          pending
            .filter(({ id }) => !streams.isInflight(stream, id))
            .map((entry) => [entry.id, entry.deliveriesCounter]),
        );

        if (deliveries.size === 0) {
          continue;
        }

        // claiming counts as a delivery, so don't claim what will not run
        if (signal.aborted) {
          return;
        }

        const binaryClient = await clients.getBinary('command');
        const claimed = await binaryClient.xClaim(
          stream,
          group,
          consumer,
          retriesDelay,
          [...deliveries.keys()],
        );

        for (const entry of claimed) {
          if (entry && !signal.aborted) {
            const id = String(entry.id);
            const attempt = (deliveries.get(id) ?? 0) + 1;

            streams.run(stream, id, (current) =>
              processor.process(current, { ...entry, id }, attempt),
            );
          }
        }
      } catch (err) {
        if (signal.aborted) {
          return;
        }

        this.logger.error(
          `Failed to reclaim entries of ${stream}: ${String(err)}`,
          err instanceof Error ? err.stack : undefined,
        );
      }
    }
  }
}
