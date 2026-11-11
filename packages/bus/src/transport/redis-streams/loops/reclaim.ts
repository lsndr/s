import { Logger } from '@nestjs/common';
import {
  type TelemetryBatch,
  type TelemetryServer,
} from '../../../telemetry/telemetry.js';
import { type Instrumentation } from '../../../telemetry/instrumentation.js';
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
  /** Redis server, which telemetry reports. */
  server?: TelemetryServer;
}

export class ReclaimLoop implements Loop {
  private readonly logger = new Logger(ReclaimLoop.name);

  public constructor(
    private readonly clients: Clients<'command'>,
    private readonly streams: Streams,
    private readonly processor: Processor,
    private readonly instrumentation: Instrumentation,
    private readonly options: ReclaimLoopOptions,
  ) {}

  public interval(): number {
    // jitter keeps instances from polling at the same moment
    return this.options.reclaimInterval * (0.9 + Math.random() * 0.2);
  }

  public async next(signal: AbortSignal): Promise<void> {
    const { instrumentation } = this;
    const { group, consumer, server } = this.options;

    for (const { name: stream } of this.streams.all()) {
      if (signal.aborted) {
        return;
      }

      try {
        await instrumentation.receive(
          {
            system: 'redis',
            operation: 'reclaim',
            name: stream,
            consumerGroup: group,
            clientId: consumer,
            server,
          },
          async (batch) => {
            batch.setSize(await this.reclaim(stream, signal, batch));
          },
        );
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

  private async reclaim(
    stream: string,
    signal: AbortSignal,
    batch: TelemetryBatch,
  ): Promise<number> {
    const { clients, streams, processor } = this;
    const { group, consumer, batchSize, retriesDelay } = this.options;
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

    // claiming counts as a delivery, so don't claim what will not run
    if (deliveries.size === 0 || signal.aborted) {
      return 0;
    }

    const binaryClient = await clients.getBinary('command');
    const claimed = (
      await binaryClient.xClaim(stream, group, consumer, retriesDelay, [
        ...deliveries.keys(),
      ])
    ).filter((entry) => entry !== null);

    for (const entry of claimed) {
      if (!signal.aborted) {
        const id = String(entry.id);
        const attempt = (deliveries.get(id) ?? 0) + 1;

        streams.run(stream, id, (current) =>
          processor.process(current, { ...entry, id }, attempt, batch),
        );
      }
    }

    return claimed.length;
  }
}
