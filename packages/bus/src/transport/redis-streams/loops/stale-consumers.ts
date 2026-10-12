import { Logger } from '@nestjs/common';
import { type Clients } from '../clients.js';
import { type Streams } from '../streams.js';
import { type Loop } from './loop.js';

export interface StaleConsumersLoopOptions {
  group: string;
  consumer: string;
  /** How long a consumer can stay idle before it is deleted, in milliseconds. */
  consumerTtl: number;
}

/**
 * Deletes the other consumers of the groups that have no pending entries and
 * have been idle for longer than `consumerTtl`.
 */
export class StaleConsumersLoop implements Loop {
  private readonly logger = new Logger(StaleConsumersLoop.name);

  public constructor(
    private readonly clients: Clients<'command'>,
    private readonly streams: Streams,
    private readonly options: StaleConsumersLoopOptions,
  ) {}

  public interval(): number {
    return this.options.consumerTtl / 2;
  }

  public async next(): Promise<void> {
    const { clients, streams } = this;
    const { group, consumer, consumerTtl } = this.options;
    const client = await clients.get('command');

    for (const { name: stream } of streams.all()) {
      const consumers = await client.xInfoConsumers(stream, group);

      for (const { name, pending, idle } of consumers) {
        if (name === consumer || pending > 0 || idle <= consumerTtl) {
          continue;
        }

        await client.xGroupDelConsumer(stream, group, name);

        this.logger.log(`Removed stale consumer ${name} of group ${group}`);
      }
    }
  }
}
