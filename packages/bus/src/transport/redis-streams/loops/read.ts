import { type Clients } from '../clients.js';
import { type Processor } from '../processor.js';
import { type Streams } from '../streams.js';
import { type Loop } from './loop.js';

export interface ReadLoopOptions {
  group: string;
  consumer: string;
  /** Maximum number of entries read from a stream at once. */
  batchSize: number;
  /** How long a read blocks waiting for new entries, in milliseconds. */
  blockTimeout: number;
}

/** Reads the streams that have a free slot, and starts the entries. */
export class ReadLoop implements Loop {
  public constructor(
    private readonly clients: Clients<'read'>,
    private readonly streams: Streams,
    private readonly processor: Processor,
    private readonly options: ReadLoopOptions,
  ) {}

  public async next(signal: AbortSignal): Promise<void> {
    const { clients, streams, processor } = this;
    const { group, consumer, batchSize, blockTimeout } = this.options;

    // Waits for a free slot instead of calling Redis when all are full
    const readable = await streams.readable(signal);

    if (readable.length === 0) {
      return;
    }

    const client = await clients.get('read');
    const reply =
      (await client.xReadGroup(
        group,
        consumer,
        readable.map((key) => ({ key, id: '>' })),
        { COUNT: batchSize, BLOCK: blockTimeout },
      )) ?? [];

    for (const { name, messages } of reply) {
      for (const entry of messages) {
        streams.run(String(name), entry.id, (stream) =>
          processor.process(stream, entry, 1),
        );
      }
    }
  }
}
