import { createHash } from 'node:crypto';
import { Logger } from '@nestjs/common';
import { type RedisClientType } from 'redis';
import { type Clients } from '../clients.js';
import { type Streams } from '../streams.js';
import { type Loop } from './loop.js';

/**
 * Deletes the consumers of a group that have no pending entries and have been
 * idle for longer than the given time.
 *
 * KEYS[1] - stream, ARGV[1] - group, ARGV[2] - consumer to keep, ARGV[3] -
 * idle time in milliseconds a consumer has to exceed.
 *
 * Returns the names of the deleted consumers.
 */
const DELETE_STALE_CONSUMERS = `
local deleted = {}

for _, info in ipairs(redis.call('XINFO', 'CONSUMERS', KEYS[1], ARGV[1])) do
  local fields = {}

  for i = 1, #info, 2 do
    fields[info[i]] = info[i + 1]
  end

  if fields.name ~= ARGV[2]
    and fields.pending == 0
    and fields.idle > tonumber(ARGV[3]) then
    redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], fields.name)
    table.insert(deleted, fields.name)
  end
end

return deleted
`;

const DELETE_STALE_CONSUMERS_SHA = createHash('sha1')
  .update(DELETE_STALE_CONSUMERS)
  .digest('hex');

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

  public async next(signal: AbortSignal): Promise<void> {
    const { clients, streams } = this;
    const { group } = this.options;

    for (const { name: stream } of streams.all()) {
      if (signal.aborted) {
        return;
      }

      try {
        const client = await clients.get('command');
        const deleted = await this.deleteStale(client, stream);

        for (const name of deleted) {
          this.logger.log(`Removed stale consumer ${name} of group ${group}`);
        }
      } catch (err) {
        if (signal.aborted) {
          return;
        }

        this.logger.error(
          `Failed to delete stale consumers of ${stream}: ${String(err)}`,
          err instanceof Error ? err.stack : undefined,
        );
      }
    }
  }

  /**
   * Runs the script for a stream.
   *
   * @returns Names of the deleted consumers.
   */
  private async deleteStale(
    client: RedisClientType,
    stream: string,
  ): Promise<string[]> {
    const { group, consumer, consumerTtl } = this.options;
    const options = {
      keys: [stream],
      arguments: [group, consumer, String(consumerTtl)],
    };

    try {
      return (await client.evalSha(
        DELETE_STALE_CONSUMERS_SHA,
        options,
      )) as string[];
    } catch (err) {
      if (!(err instanceof Error) || !err.message.startsWith('NOSCRIPT')) {
        throw err;
      }

      // Not cached by the server yet, EVAL caches it for the next call
      return (await client.eval(DELETE_STALE_CONSUMERS, options)) as string[];
    }
  }
}
