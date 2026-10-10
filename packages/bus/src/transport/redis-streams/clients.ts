import { Logger } from '@nestjs/common';
import { type RedisClientType } from 'redis';

const DEFAULT_KIND = 'default';

/** The kind is required when `Clients` is given kinds, and not allowed otherwise. */
type KindArgs<K extends string> = [K] extends [never] ? [] : [kind: K];

/**
 * Lazily connected Redis clients.
 *
 * Without kinds there is a single client. With kinds, e.g.
 * `Clients<'read' | 'command'>`, there is one client per kind, so that a
 * blocking command doesn't hold up the others.
 */
export class Clients<K extends string = never> {
  private readonly clients = new Map<string, Promise<RedisClientType>>();
  private readonly logger = new Logger(Clients.name);

  public constructor(private readonly url: string) {}

  /** Returns the client of a kind, connecting it on the first request. */
  public get(...args: KindArgs<K>): Promise<RedisClientType> {
    const kind = this.keyOf(args);
    let client = this.clients.get(kind);

    if (!client) {
      client = this.connect().catch((err) => {
        // Let the next call connect again
        this.clients.delete(kind);

        throw err;
      });
      this.clients.set(kind, client);
    }

    return client;
  }

  /**
   * Drops the connection of a kind right away, which interrupts a command
   * that is blocked on it.
   */
  public async destroy(...args: KindArgs<K>): Promise<void> {
    const kind = this.keyOf(args);
    const client = this.clients.get(kind);

    this.clients.delete(kind);

    await client?.then(
      (connected) => connected.destroy(),
      () => undefined,
    );
  }

  /** Closes all connections after their pending commands are done. */
  public async close(): Promise<void> {
    const clients = [...this.clients.values()];

    this.clients.clear();

    await Promise.all(
      clients.map((client) =>
        client.then(
          (connected) => connected.close(),
          () => undefined,
        ),
      ),
    );
  }

  private keyOf(args: KindArgs<K>): string {
    return args[0] ?? DEFAULT_KIND;
  }

  private async connect(): Promise<RedisClientType> {
    const { createClient } = await import('redis');
    const client = createClient({ url: this.url });

    client.on('error', (err: unknown) => {
      this.logger.error(
        `Redis connection error: ${err instanceof Error ? err.message : String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    });

    await client.connect();

    return client;
  }
}
