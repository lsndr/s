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
 *
 * A client that was destroyed is not connected again: getting it
 * rejects, so nothing opens a connection that nobody is left to close.
 */
export class Clients<K extends string = never> {
  private readonly clients = new Map<string, Promise<RedisClientType>>();
  private readonly destroyed = new Set<string>();
  private readonly ids = new Map<string, number>();
  private readonly logger = new Logger(Clients.name);

  public constructor(private readonly url: string) {}

  /**
   * Returns the client of a kind, connecting it on the first request.
   *
   * @throws If the client was destroyed.
   */
  public get(...args: KindArgs<K>): Promise<RedisClientType> {
    const kind = this.keyOf(args);

    if (this.destroyed.has(kind)) {
      return Promise.reject(new Error(`Redis client ${kind} is closed`));
    }

    let client = this.clients.get(kind);

    if (!client) {
      client = this.connect(kind).catch((err) => {
        // Let the next call connect again
        this.clients.delete(kind);

        throw err;
      });
      this.clients.set(kind, client);
    }

    return client;
  }

  /**
   * The id of the connection of a kind (`CLIENT ID`). It follows reconnects.
   *
   * @returns `undefined` while the client is not connected.
   */
  public id(...args: KindArgs<K>): number | undefined {
    return this.ids.get(this.keyOf(args));
  }

  /**
   * Drops the connections right away, which interrupts a command that is
   * blocked on one. The clients can't be got again.
   *
   * @param args - The kind to destroy. Everything is destroyed without it.
   */
  public async destroy(...args: [] | KindArgs<K>): Promise<void> {
    // Without a kind, the ones that were connected, and the default one that
    // a `Clients` without kinds uses
    const kinds =
      args.length === 0
        ? [...new Set([...this.clients.keys(), DEFAULT_KIND])]
        : [this.keyOf(args)];

    const clients = kinds.flatMap((kind) => {
      const client = this.clients.get(kind);

      this.destroyed.add(kind);
      this.clients.delete(kind);
      this.ids.delete(kind);

      return client ? [client] : [];
    });

    await Promise.all(
      clients.map((client) =>
        client.then(
          (connected) => connected.destroy(),
          () => undefined,
        ),
      ),
    );
  }

  private keyOf(args: KindArgs<K>): string {
    return args[0] ?? DEFAULT_KIND;
  }

  private async connect(kind: string): Promise<RedisClientType> {
    const { createClient } = await import('redis');
    const client = createClient({ url: this.url });
    let identified: Promise<void> = Promise.resolve();

    client.on('error', (err: unknown) => {
      this.logger.error(
        `Redis connection error: ${err instanceof Error ? err.message : String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    });

    // Ready on the first connection and after every reconnect, and the id
    // changes with each connection
    client.on('ready', () => {
      identified = client.clientId().then(
        (id) => {
          if (!this.destroyed.has(kind)) {
            this.ids.set(kind, id);
          }
        },
        (err: unknown) => {
          this.logger.warn(
            `Failed to get the id of Redis client ${kind}: ${String(err)}`,
          );
        },
      );
    });

    await client.connect();
    await identified;

    return client;
  }
}
