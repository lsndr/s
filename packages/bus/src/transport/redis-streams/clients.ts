import { Logger } from '@nestjs/common';
import { type RESP_TYPES, type RedisClientType } from 'redis';

const DEFAULT_KIND = 'default';

function toBinary(client: RedisClientType, blobString: 36) {
  return client.withTypeMapping({ [blobString]: Buffer });
}

type BinaryClient = ReturnType<typeof toBinary>;

/**
 * One client per kind, so that a blocking command doesn't hold up the others.
 * A client that was destroyed is not connected again: getting it rejects, so
 * nothing opens a connection that nobody is left to close.
 */
export class Clients<K extends string = never> {
  private readonly clients = new Map<string, Promise<RedisClientType>>();
  private readonly destroyed = new Set<string>();
  private readonly ids = new Map<string, number>();
  private readonly logger = new Logger(Clients.name);
  private readonly binaries = new WeakMap<RedisClientType, BinaryClient>();
  private respTypes?: typeof RESP_TYPES;

  public constructor(private readonly url: string) {}

  /** @throws If the client was destroyed. */
  public get(kind?: K): Promise<RedisClientType> {
    const key = kind ?? DEFAULT_KIND;

    if (this.destroyed.has(key)) {
      return Promise.reject(new Error(`Redis client ${key} is closed`));
    }

    let client = this.clients.get(key);

    if (!client) {
      client = this.connect(key).catch((err) => {
        // let the next call connect again
        this.clients.delete(key);

        throw err;
      });
      this.clients.set(key, client);
    }

    return client;
  }

  /**
   * Like {@link Clients.get}, but replies hold blob strings as buffers, so that
   * binary data survives.
   *
   * @throws If the client was destroyed.
   */
  public async getBinary(kind?: K): Promise<BinaryClient> {
    const client = await this.get(kind);
    let binary = this.binaries.get(client);

    if (!binary) {
      binary = toBinary(client, this.respTypes!.BLOB_STRING);
      this.binaries.set(client, binary);
    }

    return binary;
  }

  /** @returns `undefined` while the client is not connected. */
  public id(kind?: K): number | undefined {
    return this.ids.get(kind ?? DEFAULT_KIND);
  }

  /**
   * Drops the connections right away, which interrupts a command that is
   * blocked on one. Everything is destroyed when `kind` is left out.
   */
  public async destroy(kind?: K): Promise<void> {
    // without a kind, the ones that were connected, and the default one that
    // a `Clients` without kinds uses
    const kinds =
      kind === undefined
        ? [...new Set([...this.clients.keys(), DEFAULT_KIND])]
        : [kind];

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

  private async connect(kind: string): Promise<RedisClientType> {
    const { createClient, RESP_TYPES } = await import('redis');

    this.respTypes = RESP_TYPES;
    const client = createClient({ url: this.url });
    let identified: Promise<void> = Promise.resolve();

    client.on('error', (err: unknown) => {
      this.logger.error(
        `Redis connection error: ${err instanceof Error ? err.message : String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    });

    // ready on the first connection and after every reconnect, and the id
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
