import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { GenericContainer, Wait } from 'testcontainers';
import { Clients } from './clients.js';

describe('Clients', { timeout: 60_000 }, () => {
  let container: Awaited<ReturnType<GenericContainer['start']>>;
  let url: string;
  const created: Clients<any>[] = [];

  function create<K extends string = never>(): Clients<K> {
    const clients = new Clients<K>(url);

    created.push(clients);

    return clients;
  }

  beforeAll(async () => {
    container = await new GenericContainer('redis:8.2')
      .withExposedPorts(6379)
      .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
      .start();
    url = `redis://${container.getHost()}:${container.getMappedPort(6379)}`;
  }, 60_000);

  afterAll(async () => {
    await Promise.all(created.map((clients) => clients.close()));
    await container.stop();
  });

  describe('get', () => {
    it('returns a connected client', async () => {
      const clients = create();

      const client = await clients.get();

      expect(client.isReady).toBe(true);
    });

    it('returns the same client of a kind', async () => {
      const clients = create<'read' | 'command'>();

      const first = await clients.get('read');
      const second = await clients.get('read');

      expect(second).toBe(first);
    });

    it('returns a different client for every kind', async () => {
      const clients = create<'read' | 'command'>();

      const read = await clients.get('read');
      const command = await clients.get('command');

      expect(command).not.toBe(read);
    });
  });

  describe('destroy', () => {
    it('disconnects the client', async () => {
      const clients = create<'read' | 'command'>();
      const client = await clients.get('read');

      await clients.destroy('read');

      expect(client.isOpen).toBe(false);
    });

    it('rejects getting the client afterwards', async () => {
      const clients = create<'read' | 'command'>();
      await clients.get('read');

      await clients.destroy('read');

      await expect(clients.get('read')).rejects.toThrow(
        'Redis client read is closed',
      );
    });

    it('rejects getting a client that was never connected', async () => {
      const clients = create<'read' | 'command'>();

      await clients.destroy('read');

      await expect(clients.get('read')).rejects.toThrow(
        'Redis client read is closed',
      );
    });

    it('keeps the other kinds working', async () => {
      const clients = create<'read' | 'command'>();

      await clients.destroy('read');
      const command = await clients.get('command');

      expect(command.isReady).toBe(true);
    });
  });

  describe('close', () => {
    it('disconnects every client', async () => {
      const clients = create<'read' | 'command'>();
      const read = await clients.get('read');
      const command = await clients.get('command');

      await clients.close();

      expect(read.isOpen).toBe(false);
      expect(command.isOpen).toBe(false);
    });

    it('rejects getting any client afterwards', async () => {
      const clients = create<'read' | 'command'>();
      await clients.get('read');

      await clients.close();

      await expect(clients.get('read')).rejects.toThrow();
      await expect(clients.get('command')).rejects.toThrow();
    });
  });
});
