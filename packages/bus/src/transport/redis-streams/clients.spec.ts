import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
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
      .withWaitStrategy(
        Wait.forAll([
          Wait.forListeningPorts(),
          Wait.forLogMessage(/Ready to accept connections/),
        ]),
      )
      .start();
    url = `redis://${container.getHost()}:${container.getMappedPort(6379)}`;
  }, 60_000);

  afterAll(async () => {
    await container.stop();
  });

  afterEach(async () => {
    await Promise.all(created.splice(0).map((clients) => clients.destroy()));
  });

  describe('get', () => {
    it('returns a connected client', async () => {
      // arrange
      const clients = create();

      // act
      const client = await clients.get();

      // assert
      expect(client.isReady).toBe(true);
    });

    it('returns the same client of a kind', async () => {
      // arrange
      const clients = create<'read' | 'command'>();
      const first = await clients.get('read');

      // act
      const second = await clients.get('read');

      // assert
      expect(second).toBe(first);
    });

    it('returns a different client for every kind', async () => {
      // arrange
      const clients = create<'read' | 'command'>();
      const read = await clients.get('read');

      // act
      const command = await clients.get('command');

      // assert
      expect(command).not.toBe(read);
    });

    describe('with the binary option', () => {
      it('returns a client whose replies hold blob strings as buffers', async () => {
        // arrange
        const clients = create<'command'>();
        const key = `key:${crypto.randomUUID()}`;
        const bytes = Buffer.from([0xff, 0xfe, 0x00, 0x80]);
        const text = await clients.get('command');
        const binary = await clients.getBinary('command');
        await text.set(key, bytes);

        // act
        const value = await binary.get(key);

        // assert
        expect(value).toEqual(bytes);
      });

      it('leaves the replies of the default client as text', async () => {
        // arrange
        const clients = create<'command'>();
        const key = `key:${crypto.randomUUID()}`;
        const bytes = Buffer.from([0xff, 0xfe, 0x00, 0x80]);
        const text = await clients.get('command');
        await text.set(key, bytes);

        // act
        const value = await text.get(key);

        // assert
        expect(value).not.toEqual(bytes.toString('latin1'));
      });

      it('returns the same client every time, on the connection of the kind', async () => {
        // arrange
        const clients = create<'read' | 'command'>();
        const first = await clients.getBinary('read');

        // act
        const second = await clients.getBinary('read');

        // assert
        expect(second).toBe(first);
      });

      it('returns a client on the connection of the kind', async () => {
        // arrange
        const clients = create<'read' | 'command'>();

        // act
        const binary = await clients.getBinary('read');

        // assert
        expect(clients.id('read')).toBeDefined();
        expect(await binary.clientId()).toBe(clients.id('read'));
      });

      it('works without kinds', async () => {
        // arrange
        const clients = create();

        // act
        const binary = await clients.getBinary();

        // assert
        expect(binary.isReady).toBe(true);
      });
    });
  });

  describe('id', () => {
    it('is undefined before the client is connected', () => {
      // arrange
      const clients = create<'read'>();

      // act
      const id = clients.id('read');

      // assert
      expect(id).toBeUndefined();
    });

    it('is the id of the connection', async () => {
      // arrange
      const clients = create<'read' | 'command'>();
      const read = await clients.get('read');

      // act
      const id = clients.id('read');

      // assert
      expect(id).toBe(await read.clientId());
    });

    it('follows a reconnect', async () => {
      // arrange
      const clients = create<'read' | 'command'>();
      const command = await clients.get('command');
      const read = await clients.get('read');
      const before = clients.id('read');

      // act
      await command.sendCommand(['CLIENT', 'KILL', 'ID', String(before)]);
      await vi.waitFor(() => expect(read.isReady).toBe(true));
      await vi.waitFor(() => expect(clients.id('read')).not.toBe(before));

      // assert
      expect(clients.id('read')).toBe(await read.clientId());
    });

    it('is forgotten once the client is destroyed', async () => {
      // arrange
      const clients = create<'read'>();
      await clients.get('read');

      // act
      await clients.destroy('read');

      // assert
      expect(clients.id('read')).toBeUndefined();
    });
  });

  describe('destroy', () => {
    it('disconnects the client', async () => {
      // arrange
      const clients = create<'read' | 'command'>();
      const client = await clients.get('read');

      // act
      await clients.destroy('read');

      // assert
      expect(client.isOpen).toBe(false);
    });

    it('rejects getting the client afterwards', async () => {
      // arrange
      const clients = create<'read' | 'command'>();
      await clients.get('read');
      await clients.destroy('read');

      // act
      const getting = clients.get('read');

      // assert
      await expect(getting).rejects.toThrow('Redis client read is closed');
    });

    it('rejects getting a client that was never connected', async () => {
      // arrange
      const clients = create<'read' | 'command'>();
      await clients.destroy('read');

      // act
      const getting = clients.get('read');

      // assert
      await expect(getting).rejects.toThrow('Redis client read is closed');
    });

    it('keeps the other kinds working', async () => {
      // arrange
      const clients = create<'read' | 'command'>();

      await clients.destroy('read');

      // act
      const command = await clients.get('command');

      // assert
      expect(command.isReady).toBe(true);
    });
  });

  describe('destroy without a kind', () => {
    it('disconnects every client', async () => {
      // arrange
      const clients = create<'read' | 'command'>();
      const read = await clients.get('read');
      const command = await clients.get('command');

      // act
      await clients.destroy();

      // assert
      expect(read.isOpen).toBe(false);
      expect(command.isOpen).toBe(false);
    });

    it('rejects getting the default client afterwards', async () => {
      // arrange
      const clients = create();
      await clients.destroy();

      // act
      const getting = clients.get();

      // assert
      await expect(getting).rejects.toThrow();
    });

    it('rejects getting any client afterwards', async () => {
      // arrange
      const clients = create<'read' | 'command'>();
      await clients.get('read');
      await clients.get('command');
      await clients.destroy();

      // act
      const gettingRead = clients.get('read');
      const gettingCommand = clients.get('command');

      // assert
      await expect(gettingRead).rejects.toThrow();
      await expect(gettingCommand).rejects.toThrow();
    });
  });
});
