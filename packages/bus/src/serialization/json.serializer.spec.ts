import 'reflect-metadata';
import { Type } from 'class-transformer';
import { describe, expect, it } from 'vitest';
import { JsonSerializer } from './json.serializer.js';

class TestEvent {
  public value = 'a';
  @Type(() => Date)
  public createdAt = new Date('2026-01-02T03:04:05.000Z');
}

describe('JsonSerializer', () => {
  it('fails until it is initialized', () => {
    const serializer = new JsonSerializer();

    expect(() => serializer.serialize(new TestEvent())).toThrow(
      'not initialized',
    );
  });

  describe('once initialized', () => {
    async function setup() {
      const serializer = new JsonSerializer();

      await serializer.onBusInit();

      return serializer;
    }

    it('writes the envelope and the plain event as JSON', async () => {
      const serializer = await setup();
      const publishedAt = new Date('2026-02-03T04:05:06.000Z');
      const message = serializer.serialize(new TestEvent(), {
        id: 'id-1',
        meta: { key: 'value' },
        publishedAt,
      });

      expect(message).toMatchObject({
        id: 'id-1',
        name: 'TestEvent',
        publishedAt,
      });
      expect(JSON.parse(String(message.data))).toEqual({
        id: 'id-1',
        name: 'TestEvent',
        event: { value: 'a', createdAt: '2026-01-02T03:04:05.000Z' },
        meta: { key: 'value' },
        publishedAt: publishedAt.toISOString(),
      });
    });

    it('produces a string, and reads a string or a buffer', async () => {
      const serializer = await setup();
      const { data } = serializer.serialize(new TestEvent(), { id: 'id-1' });

      expect(typeof data).toBe('string');
      expect(serializer.deserialize(TestEvent, data).id).toBe('id-1');
      expect(serializer.deserialize(TestEvent, Buffer.from(data)).id).toBe(
        'id-1',
      );
    });

    it('restores the event instance and the dates', async () => {
      const serializer = await setup();
      const message = serializer.serialize(new TestEvent(), { id: 'id-1' });
      const payload = serializer.deserialize(TestEvent, message.data);

      expect(payload.event).toBeInstanceOf(TestEvent);
      expect(payload).toMatchObject({
        id: 'id-1',
        meta: {},
        publishedAt: message.publishedAt,
      });
      expect(payload.event.createdAt).toEqual(new TestEvent().createdAt);
    });

    it('fills in what the options leave out', async () => {
      const serializer = await setup();
      const { id, name, publishedAt, data } = serializer.serialize(
        new TestEvent(),
      );

      expect(id).toEqual(expect.any(String));
      expect(name).toBe('TestEvent');
      expect(publishedAt).toBeInstanceOf(Date);
      expect(serializer.deserialize(TestEvent, data)).toMatchObject({
        id,
        meta: {},
        publishedAt,
      });
    });

    it('rejects data that is not JSON', async () => {
      const serializer = await setup();

      expect(() =>
        serializer.deserialize(TestEvent, Buffer.from('nope')),
      ).toThrow();
    });
  });
});
