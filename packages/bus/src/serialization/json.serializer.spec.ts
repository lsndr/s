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
    // arrange
    const serializer = new JsonSerializer();

    // act
    const serialize = () => serializer.serialize(new TestEvent());

    // assert
    expect(serialize).toThrow('not initialized');
  });

  describe('once initialized', () => {
    async function setup() {
      const serializer = new JsonSerializer();

      await serializer.onBusInit();

      return serializer;
    }

    it('writes the envelope and the plain event as JSON', async () => {
      // arrange
      const serializer = await setup();
      const publishedAt = new Date('2026-02-03T04:05:06.000Z');

      // act
      const message = serializer.serialize(new TestEvent(), {
        id: 'id-1',
        meta: { key: 'value' },
        publishedAt,
      });

      // assert
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

    it('produces a string', async () => {
      // arrange
      const serializer = await setup();

      // act
      const { data } = serializer.serialize(new TestEvent(), { id: 'id-1' });

      // assert
      expect(typeof data).toBe('string');
    });

    it('reads a string', async () => {
      // arrange
      const serializer = await setup();
      const { data } = serializer.serialize(new TestEvent(), { id: 'id-1' });

      // act
      const payload = serializer.deserialize(TestEvent, data);

      // assert
      expect(payload.id).toBe('id-1');
    });

    it('reads a buffer', async () => {
      // arrange
      const serializer = await setup();
      const { data } = serializer.serialize(new TestEvent(), { id: 'id-1' });

      // act
      const payload = serializer.deserialize(TestEvent, Buffer.from(data));

      // assert
      expect(payload.id).toBe('id-1');
    });

    it('restores the event instance and the dates', async () => {
      // arrange
      const serializer = await setup();
      const message = serializer.serialize(new TestEvent(), { id: 'id-1' });

      // act
      const payload = serializer.deserialize(TestEvent, message.data);

      // assert
      expect(payload.event).toBeInstanceOf(TestEvent);
      expect(payload).toMatchObject({
        id: 'id-1',
        meta: {},
        publishedAt: message.publishedAt,
      });
      expect(payload.event.createdAt).toEqual(new TestEvent().createdAt);
    });

    it('fills in what the options leave out', async () => {
      // arrange
      const serializer = await setup();

      // act
      const { id, name, publishedAt } = serializer.serialize(new TestEvent());

      // assert
      expect(id).toEqual(expect.any(String));
      expect(name).toBe('TestEvent');
      expect(publishedAt).toBeInstanceOf(Date);
    });

    it('reads back what the options left out', async () => {
      // arrange
      const serializer = await setup();
      const { id, publishedAt, data } = serializer.serialize(new TestEvent());

      // act
      const payload = serializer.deserialize(TestEvent, data);

      // assert
      expect(payload).toMatchObject({ id, meta: {}, publishedAt });
    });

    it('rejects data that is not JSON', async () => {
      // arrange
      const serializer = await setup();

      // act
      const deserialize = () =>
        serializer.deserialize(TestEvent, Buffer.from('nope'));

      // assert
      expect(deserialize).toThrow();
    });
  });
});
