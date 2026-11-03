import { type EventConstructor, type EventPayload } from '../types.js';
import { type PublishOptions } from '../transport/dispatcher.js';

export type SerializedData = Buffer | string;

export interface SerializedMessage {
  id: string;
  /** Event name, which transports route by. */
  name: string;
  publishedAt: Date;
  data: SerializedData;
}

/**
 * Covers the whole message, the envelope included, so the format is up to the
 * implementation.
 */
export interface Serializer {
  /**
   * Encodes an event together with its envelope. What `options` leaves out is
   * filled in: a UUID v7 `id`, the class name of the event as `name`, empty
   * `meta` and the current time as `publishedAt`. A string `data` is sent as
   * its UTF-8 bytes, so both kinds come out the same on the wire.
   *
   * @returns The `data`, and the `id`, `name` and `publishedAt` it carries.
   */
  serialize(event: object, options?: PublishOptions): SerializedMessage;

  /**
   * @param data - What {@link Serializer.serialize} returned. Transports pass
   * the bytes they received, as a `Buffer`, and the outbox passes what it
   * stored, which is a `string` if the serializer produced one.
   * @throws If `data` can't be decoded.
   */
  deserialize<E extends object>(
    event: EventConstructor<E>,
    data: SerializedData,
  ): EventPayload<E>;
}

export const Serializer = Symbol('Serializer');
