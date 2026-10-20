import { instanceToPlain, plainToInstance } from 'class-transformer';
import { v7 as uuidv7 } from 'uuid';
import { type EventConstructor, type EventPayload } from '../types.js';
import { type PublishOptions } from './dispatcher.js';

/** Wire format of an event, shared by all transports. */
export interface EventDto {
  id: string;
  name: string;
  event: Record<string, string | number | undefined | null>;
  meta: Record<string, string | number | undefined | null>;
  publishedAt: Date;
}

/**
 * Serializes an event to its wire format.
 *
 * @param event - Event instance. Its class name becomes the event name.
 * @param options - Optional event id, name, meta and publication time.
 * @returns The DTO, with a UUID v7 id unless `options.id` is set.
 */
export function createEventDto<E extends object>(
  event: E,
  options?: PublishOptions,
): EventDto {
  return {
    id: options?.id ?? uuidv7(),
    name: options?.name ?? event.constructor.name,
    event: instanceToPlain(event),
    meta: options?.meta ?? {},
    publishedAt: options?.publishedAt ?? new Date(),
  };
}

/**
 * Deserializes the wire format into an event payload.
 *
 * @param eventConstructor - Class to instantiate the event with.
 * @param content - JSON-encoded {@link EventDto}.
 * @returns The payload holding an instance of `eventConstructor`.
 * @throws If `content` is not valid JSON.
 */
export function parseEventPayload<E extends object>(
  eventConstructor: EventConstructor<E>,
  content: string,
): EventPayload<E> {
  const dto = JSON.parse(content) as EventDto;

  return {
    ...dto,
    event: plainToInstance(eventConstructor, dto.event, {
      enableImplicitConversion: true,
    }),
    publishedAt: new Date(dto.publishedAt),
  };
}
