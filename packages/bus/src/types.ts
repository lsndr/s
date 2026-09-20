import { type Type } from '@nestjs/common';

export interface EventPayload<E extends object> {
  id: string;
  event: E;
  meta: Record<string, string | number | undefined | null>;
  publishedAt: Date;
}

export type EventConstructor<E extends object> = new (...args: any) => E;

export interface EventHandler<E extends object> {
  handle(event: EventPayload<E>): Promise<void> | void;
}

export type EventHandlers = Type<EventHandler<object>>[];

export const EventHandlers = Symbol('EventHandlers');

export interface BusRetriesConfig {
  max: number;
  delay: number;
}

export interface BusConfig {
  retries: BusRetriesConfig;
  group: string;
}

export const BusConfig = Symbol('BusConfig');
