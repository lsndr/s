import type * as ClassTransformer from 'class-transformer';
import { v7 as uuidv7 } from 'uuid';
import { type EventConstructor, type EventPayload } from '../types.js';
import { type OnBusInit } from '../lifecycle.js';
import { type PublishOptions } from '../transport/dispatcher.js';
import {
  type SerializedData,
  type SerializedMessage,
  type Serializer,
} from './serializer.js';

/** Default serializer, which encodes events as JSON and needs `class-transformer` to be installed. */
export class JsonSerializer implements Serializer, OnBusInit {
  private classTransformerModule?: typeof ClassTransformer;

  public async onBusInit(): Promise<void> {
    this.classTransformerModule = await import('class-transformer');
  }

  public serialize(event: object, options?: PublishOptions): SerializedMessage {
    const { instanceToPlain } = this.classTransformer();
    const id = options?.id ?? uuidv7();
    const name = options?.name ?? event.constructor.name;
    const publishedAt = options?.publishedAt ?? new Date();

    return {
      id,
      name,
      publishedAt,
      data: JSON.stringify({
        id,
        name,
        event: instanceToPlain(event),
        meta: options?.meta ?? {},
        publishedAt,
      }),
    };
  }

  public deserialize<E extends object>(
    event: EventConstructor<E>,
    data: SerializedData,
  ): EventPayload<E> {
    const { plainToInstance } = this.classTransformer();
    const dto = JSON.parse(data.toString()) as Omit<
      EventPayload<E>,
      'event'
    > & {
      name: string;
      event: Record<string, unknown>;
    };

    return {
      ...dto,
      event: plainToInstance(event, dto.event, {
        enableImplicitConversion: true,
      }),
      publishedAt: new Date(dto.publishedAt),
    };
  }

  private classTransformer(): typeof ClassTransformer {
    if (!this.classTransformerModule) {
      throw new Error(
        'JsonSerializer is not initialized, call onBusInit first',
      );
    }

    return this.classTransformerModule;
  }
}
