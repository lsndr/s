# Serialization

A serializer turns an event, together with its envelope, into the message a transport sends, and back into an event when the message is received. The format is up to the serializer.

## JsonSerializer

`JsonSerializer` is the default. It writes the message as JSON text and converts events with [`class-transformer`](https://github.com/typestack/class-transformer), which has to be installed.

## Custom serializer

Provide your own with the `serializer` option, in any form accepted by [`transport`](transports.md#providing-a-transport):

```typescript
import { Injectable } from '@nestjs/common';
import { pack, unpack } from 'msgpackr';
import { randomUUID } from 'node:crypto';
import {
  BusModule,
  type EventPayload,
  type PublishOptions,
  type Serializer,
  type SerializedData,
  type SerializedMessage,
} from '@lsndr/bus';

@Injectable()
class MsgpackSerializer implements Serializer {
  serialize(event: object, options?: PublishOptions): SerializedMessage {
    const id = options?.id ?? randomUUID();
    const name = options?.name ?? event.constructor.name;
    const publishedAt = options?.publishedAt ?? new Date();

    return {
      id,
      name,
      publishedAt,
      headers: { ...options?.headers },
      data: pack({ id, name, event, meta: options?.meta ?? {}, publishedAt }),
    };
  }

  deserialize<E extends object>(
    event: new (...args: any) => E,
    data: SerializedData,
  ): EventPayload<E> {
    const message = unpack(Buffer.from(data));

    return {
      ...message,
      event: Object.assign(Object.create(event.prototype), message.event),
    };
  }
}

BusModule.forRoot({ transport, serializer: MsgpackSerializer });
```
