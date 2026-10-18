import { type EventCallback } from '../subscriber.js';

export type Callback = EventCallback<any>;

/** A named callback of a stream. The name is what is recorded once it succeeds. */
export class Handler {
  public constructor(
    public readonly name: string,
    public readonly callback: Callback,
  ) {}
}
