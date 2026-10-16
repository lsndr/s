import { type EventPayload } from '../../types.js';

export type Callback = (event: EventPayload<any>) => void | Promise<void>;

/** A named callback of a stream. The name is what is recorded once it succeeds. */
export class Handler {
  public constructor(
    public readonly name: string,
    public readonly callback: Callback,
  ) {}
}
