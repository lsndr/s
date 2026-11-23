import { type EventPayload } from '../types.js';

export type Callback = (event: EventPayload<any>) => void | Promise<void>;

export class Handler {
  private readonly callbacks: Callback[];

  public constructor(
    public readonly name: string,
    callback: Callback,
  ) {
    this.callbacks = [callback];
  }

  public merge(other: Handler): void {
    this.callbacks.push(...other.callbacks);
  }

  public async handle(event: EventPayload<any>): Promise<void> {
    const results = await Promise.allSettled(
      // async, so that a synchronous throw is a rejection
      this.callbacks.map(async (callback) => callback(event)),
    );
    const errors = results
      .filter((result) => result.status === 'rejected')
      .map(({ reason }) => reason as unknown);

    if (errors.length === 1) {
      throw errors[0];
    }

    if (errors.length > 1) {
      throw new AggregateError(
        errors,
        `${errors.length} of ${results.length} handlers failed`,
      );
    }
  }
}
