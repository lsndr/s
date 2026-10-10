/**
 * Lets a loop sleep until another part of the app has something for it.
 *
 * A notification that arrives while nobody waits is dropped, so the waiter
 * has to check its condition right before it calls {@link Signal.wait}.
 */
export class Signal {
  private resolve: (() => void) | undefined;

  /** Resolves on the next {@link Signal.notify}. */
  public wait(): Promise<void> {
    return new Promise((resolve) => {
      this.resolve = resolve;
    });
  }

  /** Wakes the waiter, if there is one. */
  public notify(): void {
    const resolve = this.resolve;

    this.resolve = undefined;
    resolve?.();
  }
}
