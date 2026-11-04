/**
 * Lets loops sleep until another part of the app has something for them.
 *
 * A notification that arrives while nobody waits is dropped, so a waiter
 * has to check its condition right before it calls {@link Signal.wait}.
 */
export class Signal {
  private waiters = new Set<() => void>();

  public wait(): Promise<void> {
    return new Promise((resolve) => {
      this.waiters.add(resolve);
    });
  }

  public notify(): void {
    const waiters = this.waiters;

    this.waiters = new Set();

    for (const resolve of waiters) {
      resolve();
    }
  }
}
