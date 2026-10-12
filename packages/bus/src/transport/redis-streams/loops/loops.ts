import { setTimeout as sleep } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { type Loop } from './loop.js';

/** How long a loop waits after a failed iteration, in milliseconds. */
const FAILURE_DELAY = 1_000;

/**
 * Runs background loops until they are aborted.
 *
 * An iteration that throws is logged and the loop carries on, so a loop only
 * ends when {@link Loops.abort} is called.
 */
export class Loops {
  private readonly controller = new AbortController();
  private readonly running: Promise<void>[] = [];
  private readonly logger = new Logger(Loops.name);

  public constructor(private readonly loops: Loop[]) {}

  /** Starts the loops. Does nothing when they are started already. */
  public start(): void {
    if (this.running.length > 0) {
      return;
    }

    for (const loop of this.loops) {
      this.running.push(this.run(loop));
    }
  }

  /** Tells the loops to stop. Iterations that are running are not interrupted. */
  public abort(): void {
    this.controller.abort();
  }

  /** Resolves once all loops have ended. */
  public async wait(): Promise<void> {
    await Promise.allSettled(this.running);
  }

  private async run(loop: Loop): Promise<void> {
    const { signal } = this.controller;

    while (!signal.aborted) {
      if (loop.interval) {
        await this.pause(loop.interval());

        if (signal.aborted) {
          return;
        }
      }

      try {
        await loop.next(signal);
      } catch (err) {
        if (signal.aborted) {
          return;
        }

        this.logger.error(
          `${loop.constructor.name} failed: ${String(err)}`,
          err instanceof Error ? err.stack : undefined,
        );

        // Keeps a loop without an interval from spinning on a failure
        await this.pause(FAILURE_DELAY);
      }
    }
  }

  private async pause(ms: number): Promise<void> {
    try {
      await sleep(ms, undefined, { signal: this.controller.signal });
    } catch {
      // Aborted, which is how the loops are woken up
    }
  }
}
