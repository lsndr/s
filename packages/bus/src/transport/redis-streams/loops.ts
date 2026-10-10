import { setTimeout as sleep } from 'node:timers/promises';
import { Logger } from '@nestjs/common';

export interface LoopOptions {
  /**
   * Waits this many milliseconds before every iteration. Without it,
   * iterations follow each other right away, so an iteration is expected to
   * wait for something itself.
   */
  interval?: () => number;
  /** Waits this many milliseconds after a failed iteration. */
  retryDelay?: number;
}

/**
 * Background loops that run until they are aborted.
 *
 * An iteration that throws is logged and the loop carries on, so a loop only
 * ends when {@link Loops.abort} is called.
 */
export class Loops {
  private readonly controller = new AbortController();
  private readonly loops: Promise<void>[] = [];
  private readonly logger = new Logger(Loops.name);

  /** Aborted when the loops are told to stop. */
  public get signal(): AbortSignal {
    return this.controller.signal;
  }

  /** Whether any loop was started. */
  public get running(): boolean {
    return this.loops.length > 0;
  }

  /**
   * Starts a loop.
   *
   * @param name - Describes what the loop does, for the log of a failure.
   * @param iteration - One round of the loop.
   */
  public start(
    name: string,
    iteration: () => Promise<void>,
    options: LoopOptions = {},
  ): void {
    this.loops.push(this.run(name, iteration, options));
  }

  /** Tells the loops to stop. Iterations that are running are not interrupted. */
  public abort(): void {
    this.controller.abort();
  }

  /** Resolves once all loops have ended. */
  public async wait(): Promise<void> {
    await Promise.allSettled(this.loops);
  }

  private async run(
    name: string,
    iteration: () => Promise<void>,
    { interval, retryDelay }: LoopOptions,
  ): Promise<void> {
    const { signal } = this.controller;

    while (!signal.aborted) {
      if (interval) {
        await this.pause(interval());

        if (signal.aborted) {
          return;
        }
      }

      try {
        await iteration();
      } catch (err) {
        if (signal.aborted) {
          return;
        }

        this.logger.error(
          `Failed to ${name}: ${String(err)}`,
          err instanceof Error ? err.stack : undefined,
        );

        if (retryDelay) {
          await this.pause(retryDelay);
        }
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
