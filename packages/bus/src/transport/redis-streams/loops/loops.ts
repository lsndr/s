import { setTimeout as sleep } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { type OnTransportShutdown } from '../lifecycle.js';
import { type Loop } from './loop.js';

/** How long a loop waits after a failed iteration, in milliseconds. */
const FAILURE_DELAY = 1_000;

/**
 * An iteration that throws is logged and the loop carries on, so a loop only
 * ends when the loops are shut down.
 */
export class Loops implements OnTransportShutdown {
  private readonly controller = new AbortController();
  private readonly running: Promise<void>[] = [];
  private readonly logger = new Logger(Loops.name);

  public constructor(private readonly loops: Loop[]) {}

  public start(): void {
    if (this.running.length > 0) {
      return;
    }

    for (const loop of this.loops) {
      this.running.push(this.run(loop));
    }
  }

  public async onTransportShutdown(): Promise<void> {
    this.controller.abort();

    await Promise.all([
      Promise.allSettled(this.running),
      ...this.loops.map(async (loop) => {
        try {
          await loop.onTransportShutdown?.();
        } catch (err) {
          this.logger.error(
            `${loop.constructor.name} failed to shut down: ${String(err)}`,
            err instanceof Error ? err.stack : undefined,
          );
        }
      }),
    ]);
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

        // keeps a loop without an interval from spinning on a failure
        await this.pause(FAILURE_DELAY);
      }
    }
  }

  private async pause(ms: number): Promise<void> {
    try {
      await sleep(ms, undefined, { signal: this.controller.signal });
    } catch {
      // aborted, which is how the loops are woken up
    }
  }
}
