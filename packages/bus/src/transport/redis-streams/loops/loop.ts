/** A background job that {@link Loops} runs again and again until aborted. */
export interface Loop {
  /**
   * Waits this many milliseconds before every iteration. Without it,
   * iterations follow each other right away, so an iteration is expected to
   * wait for something itself.
   */
  interval?(): number;
  /**
   * One round of the loop.
   *
   * @param signal - Aborted when the loops are told to stop.
   */
  next(signal: AbortSignal): Promise<void>;
}
