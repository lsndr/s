/** Holds tasks back until `release`, and runs the ones added after it right away. */
export class DeferredTasks {
  private readonly tasks: (() => Promise<void>)[] = [];
  private released = false;

  public async run(task: () => Promise<void>): Promise<void> {
    if (this.released) {
      await task();
    } else {
      this.tasks.push(task);
    }
  }

  /** Runs the held tasks one after another. */
  public async release(): Promise<void> {
    this.released = true;

    for (const task of this.tasks.splice(0)) {
      await task();
    }
  }
}
