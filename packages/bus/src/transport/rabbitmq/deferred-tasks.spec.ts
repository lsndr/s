import { describe, expect, it, vi } from 'vitest';
import { DeferredTasks } from './deferred-tasks.js';

describe('DeferredTasks', () => {
  it('holds a task back until release', async () => {
    // arrange
    const task = vi.fn().mockResolvedValue(undefined);
    const tasks = new DeferredTasks();

    // act
    await tasks.run(task);

    // assert
    expect(task).not.toHaveBeenCalled();
  });

  it('runs the held tasks in order on release', async () => {
    // arrange
    const order: number[] = [];
    const tasks = new DeferredTasks();

    await tasks.run(() => Promise.resolve(void order.push(1)));
    await tasks.run(() => Promise.resolve(void order.push(2)));

    // act
    await tasks.release();

    // assert
    expect(order).toEqual([1, 2]);
  });

  it('runs a task added after release right away', async () => {
    // arrange
    const task = vi.fn().mockResolvedValue(undefined);
    const tasks = new DeferredTasks();

    await tasks.release();

    // act
    await tasks.run(task);

    // assert
    expect(task).toHaveBeenCalledOnce();
  });

  it('runs each task once', async () => {
    // arrange
    const task = vi.fn().mockResolvedValue(undefined);
    const tasks = new DeferredTasks();

    await tasks.run(task);
    await tasks.release();

    // act
    await tasks.release();

    // assert
    expect(task).toHaveBeenCalledOnce();
  });
});
