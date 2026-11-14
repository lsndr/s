import { AsyncLocalStorage } from 'node:async_hooks';
import { deepMerge } from './deep-merge.js';
import type { LogParams } from './types.js';

const storage = new AsyncLocalStorage<LogParams>();

/**
 * Carries log params across async calls with `AsyncLocalStorage`. Nested contexts deep-merge over outer ones.
 */
export class LoggerContext {
  public static create<T>(params: LogParams, fn: () => T): T {
    const existingParams = this.params();
    const mergedParams = deepMerge(existingParams, params);

    return storage.run(mergedParams, fn);
  }

  public static params(): LogParams {
    return storage.getStore() ?? {};
  }
}
