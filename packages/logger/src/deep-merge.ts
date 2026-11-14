import type { LogParams } from './types.js';

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  const prototype: unknown = Object.getPrototypeOf(value);

  return prototype === Object.prototype || prototype === null;
}

// copies plain objects and arrays so the result never shares them with a source;
// other values (Error, Date, class instances) are kept as they are
function clone(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(clone);
  }

  if (isPlainObject(value)) {
    return mergeInto({}, value);
  }

  return value;
}

function mergeInto(
  target: Record<string, unknown>,
  source: Record<string, unknown>,
): Record<string, unknown> {
  for (const key of Object.keys(source)) {
    // params may come from parsed user input, where `__proto__` is an own key
    if (key === '__proto__') {
      continue;
    }

    const value = source[key];

    if (value === undefined) {
      continue;
    }

    const current = target[key];

    if (isPlainObject(value) && isPlainObject(current)) {
      mergeInto(current, value);
    } else {
      target[key] = clone(value);
    }
  }

  return target;
}

/**
 * Deep-merges objects into a new one, later sources winning. Arrays and non-plain
 * values replace earlier ones instead of being merged; `undefined` is skipped.
 */
export function deepMerge(...sources: unknown[]): LogParams {
  const result: Record<string, unknown> = {};

  for (const source of sources) {
    if (typeof source === 'object' && source !== null) {
      mergeInto(result, source as Record<string, unknown>);
    }
  }

  return result;
}
