import { type ContextCarrier } from './telemetry.js';

/**
 * Where the creation context of an event travels: in its `meta`, so that it
 * goes through any serializer and any transport, and the outbox, along with
 * the event. The value is the JSON of the {@link ContextCarrier}.
 */
export const CONTEXT_META_KEY = 'telemetry.context';

type Meta = Record<string, string | number | undefined | null>;

/** The creation context stored in `meta`, if there is a valid one. */
export function readContext(
  meta: Meta | undefined,
): ContextCarrier | undefined {
  const raw = meta?.[CONTEXT_META_KEY];

  if (typeof raw !== 'string') {
    return undefined;
  }

  try {
    const parsed: unknown = JSON.parse(raw);

    if (
      typeof parsed === 'object' &&
      parsed !== null &&
      !Array.isArray(parsed) &&
      Object.values(parsed).every((value) => typeof value === 'string')
    ) {
      return parsed as ContextCarrier;
    }
  } catch {
    // Not ours, or damaged: handled as if there was none
  }

  return undefined;
}

/** A copy of `meta` that carries the creation context, if it has anything. */
export function withContext(
  meta: Meta | undefined,
  context: ContextCarrier | undefined,
): Meta | undefined {
  if (!context || Object.keys(context).length === 0) {
    return meta;
  }

  return { ...meta, [CONTEXT_META_KEY]: JSON.stringify(context) };
}
