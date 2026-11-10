/** Value of the `error.type` attribute, the same on spans and metrics. */
export function getErrorType(error: unknown): string {
  return error instanceof Error ? error.constructor.name : '_OTHER';
}
