import { type TelemetryServer } from './telemetry.js';

/**
 * Takes the broker address out of a connection URL, without its credentials.
 * Returns nothing for a URL that can't be parsed, so telemetry never breaks
 * a connection that works.
 */
export function parseServer(
  url: string,
  defaultPorts: Record<string, number>,
): TelemetryServer | undefined {
  try {
    const { protocol, hostname, port } = new URL(url);

    if (!hostname) {
      return undefined;
    }

    const resolved = port ? Number(port) : defaultPorts[protocol];

    return resolved === undefined
      ? { address: hostname }
      : { address: hostname, port: resolved };
  } catch {
    return undefined;
  }
}
