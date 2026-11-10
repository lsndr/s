import { Logger } from '@nestjs/common';
import { toMetricAttributes } from '../../telemetry/attributes.js';
import {
  type TelemetryAttributes,
  type TelemetryObservation,
  type TelemetryServer,
} from '../../telemetry/telemetry.js';
import { type Clients } from './clients.js';
import { type Streams } from './streams.js';

export interface BacklogOptions {
  group: string;
  /** Redis server, which telemetry reports. */
  server?: TelemetryServer;
}

interface GroupBacklog {
  stream: string;
  pending: number;
  /** Unknown when Redis can't tell, e.g. after entries were deleted. */
  lag: number | undefined;
}

export class Backlog {
  private readonly logger = new Logger(Backlog.name);
  private loading?: Promise<GroupBacklog[]>;

  public constructor(
    private readonly clients: Clients<'command'>,
    private readonly streams: Streams,
    private readonly options: BacklogOptions,
  ) {}

  /** Entries this consumer is processing, per stream. */
  public inflight(): TelemetryObservation[] {
    return this.streams.all().map(({ name }) => ({
      value: this.streams.inflight(name).length,
      attributes: this.attributesOf(name),
    }));
  }

  /** Entries delivered to the group and not acknowledged, per stream. */
  public async pending(): Promise<TelemetryObservation[]> {
    return (await this.load()).map(({ stream, pending }) => ({
      value: pending,
      attributes: this.attributesOf(stream),
    }));
  }

  /** Entries not delivered to the group yet, per stream. */
  public async lag(): Promise<TelemetryObservation[]> {
    const observations: TelemetryObservation[] = [];

    for (const { stream, lag } of await this.load()) {
      if (lag !== undefined) {
        observations.push({
          value: lag,
          attributes: this.attributesOf(stream),
        });
      }
    }

    return observations;
  }

  /**
   * Shared by the calls made while it runs, so that gauges observed in the
   * same collection ask Redis once.
   */
  private load(): Promise<GroupBacklog[]> {
    this.loading ??= this.query().finally(() => {
      this.loading = undefined;
    });

    return this.loading;
  }

  /** Never rejects: a stream that can't be queried is left out. */
  private async query(): Promise<GroupBacklog[]> {
    const { group } = this.options;
    const found: GroupBacklog[] = [];

    for (const { name: stream } of this.streams.all()) {
      try {
        const client = await this.clients.get('command');
        const info = (await client.xInfoGroups(stream)).find(
          ({ name }) => name === group,
        );

        if (info) {
          found.push({
            stream,
            pending: info.pending,
            lag: info.lag ?? undefined,
          });
        }
      } catch (err) {
        this.logger.warn(
          `Failed to observe the backlog of ${stream}: ${String(err)}`,
        );
      }
    }

    return found;
  }

  private attributesOf(stream: string): TelemetryAttributes {
    const { group, server } = this.options;

    return toMetricAttributes({
      system: 'redis',
      name: stream,
      consumerGroup: group,
      server,
    });
  }
}
