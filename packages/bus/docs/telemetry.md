# Telemetry

Tracing and metrics are pluggable through the `telemetry` option.

## Supported telemetry

- [OpenTelemetry](#opentelemetry) (`OtelTelemetry`)
- Any [custom telemetry](#custom-telemetry) implementing the `Telemetry` interface

## OpenTelemetry

`OtelTelemetry` uses [OpenTelemetry](https://opentelemetry.io) and follows the messaging semantic conventions for [spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/) and [metrics](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/). It needs `@opentelemetry/api`; spans and metrics are exported by whatever SDK you have set up.

```typescript
import { BusModule, OtelTelemetry } from '@lsndr/bus';

BusModule.forRoot({
  transport,
  telemetry: OtelTelemetry.withConfig(),
});
```

### Options

| Option           | Default                | Description                                                                                                                                                               |
| ---------------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tracerProvider` | global tracer provider | OpenTelemetry tracer provider that creates the spans.                                                                                                                     |
| `meterProvider`  | global meter provider  | OpenTelemetry meter provider that creates the metrics. It is taken when the bus initializes, so register a global one before the application bootstraps, or pass it here. |
| `propagator`     | global propagator      | Injects the trace context into the message headers and reads it back when a message is processed.                                                                         |
| `prefix`         | `bus`                  | Goes before the names the conventions have no name for, e.g. `bus.delivery.latency`.                                                                                      |

### Traces

| Span                                       | Kind     | When                                                                                               |
| ------------------------------------------ | -------- | -------------------------------------------------------------------------------------------------- |
| `send <destination>`                       | producer | An event is published. The trace context is injected into the message headers.                     |
| `process <destination>`                    | consumer | A handler processes a message. It is a child of the `send` span, and your own spans nest under it. |
| `ack <destination>` / `nack <destination>` | client   | A delivery is settled.                                                                             |
| `create <event>`                           | producer | An event is stored in an [outbox](outbox.md). The `send` span of its relay is a child of it.       |

Spans carry the attributes the conventions require or recommend, including `server.address` and `server.port`. `process` spans also have `bus.delivery.attempt`: 1 for the first try, growing with every retry. A failing handler marks its span as an error.

### Metrics

Metrics of the semantic conventions:

| Metric                                | Type         | Recorded for                                                        |
| ------------------------------------- | ------------ | ------------------------------------------------------------------- |
| `messaging.client.operation.duration` | histogram, s | Every `create`, `send`, `receive` and `settle`.                     |
| `messaging.client.sent.messages`      | counter      | Every `send`. Storing in an outbox doesn't count, relaying it does. |
| `messaging.client.consumed.messages`  | counter      | Every delivery, once.                                               |
| `messaging.process.duration`          | histogram, s | Every handler that processes a message.                             |

Metrics of the bus:

| Metric                        | Type         | What it tells                                                                                                                                      |
| ----------------------------- | ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bus.settled.messages`        | counter      | How a delivery ended, as `bus.settlement.outcome`: `acked`, `retried` (will be delivered again) or `dropped` (failed with no retries left).        |
| `bus.delivery.latency`        | histogram, s | Time from `publishedAt` to the first processing, outbox wait included. Retries are not recorded. It compares the clocks of publisher and consumer. |
| `bus.outbox.relay.latency`    | histogram, s | Time a record waited in an [outbox](outbox.md) before its relay.                                                                                   |
| `bus.outbox.relayed.messages` | counter      | Records an outbox handed to `dispatch`. Records of a failed dispatch are counted again when retried.                                               |
| `bus.outbox.relay.duration`   | histogram, s | Duration of one `relay` round, empty ones included.                                                                                                |

A transport can report metrics of its own, which are described in its section of [Transports](transports.md).

Metrics have `messaging.operation.name`, `messaging.system.name`, `messaging.destination.name`, `messaging.consumer.group.name`, `messaging.destination.subscription.name`, `server.address` and `server.port` where they apply, and `error.type` when the operation failed. The message id, client id and routing key are left out, since each value would create a new series.

### Dashboards

Grafana dashboards of these metrics, one for each built-in transport, are in the [`dashboards`](../dashboards) folder. They show throughput, durations, failures and the outbox. Import one in Grafana and pick a Prometheus data source. Every release has the files attached.

They expect the names Prometheus gives OpenTelemetry metrics, with the unit as a suffix (`messaging.process.duration` becomes `messaging_process_duration_seconds`, counters end in `_total`), the service as the `job` label, and the default `prefix` of `OtelTelemetry`.

## Custom telemetry

To plug in something else, provide your own `Telemetry`:

```typescript
interface Telemetry {
  getTracer(): TelemetryTracer;
  getMeter(): TelemetryMeter;
}
```

Names are given as parts, and the telemetry joins them the way its backend writes them: a metric is `['delivery', 'latency']`, an attribute is `{ name: ['system'], value: 'my-transport' }`.

The names of the bus have no prefix; adding one is up to the telemetry. `OtelTelemetry` joins parts with dots, exports names the semantic conventions know under their convention name, keeps the names in the namespaces of the built-in transports as they are, and puts the rest under `prefix` (default `bus`).
