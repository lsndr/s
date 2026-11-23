# lsndr's packages

| Package                                       | Description                                                                                                                                                          |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`@lsndrs/bus`](packages/bus/README.md)       | Event bus for NestJS. Transport agnostic (Redis Streams, RabbitMQ or your own), with middlewares, a transactional outbox, pluggable serialization and observability. |
| [`@lsndrs/domain`](packages/domain/README.md) | Building blocks for DDD-style domain models: aggregate roots, value objects and domain errors, plus an ESLint rule. No runtime dependencies.                         |
| [`@lsndrs/logger`](packages/logger/README.md) | A pino-based NestJS `LoggerService` that attaches request-scoped fields to every log entry written inside a context.                                                 |

Each package README covers its own requirements, peer dependencies and usage.

## Changelog

Release notes are published as [GitHub releases](https://github.com/lsndr/s/releases).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). To report a vulnerability, see [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
