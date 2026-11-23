# @lsndrs/logger

A [NestJS](https://nestjs.com/) `LoggerService` built on [pino](https://github.com/pinojs/pino). It writes structured JSON logs, and it attaches request-scoped fields (a request id, a user id) to every log entry written inside a context, without passing a logger or those fields through every call.

## Requirements

- Node.js >= 24
- `@nestjs/common` (`^12.0.0`), a peer dependency.
- `pino-pretty` (`^13.0.0`), an optional peer dependency. It is loaded whenever `NODE_ENV` is not `production` (including when it is unset); see [Configuration](#configuration).

## Installation

```bash
npm install @lsndrs/logger
```

For readable output during development, also install `pino-pretty`:

```bash
npm install -D pino-pretty
```

## Configuration

The logger is configured through environment variables. They are read once, when `@lsndrs/logger` is first imported, and all `Logger` instances share the same underlying pino instance.

| Variable    | Default | Effect                                                                                                                                                                                                                                                 |
| ----------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `LOG_LEVEL` | `info`  | Minimum level that is written: `trace`, `debug`, `info`, `warn`, `error` or `fatal`.                                                                                                                                                                   |
| `NODE_ENV`  | unset   | When it is anything other than `production`, output goes through `pino-pretty` (colorized, one line per entry, local time). With `production`, entries are written as JSON. Importing the package throws if `pino-pretty` is needed but not installed. |

## Output

In production each entry is one JSON line:

```json
{
  "level": "info",
  "time": 1794493687663,
  "pid": 62738,
  "hostname": "mac.local",
  "requestId": "r1",
  "itemId": "1",
  "source": "OrderService",
  "msg": "[OrderService] Processing started"
}
```

- `level` is the level name, not pino's numeric value.
- `source` is the name the logger was created with (see [Logging in services](#logging-in-services)). When there is one, the message is also prefixed with it as `[source]`; the prefix is colored yellow only when stdout is a terminal.
- Every other field comes from the params you pass, the logger's own params and the active [context](#contextual-logging).

## Logging in services

Create a logger with a source name, usually the class name, and call the usual NestJS methods on it:

```typescript
import { Logger } from '@lsndrs/logger';

class OrderService {
  private readonly logger = new Logger(OrderService.name);

  place(orderId: string) {
    this.logger.log('Processing started', { orderId });
  }
}
```

The methods map to pino levels as follows:

| Method      | Level   |
| ----------- | ------- |
| `verbose()` | `trace` |
| `debug()`   | `debug` |
| `log()`     | `info`  |
| `warn()`    | `warn`  |
| `error()`   | `error` |
| `fatal()`   | `fatal` |

Each method takes a message and any number of extra params:

- Object params are deep-merged into the entry's fields, in order. Anything that is not an object (a number, a string) is ignored.
- Passing an `Error` as the message logs its `message` as `msg` and adds pino's serialized error (`type`, `message`, `stack`) as the `err` field.
- `setParams()` sets fields that are added to every entry of that logger instance. It replaces the previous value rather than merging with it.

```typescript
const logger = new Logger('OrderService');
logger.setParams({ region: 'eu' });

logger.error(new Error('Payment failed'), { orderId: '42' });
// fields: region, orderId, source, err
```

### Params precedence

When the same field is set in several places, the later one wins. Nested objects are merged key by key; arrays and any other value replace the earlier one:

1. the active [context](#contextual-logging)
2. `setParams()`
3. the params passed to the call
4. `source`

## NestJS application logger

Pass an instance to NestJS to use it for the framework's own logs as well:

```typescript
import { NestFactory } from '@nestjs/core';
import { Logger } from '@lsndrs/logger';
import { AppModule } from './app.module.js';

const app = await NestFactory.create(AppModule, { logger: new Logger() });
```

NestJS calls the logger as `logger.log('message', 'ContextName')`. When the message is a string and the only extra param is a string, that string is used as the `source`, taking priority over the name given to the constructor. This is how NestJS's own logs get a source such as `NestFactory`.

`setLogLevels()` is part of the NestJS interface but is not implemented: it throws. Use `LOG_LEVEL` instead.

## Contextual logging

`withContext` runs a function with a set of params that every `Logger` adds to its entries while that function is running, including code it calls and anything that continues after an `await`. It is built on `AsyncLocalStorage`, so concurrent requests don't see each other's params.

```typescript
import { Logger } from '@lsndrs/logger';

const logger = new Logger('Orders');

async function handle(requestId: string) {
  await logger.withContext({ requestId }, async () => {
    logger.log('Request received');
    await load();
  });
}

async function load() {
  // a different Logger instance, still gets requestId
  new Logger('Repository').log('Loading orders');
}
```

Contexts nest. Params of an inner context are deep-merged over those of the outer one, and the outer context is restored when the inner function returns:

```typescript
logger.withContext({ requestId: 'r1' }, () => {
  logger.withContext({ userId: 'u1' }, () => {
    logger.log('Both'); // requestId and userId
  });

  logger.log('Only requestId');
});
```

`logger.withContext(params, fn)` and `LoggerContext.create(params, fn)` are equivalent, and `LoggerContext.params()` returns the params of the active context (an empty object outside of one). Use `LoggerContext` directly where you don't have a logger at hand, for example in middleware:

```typescript
import { LoggerContext } from '@lsndrs/logger';

function middleware(requestId: string, next: () => void) {
  LoggerContext.create({ requestId }, next);
}
```

## Measuring operations

`measure` runs a function, times it and logs the duration. The function can be synchronous or asynchronous; the result is always returned as a promise.

```typescript
import { Logger } from '@lsndrs/logger';

const logger = new Logger('Orders');

const orders = await logger.measure('loadOrders', () => fetchOrders(), {
  level: 'debug',
});
```

It logs a message like "Operation loadOrders completed in 12.34ms" at the given level (`info` by default) and adds `performance: { operationName, duration }` to the entry, with `duration` in milliseconds. If the function throws, nothing is logged and the error is passed on to the caller.

## Changelog

Release notes are published as [GitHub releases](https://github.com/lsndr/s/releases?q=%40lsndr%2Flogger%40&expanded=true).
