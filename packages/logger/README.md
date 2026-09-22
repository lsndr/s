# @lsndr/logger

Structured logging library built on [pino](https://github.com/pinojs/pino) with [NestJS](https://nestjs.com/) `LoggerService` integration and `AsyncLocalStorage`-based contextual logging.

## Installation

```bash
npm install @lsndr/logger
```

For local development with pretty-printed logs, also install `pino-pretty`:

```bash
npm install -D pino-pretty
```

## Usage

### As NestJS application logger

```typescript
import { NestFactory } from '@nestjs/core';
import { Logger } from '@lsndr/logger';

const app = await NestFactory.create(AppModule, { logger: new Logger() });
```

### In services

```typescript
import { Logger } from '@lsndr/logger';

class MyService {
  private readonly logger = new Logger(MyService.name);

  doWork() {
    this.logger.log('Processing started', { itemId: '123' });
  }
}
```

### Performance measurement

```typescript
const result = await this.logger.measure('fetchData', () => fetchData());
```

### Contextual logging

```typescript
import { Logger, LoggerContext } from '@lsndr/logger';

const logger = new Logger('MyService');

logger.withContext({ requestId: 'abc-123' }, () => {
  logger.log('This log entry includes requestId automatically');
});
```

## Configuration

- `LOG_LEVEL` — log level (`trace`, `debug`, `info`, `warn`, `error`, `fatal`). Defaults to `info`.
- `NODE_ENV` — when not `production`, enables pretty-printed output via `pino-pretty`.
