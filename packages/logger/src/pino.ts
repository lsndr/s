import createPino from 'pino';
import type { TransportSingleOptions } from 'pino';

const level = process.env['LOG_LEVEL'] ?? 'info';
const pretty = process.env['NODE_ENV'] !== 'production';

const transport: TransportSingleOptions | undefined = pretty
  ? {
      target: 'pino-pretty',
      options: {
        colorize: true,
        singleLine: true,
        translateTime: 'SYS:HH:MM:ss.l',
      },
    }
  : undefined;

const pino = createPino({
  level,
  formatters: {
    level: (label) => ({ level: label }),
  },
  ...(transport !== undefined ? { transport } : {}),
});

export { pino };
