import { type LoggerService } from '@nestjs/common';
import type { Level, LogFn } from 'pino';
import { styleText } from 'node:util';
import { merge } from 'lodash-es';
import { pino } from './pino.js';
import { type LogParams } from './types.js';
import { LoggerContext } from './logger-context.js';

export type LogLevel = 'info' | 'error' | 'warn' | 'debug' | 'trace' | 'fatal';

export interface MeasureOptions {
  level?: LogLevel;
}

/**
 * NestJS-compatible logger that writes structured entries through a shared pino instance,
 * merging in the params of the active `LoggerContext`.
 */
export class Logger implements LoggerService {
  private baseParams?: LogParams;

  public constructor(private readonly source?: string) {}

  public setParams(params: LogParams): void {
    this.baseParams = params;
  }

  public withContext<T>(params: LogParams, fn: () => T): T {
    return LoggerContext.create(params, fn);
  }

  public async measure<R>(
    operationName: string,
    fn: () => R,
    options?: MeasureOptions,
  ): Promise<Awaited<R>> {
    const level = options?.level ?? 'info';

    const s = performance.now();
    const result = await fn();
    const duration = performance.now() - s;

    this.write(
      level,
      `Operation \`${operationName}\` completed in ${duration.toFixed(2)}ms`,
      [{ performance: { operationName, duration } }],
    );

    return result;
  }

  public log(message: unknown, ...optionalParams: unknown[]): void {
    this.write('info', message, optionalParams);
  }

  public error(message: unknown, ...optionalParams: unknown[]): void {
    this.write('error', message, optionalParams);
  }

  public warn(message: unknown, ...optionalParams: unknown[]): void {
    this.write('warn', message, optionalParams);
  }

  public debug(message: unknown, ...optionalParams: unknown[]): void {
    this.write('debug', message, optionalParams);
  }

  public verbose(message: unknown, ...optionalParams: unknown[]): void {
    this.write('trace', message, optionalParams);
  }

  public fatal(message: unknown, ...optionalParams: unknown[]): void {
    this.write('fatal', message, optionalParams);
  }

  public setLogLevels(): void {
    throw new Error('Unimplemented');
  }

  private buildParams(paramsArray: unknown[]): LogParams {
    const result: LogParams = {};

    for (const params of paramsArray) {
      if (typeof params === 'object' && params !== null) {
        merge(result, params);
      }
    }

    return result;
  }

  private write(
    level: Level,
    message: unknown,
    optionalParams: unknown[],
  ): void {
    const contextParams = LoggerContext.params();
    let source = this.source;

    if (
      typeof message === 'string' &&
      optionalParams.length === 1 &&
      typeof optionalParams[0] === 'string'
    ) {
      source = optionalParams[0];
      optionalParams = [];
    }

    const params = merge(
      {},
      contextParams,
      this.baseParams,
      this.buildParams(optionalParams),
      source !== undefined ? { source } : {},
    );

    const fn = this.levelFn(level);

    if (message instanceof Error) {
      fn(
        { ...params, err: message },
        this.composeMessage(message.message, source),
      );
    } else {
      fn(params, this.composeMessage(String(message), source));
    }
  }

  private composeMessage(message: string, source?: string): string {
    return source
      ? `${styleText('yellow', `[${source}]`)} ${message}`
      : message;
  }

  private levelFn(level: Level): LogFn {
    switch (level) {
      case 'fatal':
        return pino.fatal.bind(pino);
      case 'error':
        return pino.error.bind(pino);
      case 'warn':
        return pino.warn.bind(pino);
      case 'info':
        return pino.info.bind(pino);
      case 'debug':
        return pino.debug.bind(pino);
      case 'trace':
        return pino.trace.bind(pino);
    }
  }
}
