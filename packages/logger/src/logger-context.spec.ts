import { describe, expect, it } from 'vitest';
import { LoggerContext } from './logger-context.js';

describe('LoggerContext', () => {
  describe('params', () => {
    it('returns empty object when no context is set', () => {
      const params = LoggerContext.params();
      expect(params).toEqual({});
    });

    it('returns stored params within a context', () => {
      LoggerContext.create({ requestId: '123' }, () => {
        expect(LoggerContext.params()).toEqual({ requestId: '123' });
      });
    });

    it('returns empty object after context exits', () => {
      LoggerContext.create({ requestId: '123' }, () => {});
      expect(LoggerContext.params()).toEqual({});
    });
  });

  describe('create', () => {
    it('runs the given function and returns its result', () => {
      const result = LoggerContext.create({ userId: 'abc' }, () => 42);
      expect(result).toBe(42);
    });

    it('merges params with existing context params', () => {
      LoggerContext.create({ requestId: '123' }, () => {
        LoggerContext.create({ userId: 'abc' }, () => {
          expect(LoggerContext.params()).toEqual({
            requestId: '123',
            userId: 'abc',
          });
        });
      });
    });

    it('inner context params override outer context params for the same key', () => {
      LoggerContext.create({ requestId: 'outer' }, () => {
        LoggerContext.create({ requestId: 'inner' }, () => {
          expect(LoggerContext.params()).toEqual({ requestId: 'inner' });
        });
      });
    });

    it('outer context is restored after inner context exits', () => {
      LoggerContext.create({ requestId: 'outer' }, () => {
        LoggerContext.create({ requestId: 'inner' }, () => {});
        expect(LoggerContext.params()).toEqual({ requestId: 'outer' });
      });
    });
  });
});
