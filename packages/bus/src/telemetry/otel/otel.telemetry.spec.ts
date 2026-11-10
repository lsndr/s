import { describe, expect, it } from 'vitest';
import { BusOtel } from './otel.telemetry.js';

describe('BusOtel', () => {
  it('has no tracer before it is initialized', () => {
    // arrange
    const telemetry = new BusOtel({});

    // act
    const result = () => telemetry.getTracer();

    // assert
    expect(result).toThrow('not initialized');
  });

  it('has no meter before it is initialized', () => {
    // arrange
    const telemetry = new BusOtel({});

    // act
    const result = () => telemetry.getMeter();

    // assert
    expect(result).toThrow('not initialized');
  });
});
