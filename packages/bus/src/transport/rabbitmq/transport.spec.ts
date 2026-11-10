import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import { GenericContainer, Wait } from 'testcontainers';
import { Logger } from '@nestjs/common';
import { JsonSerializer } from '../../serialization/json.serializer.js';
import { NoopTelemetry } from '../../telemetry/noop.telemetry.js';
import { RabbitmqTransport } from './transport.js';

class TestEvent {
  public constructor(public readonly value: string) {}
}

describe('RabbitmqTransport', { timeout: 60_000 }, () => {
  let container: Awaited<ReturnType<GenericContainer['start']>>;
  let url: string;
  const serializer = new JsonSerializer();

  beforeAll(async () => {
    await serializer.onBusInit();

    container = await new GenericContainer('rabbitmq:3-management')
      .withExposedPorts(5672)
      .withWaitStrategy(
        Wait.forAll([
          Wait.forListeningPorts(),
          Wait.forLogMessage(/Server startup complete/),
        ]),
      )
      .start();
    url = `amqp://${container.getHost()}:${container.getMappedPort(5672)}`;
  }, 60_000);

  afterAll(async () => {
    await container.stop();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /**
   * A transport that was shut down while its handler was still running.
   * `finish` ends the handler, with a failure if one is given.
   */
  async function shutDownWhileHandling(retries: { max: number }) {
    const transport = new RabbitmqTransport(
      { url, group: crypto.randomUUID(), retries: { ...retries, delay: 100 } },
      serializer,
      new NoopTelemetry(),
    );
    const handling = Promise.withResolvers<void>();
    const handled = Promise.withResolvers<void>();

    await transport.onBusInit();
    await transport.subscribe(TestEvent, 'TestHandler', () => {
      handling.resolve();

      return handled.promise;
    });
    await transport.publish(new TestEvent('hello'));
    await handling.promise;
    await transport.onBusShutdown();

    return {
      warn: vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {}),
      finish: (failure?: Error) =>
        failure ? handled.reject(failure) : handled.resolve(),
    };
  }

  const settleWarning = expect.stringContaining('Failed to settle event');

  it('warns when a handled message cannot be acked after a shutdown', async () => {
    // arrange
    const { warn, finish } = await shutDownWhileHandling({ max: 0 });

    // act
    finish();

    // assert
    await vi.waitFor(() =>
      expect(warn).toHaveBeenCalledWith(settleWarning, expect.any(String)),
    );
  });

  it('warns when a failed message cannot be retried after a shutdown', async () => {
    // arrange
    const { warn, finish } = await shutDownWhileHandling({ max: 1 });

    vi.spyOn(Logger.prototype, 'error').mockImplementation(() => {});

    // act
    finish(new Error('boom'));

    // assert
    await vi.waitFor(() =>
      expect(warn).toHaveBeenCalledWith(settleWarning, expect.any(String)),
    );
  });

  it('warns when a dropped message cannot be acked after a shutdown', async () => {
    // arrange
    const { warn, finish } = await shutDownWhileHandling({ max: 0 });

    vi.spyOn(Logger.prototype, 'error').mockImplementation(() => {});

    // act
    finish(new Error('boom'));

    // assert
    await vi.waitFor(() =>
      expect(warn).toHaveBeenCalledWith(settleWarning, expect.any(String)),
    );
  });
});
