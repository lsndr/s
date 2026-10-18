import { Logger } from '@nestjs/common';
import { context, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { type PublishEvent } from '../../transport/dispatcher.js';
import { Outbox, type OutboxRecord } from '../../transport/outbox/outbox.js';
import {
  type EventCallback,
  type Subscription,
} from '../../transport/subscriber.js';
import {
  Transport,
  type TransportPublishOptions,
} from '../../transport/transport.js';
import { type EventConstructor, type EventPayload } from '../../types.js';
import { type Delivery, type TelemetryDescriptor } from '../telemetry.js';
import { BusOtel } from './bus-otel.js';
import { createOtelFixture } from './otel.fixture.js';

const descriptor: TelemetryDescriptor = {
  system: 'testmq',
  destination: (eventName, handler) =>
    handler ? `ex:${eventName}:${handler}` : `ex:${eventName}`,
  attributes: (eventName) => ({ 'testmq.routing_key': eventName }),
  consumerGroup: 'billing',
  server: { address: 'broker.local', port: 5672 },
};

/** Keeps what it is given, like a broker that is down when told to be. */
class FakeTransport extends Transport {
  public readonly published: PublishEvent<TransportPublishOptions>[][] = [];
  public readonly subscriptions: Subscription[] = [];
  public failWith: unknown;

  public override getTelemetryDescriptor(): TelemetryDescriptor {
    return descriptor;
  }

  public assertHealthy(): Promise<void> {
    return Promise.resolve();
  }

  public publish(
    eventOrEvents: object | PublishEvent<TransportPublishOptions>[],
    options?: TransportPublishOptions,
  ): Promise<void> {
    this.published.push(
      Array.isArray(eventOrEvents)
        ? eventOrEvents
        : [{ event: eventOrEvents, options }],
    );

    return this.failWith === undefined
      ? Promise.resolve()
      : // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors --- tests reject with whatever they are told to
        Promise.reject(this.failWith);
  }

  public subscribe(
    eventOrSubscriptions: EventConstructor<any> | Subscription[],
    handler?: string,
    callback?: EventCallback<any>,
  ): Promise<void> {
    this.subscriptions.push(
      ...(Array.isArray(eventOrSubscriptions)
        ? eventOrSubscriptions
        : [
            {
              event: eventOrSubscriptions,
              handler: handler!,
              callback: callback!,
            },
          ]),
    );

    return Promise.resolve();
  }

  /** The creation context sent along with each published message. */
  public contexts(): Record<string, string>[] {
    return this.published.at(-1)!.map(({ options }) => options!.context!);
  }
}

class FakeOutbox extends Outbox {
  public readonly saved: OutboxRecord[] = [];
  public subscribed = false;

  public override getTelemetryDescriptor(): TelemetryDescriptor {
    return descriptor;
  }

  public override subscribe(): Promise<void> {
    this.subscribed = true;

    return Promise.resolve();
  }

  protected save(records: OutboxRecord[]): Promise<void> {
    this.saved.push(...records);

    return Promise.resolve();
  }

  protected relay(): Promise<number> {
    return Promise.resolve(0);
  }
}

/** An event class with the given name. */
function eventClass(name: string): EventConstructor<object> {
  return { [name]: class {} }[name]!;
}

function payload(id: string, context?: Record<string, string>) {
  return {
    id,
    event: {},
    meta: {},
    publishedAt: new Date(),
    context,
  } as EventPayload<object>;
}

describe('BusOtel', () => {
  let otel: ReturnType<typeof createOtelFixture>;
  let transport: FakeTransport;

  /** Subscribes the handler and has the broker deliver a message to it. */
  async function deliver(
    options: {
      handler?: string;
      event?: string;
      id?: string;
      context?: Record<string, string>;
      delivery?: Delivery;
    },
    fn: () => Promise<void> = () => Promise.resolve(),
  ) {
    const { handler = 'BillingHandler', event = 'OrderPlaced' } = options;

    await transport.subscribe(eventClass(event), handler, fn);
    await transport.subscriptions
      .at(-1)!
      .callback(payload(options.id ?? 'm1', options.context), options.delivery);
  }

  beforeEach(async () => {
    otel = createOtelFixture();
    transport = new FakeTransport();

    await otel.telemetry.onBusInit();
    transport.instrument(otel.telemetry);
  });

  afterEach(async () => {
    await otel.shutdown();
  });

  it('lets transports report before init, which drops it, and registers their gauges after', async () => {
    const early = createOtelFixture();
    const reporter = early.telemetry.transport(descriptor);

    reporter.counter('retries').add(1);
    reporter.gauge('inflight', {}, () => [{ value: 3 }]);
    await expect(
      reporter.span({ name: 'compact' }, () => Promise.resolve('done')),
    ).resolves.toBe('done');

    await early.telemetry.onBusInit();
    reporter.counter('retries').add(2);

    expect((await early.metric('bus.retries'))[0]?.value).toBe(2);
    expect((await early.metric('bus.inflight'))[0]?.value).toBe(3);
    expect(early.spans()).toEqual([]);

    await early.shutdown();
  });

  it('refuses to process a message before init', async () => {
    const telemetry = new BusOtel();

    await expect(
      telemetry.process(
        { descriptor, eventName: 'E', handler: 'H', messageId: 'm1' },
        () => Promise.resolve(),
      ),
    ).rejects.toThrow(/init/);
  });

  it('instruments a transport once', async () => {
    transport.instrument(otel.telemetry);
    await transport.publish({}, { id: 'm1', name: 'OrderPlaced' });

    expect(otel.spans()).toHaveLength(1);
  });

  describe('publish', () => {
    it('sends a single message with a producer span that is its creation context', async () => {
      await transport.publish({}, { id: 'm1', name: 'OrderPlaced' });

      const span = otel.span('send ex:OrderPlaced');
      const [carrier] = transport.contexts();

      expect(span.kind).toBe(SpanKind.PRODUCER);
      expect(span.attributes).toEqual({
        'messaging.system': 'testmq',
        'messaging.operation.name': 'send',
        'messaging.operation.type': 'send',
        'messaging.destination.name': 'ex:OrderPlaced',
        'messaging.consumer.group.name': 'billing',
        'server.address': 'broker.local',
        'server.port': 5672,
        'messaging.message.id': 'm1',
        'testmq.routing_key': 'OrderPlaced',
      });
      expect(span.links).toEqual([]);
      expect(carrier!['traceparent']).toContain(span.spanContext().traceId);
      expect(carrier!['traceparent']).toContain(span.spanContext().spanId);
      expect(otel.spans()).toHaveLength(1);
    });

    it('gives the message an id and a name when it has none', async () => {
      await transport.publish(new (eventClass('OrderPlaced'))());

      const { options } = transport.published[0]![0]!;

      expect(options?.name).toBe('OrderPlaced');
      expect(options?.id).toEqual(expect.any(String));
      expect(otel.span('send ex:OrderPlaced').attributes).toMatchObject({
        'messaging.message.id': options?.id,
      });
    });

    it('does not report publishing nothing', async () => {
      await transport.publish([]);

      expect(otel.spans()).toEqual([]);
    });

    it('creates a span for each message of a batch and links the send span to them', async () => {
      await transport.publish([
        { event: {}, options: { id: 'm1', name: 'OrderPlaced' } },
        { event: {}, options: { id: 'm2', name: 'OrderPlaced' } },
      ]);

      const creates = otel.spans().filter((s) => s.name.startsWith('create'));
      const send = otel.span('send ex:OrderPlaced');
      const contexts = transport.contexts();

      expect(creates).toHaveLength(2);
      expect(creates.every((s) => s.kind === SpanKind.PRODUCER)).toBe(true);
      expect(send.kind).toBe(SpanKind.CLIENT);
      expect(send.attributes['messaging.batch.message_count']).toBe(2);
      expect(send.attributes).not.toHaveProperty('messaging.message.id');
      expect(send.links.map((l) => l.context.spanId)).toEqual(
        creates.map((s) => s.spanContext().spanId),
      );
      expect(contexts.map((c) => c['traceparent'])).toEqual(
        creates.map((s) => expect.stringContaining(s.spanContext().spanId)),
      );
    });

    it('leaves the destination out of a batch of different destinations', async () => {
      await transport.publish([
        { event: {}, options: { id: 'm1', name: 'A' } },
        { event: {}, options: { id: 'm2', name: 'B' } },
      ]);

      expect(otel.span('send').attributes).not.toHaveProperty(
        'messaging.destination.name',
      );
    });

    it('keeps the creation context a message has and links to it', async () => {
      const outbox = new FakeOutbox();

      outbox.instrument(otel.telemetry);
      await outbox.publish({}, { id: 'm1', name: 'OrderPlaced' });

      const create = otel.span('create ex:OrderPlaced');
      const context = outbox.saved[0]!.context!;

      await transport.publish({}, { id: 'm1', name: 'OrderPlaced', context });

      const send = otel.span('send ex:OrderPlaced');

      expect(create.kind).toBe(SpanKind.PRODUCER);
      expect(transport.contexts()).toEqual([context]);
      expect(send.kind).toBe(SpanKind.CLIENT);
      expect(send.links.map((l) => l.context.spanId)).toEqual([
        create.spanContext().spanId,
      ]);
      expect(otel.spans()).toHaveLength(2);
    });

    it('records the duration and the count of the messages sent', async () => {
      await transport.publish([
        { event: {}, options: { id: 'm1', name: 'OrderPlaced' } },
        { event: {}, options: { id: 'm2', name: 'OrderPlaced' } },
      ]);

      const [sent] = await otel.metric('messaging.client.sent.messages');
      const [duration] = await otel.metric(
        'messaging.client.operation.duration',
      );

      expect(sent?.value).toBe(2);
      expect(sent?.attributes).toMatchObject({
        'messaging.system': 'testmq',
        'messaging.operation.name': 'send',
        'messaging.destination.name': 'ex:OrderPlaced',
      });
      expect(sent?.attributes).not.toHaveProperty('error.type');
      expect(duration?.attributes['messaging.operation.name']).toBe('send');
    });

    it('reports a failure on the span, the metrics and as a warning', async () => {
      class BrokerDown extends Error {}

      transport.failWith = new BrokerDown('down');

      await expect(
        transport.publish({}, { id: 'm1', name: 'OrderPlaced' }),
      ).rejects.toThrow('down');

      const span = otel.span('send ex:OrderPlaced');
      const [sent] = await otel.metric('messaging.client.sent.messages');
      const [log] = otel.logs();

      expect(span.status.code).toBe(SpanStatusCode.ERROR);
      expect(span.attributes['error.type']).toBe('BrokerDown');
      expect(sent?.attributes['error.type']).toBe('BrokerDown');
      expect(log?.eventName).toBe('messaging.send.exception');
      expect(log?.severityNumber).toBe(13);
      expect(log?.attributes).toMatchObject({
        'exception.type': 'BrokerDown',
        'exception.message': 'down',
      });
      expect(log?.spanContext?.spanId).toBe(span.spanContext().spanId);
    });
  });

  describe('outbox', () => {
    it('reports publishing as creating the messages and leaves subscribing alone', async () => {
      const outbox = new FakeOutbox();

      outbox.instrument(otel.telemetry);
      await outbox.publish({}, { id: 'm1', name: 'OrderPlaced' });
      await outbox.subscribe();

      const span = otel.span('create ex:OrderPlaced');

      expect(span.kind).toBe(SpanKind.PRODUCER);
      expect(span.attributes).toMatchObject({
        'messaging.operation.name': 'create',
        'messaging.operation.type': 'create',
      });
      expect(outbox.saved[0]?.context?.['traceparent']).toContain(
        span.spanContext().spanId,
      );
      expect(outbox.subscribed).toBe(true);
      expect(await otel.metric('messaging.client.sent.messages')).toEqual([]);
    });
  });

  describe('process', () => {
    it('handles a message with a consumer span linked to its creation context', async () => {
      await transport.publish({}, { id: 'm1', name: 'OrderPlaced' });

      const [context] = transport.contexts();
      const send = otel.span('send ex:OrderPlaced');

      await deliver({
        context,
        delivery: { attempt: 2, attributes: { 'testmq.tag': 7 } },
      });

      const span = otel.span('process ex:OrderPlaced:BillingHandler');

      expect(span.kind).toBe(SpanKind.CONSUMER);
      expect(span.parentSpanContext).toBeUndefined();
      expect(span.spanContext().traceId).not.toBe(send.spanContext().traceId);
      expect(span.links.map((l) => l.context.spanId)).toEqual([
        send.spanContext().spanId,
      ]);
      expect(span.attributes).toMatchObject({
        'messaging.system': 'testmq',
        'messaging.operation.name': 'process',
        'messaging.operation.type': 'process',
        'messaging.destination.name': 'ex:OrderPlaced:BillingHandler',
        'messaging.destination.subscription.name': 'BillingHandler',
        'messaging.consumer.group.name': 'billing',
        'messaging.message.id': 'm1',
        'bus.delivery.attempt': 2,
        'testmq.tag': 7,
      });
    });

    it('is a child of the active span instead of the creation context', async () => {
      const outer = otel.tracer.startSpan('outer');

      await context.with(trace.setSpan(context.active(), outer), () =>
        deliver({}),
      );
      outer.end();

      const span = otel.span('process ex:OrderPlaced:BillingHandler');

      expect(span.parentSpanContext?.spanId).toBe(outer.spanContext().spanId);
      expect(span.links).toEqual([]);
    });

    it('ignores a creation context that is not valid', async () => {
      await deliver({ handler: 'H', context: { traceparent: 'garbage' } });

      expect(otel.span('process ex:OrderPlaced:H').links).toEqual([]);
    });

    it('records the duration and the delivery of the message', async () => {
      await deliver({ handler: 'H' });

      const [duration] = await otel.metric('messaging.process.duration');
      const [consumed] = await otel.metric(
        'messaging.client.consumed.messages',
      );

      expect(duration?.attributes).toMatchObject({
        'messaging.operation.name': 'process',
        'messaging.consumer.group.name': 'billing',
        'messaging.destination.subscription.name': 'H',
      });
      expect(consumed?.value).toBe(1);
    });

    it('passes the payload and delivery on to the handler', async () => {
      const handler = vi.fn().mockResolvedValue(undefined);
      const delivery = { attempt: 3 };

      await transport.subscribe(eventClass('OrderPlaced'), 'H', handler);
      await transport.subscriptions[0]!.callback(payload('m9'), delivery);

      expect(handler).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ id: 'm9' }),
        delivery,
      );
    });

    it('reports a failure on the span, the metrics and as an error', async () => {
      await expect(
        deliver({ handler: 'H' }, () => Promise.reject(new TypeError('bad'))),
      ).rejects.toThrow('bad');

      const span = otel.span('process ex:OrderPlaced:H');
      const [duration] = await otel.metric('messaging.process.duration');
      const [log] = otel.logs();

      expect(span.status).toMatchObject({
        code: SpanStatusCode.ERROR,
        message: 'bad',
      });
      expect(span.attributes['error.type']).toBe('TypeError');
      expect(duration?.attributes['error.type']).toBe('TypeError');
      expect(log?.eventName).toBe('messaging.process.exception');
      expect(log?.severityNumber).toBe(17);
      expect(log?.attributes['exception.stacktrace']).toContain('TypeError');
    });

    it('uses _OTHER as the type of an error that is not an Error', async () => {
      await expect(
        deliver(
          { handler: 'H' },
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors --- a rejection that is not an Error is the point
          () => Promise.reject('nope'),
        ),
      ).rejects.toBe('nope');

      expect(
        otel.span('process ex:OrderPlaced:H').attributes['error.type'],
      ).toBe('_OTHER');
    });
  });

  describe('transport', () => {
    it('reports a settle operation as a timed client span', async () => {
      const reporter = otel.telemetry.transport(descriptor);

      await reporter.span(
        {
          name: 'ack',
          operation: { name: 'ack', type: 'settle' },
          destination: 'ex:OrderPlaced:H',
          attributes: { 'testmq.tag': 3 },
        },
        () => Promise.resolve(),
      );

      const span = otel.span('ack ex:OrderPlaced:H');
      const duration = await otel.metric('messaging.client.operation.duration');

      expect(span.kind).toBe(SpanKind.CLIENT);
      expect(span.attributes).toMatchObject({
        'messaging.operation.name': 'ack',
        'messaging.operation.type': 'settle',
        'testmq.tag': 3,
      });
      expect(duration).toHaveLength(1);
    });

    it('reports a span that is not an operation as internal', async () => {
      const reporter = otel.telemetry.transport(descriptor);

      await reporter.span({ name: 'compact' }, () => Promise.resolve());

      expect(otel.span('compact').kind).toBe(SpanKind.INTERNAL);
    });

    it('reports counters, histograms and gauges under the namespace', async () => {
      const reporter = otel.telemetry.transport(descriptor);

      reporter.counter('retries').add(2, { queue: 'q' });
      reporter.histogram('relay.duration', { unit: 's' }).record(0.5);
      reporter.gauge('inflight', {}, () => [
        { value: 4, attributes: { stream: 's' } },
      ]);

      const [retries] = await otel.metric('bus.retries');
      const [relay] = await otel.metric('bus.relay.duration');
      const [inflight] = await otel.metric('bus.inflight');

      expect(retries?.value).toBe(2);
      expect(retries?.attributes).toEqual({
        'messaging.system': 'testmq',
        queue: 'q',
      });
      expect(relay).toBeDefined();
      expect(inflight?.value).toBe(4);
      expect(inflight?.attributes).toEqual({
        'messaging.system': 'testmq',
        stream: 's',
      });
    });

    it('prefixes the instruments with a namespace of choice', async () => {
      const custom = createOtelFixture({ namespace: 'orders' });

      await custom.telemetry.onBusInit();
      custom.telemetry.transport(descriptor).counter('retries').add(1);

      expect(await custom.metric('orders.retries')).toHaveLength(1);
      expect(await custom.metric('bus.retries')).toHaveLength(0);

      await custom.shutdown();
    });
  });

  describe('without @opentelemetry/api-logs', () => {
    afterEach(() => {
      vi.doUnmock('@opentelemetry/api-logs');
      vi.restoreAllMocks();
      vi.resetModules();
    });

    it('warns and skips the exception events', async () => {
      const warn = vi.spyOn(Logger.prototype, 'warn').mockReturnValue();

      vi.resetModules();
      vi.doMock('@opentelemetry/api-logs', () => {
        throw new Error('Cannot find package');
      });

      const { BusOtel: Fresh } = await import('./bus-otel.js');
      const telemetry = new Fresh();
      const other = new FakeTransport();

      await telemetry.onBusInit();
      other.instrument(telemetry);
      await other.subscribe(eventClass('OrderPlaced'), 'H', () =>
        Promise.reject(new Error('boom')),
      );

      await expect(
        other.subscriptions.at(-1)!.callback(payload('m1')),
      ).rejects.toThrow('boom');

      expect(warn).toHaveBeenCalledExactlyOnceWith(
        expect.stringContaining('@opentelemetry/api-logs'),
      );
    });
  });
});
