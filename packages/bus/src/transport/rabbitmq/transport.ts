import {
  Injectable,
  Logger,
  BeforeApplicationShutdown,
  type OnModuleInit,
} from '@nestjs/common';
import { type EventConstructor } from '../../types.js';
import { Transport, type TransportPublishOptions } from '../transport.js';
import {
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
} from 'amqplib';
import { type EventCallback, type Subscription } from '../subscriber.js';
import { type PublishEvent } from '../dispatcher.js';
import { type TelemetryDescriptor } from '../../telemetry/telemetry.js';
import {
  createEventDto,
  parseEventPayload,
  type EventDto,
} from '../event-dto.js';

export interface RabbitmqTransportConfig {
  url: string;
  /** Consumer group name, used as a prefix for queue names. */
  group: string;
  retries?: {
    /**
     * Maximum retries before a message is dropped.
     *
     * @default 5
     */
    max?: number;
    /**
     * Delay between retries, in milliseconds.
     *
     * @default 5000
     */
    delay?: number;
  };
}

/** The address of the server in an AMQP URL, if it is a valid one. */
function parseServer(url: string): TelemetryDescriptor['server'] {
  try {
    const { hostname, port, protocol } = new URL(url);
    const defaultPort = protocol === 'amqps:' ? 5671 : 5672;

    return hostname
      ? { address: hostname, port: port ? Number(port) : defaultPort }
      : undefined;
  } catch {
    return undefined;
  }
}

@Injectable()
export class RabbitmqTransport
  extends Transport<RabbitmqTransportConfig>
  implements OnModuleInit, BeforeApplicationShutdown
{
  private readonly exchange = 'events';

  private connection?: ChannelModel;
  private channel: ConfirmChannel | undefined;
  private readonly logger = new Logger(RabbitmqTransport.name);

  public async beforeApplicationShutdown(): Promise<void> {
    await this.getConnection().close();
  }

  public async onModuleInit(): Promise<void> {
    const { connect } = await import('amqplib');

    this.connection = await connect(this.config.url);
    this.connection.on('error', (err) => {
      this.logger.error(
        `RabbitMQ connection error: ${err instanceof Error ? err.message : String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    });
    this.channel = await this.connection.createConfirmChannel();

    await this.channel.assertExchange(this.exchange, 'direct', {
      durable: true,
    });
    await this.channel.assertExchange(`${this.exchange}.delay`, 'direct', {
      durable: true,
    });
  }

  public override getTelemetryDescriptor(): TelemetryDescriptor {
    const { url, group } = this.config;
    const server = parseServer(url);

    return {
      system: 'rabbitmq',
      // `{exchange}:{routing key}`, and the queue too for a consumer
      destination: (eventName, handler) =>
        handler
          ? `${this.exchange}:${eventName}:${this.getQueueName(eventName, handler)}`
          : `${this.exchange}:${eventName}`,
      attributes: (eventName) => ({
        'messaging.rabbitmq.destination.routing_key': eventName,
      }),
      consumerGroup: group,
      server,
    };
  }

  public async assertHealthy(): Promise<void> {
    await this.getChannel().checkExchange(this.exchange);
  }

  public publish(
    events: PublishEvent<TransportPublishOptions>[],
  ): Promise<void>;
  public publish(
    event: object,
    options?: TransportPublishOptions,
  ): Promise<void>;
  public async publish(
    eventOrEvents: object | PublishEvent<TransportPublishOptions>[],
    options?: TransportPublishOptions,
  ): Promise<void> {
    const events = Array.isArray(eventOrEvents)
      ? eventOrEvents
      : [{ event: eventOrEvents, options }];

    for (const { event, options: eventOptions } of events) {
      const payload = createEventDto(event, eventOptions);

      await this.publishEvent(payload.name, payload);
    }
  }

  public subscribe(subscriptions: Subscription[]): Promise<void>;
  public subscribe<
    C extends EventConstructor<any>,
    E extends InstanceType<C> = InstanceType<C>,
  >(event: C, handler: string, callback: EventCallback<E>): Promise<void>;
  public async subscribe(
    eventOrSubscriptions: EventConstructor<any> | Subscription[],
    handler?: string,
    callback?: EventCallback<any>,
  ): Promise<void> {
    const subscriptions = Array.isArray(eventOrSubscriptions)
      ? eventOrSubscriptions
      : [
          {
            event: eventOrSubscriptions,
            handler: handler!,
            callback: callback!,
          },
        ];

    for (const subscription of subscriptions) {
      await this.subscribeEvent(
        subscription.event,
        subscription.handler,
        subscription.callback,
      );
    }
  }

  private async subscribeEvent<
    C extends EventConstructor<any>,
    E extends InstanceType<C> = InstanceType<C>,
  >(event: C, handlerName: string, callback: EventCallback<E>): Promise<void> {
    const queue = await this.assertQueue(event.name, handlerName);
    const channel = this.getChannel();

    this.logger.log(
      `Subscribed to event ${event.name} (handler: ${handlerName})`,
    );

    const destination = this.getTelemetryDescriptor().destination?.(
      event.name,
      handlerName,
    );

    const handler = (message: ConsumeMessage | null) => {
      if (!message) {
        return;
      }

      const deliveryTag = message.fields.deliveryTag;
      const settle = (name: 'ack' | 'nack', fn: () => void) =>
        this.telemetry.span(
          {
            name,
            operation: { name, type: 'settle' },
            destination,
            attributes: {
              'messaging.rabbitmq.message.delivery_tag': deliveryTag,
            },
          },
          () => Promise.resolve(fn()),
        );

      const content = message.content.toString();

      this.logger.log(`Received message for event ${event.name}`);
      this.logger.verbose(`Event ${event.name} payload: ${content}`);

      Promise.resolve()
        .then(() =>
          callback(parseEventPayload(event, content), {
            attempt: this.getRetryCount(message) + 1,
            attributes: {
              'messaging.rabbitmq.message.delivery_tag': deliveryTag,
            },
          }),
        )
        .then(async () => {
          await settle('ack', () => channel.ack(message));

          this.logger.log(`Event ${event.name} handled successfully`);
        })
        .catch(async (err) => {
          const retryCount = this.getRetryCount(message);
          const dropped = retryCount >= this.getRetries().max;

          this.telemetry
            .counter(dropped ? 'dropped' : 'retries', {
              description: dropped
                ? 'Messages dropped after failing every retry'
                : 'Messages scheduled to be retried after a failure',
              unit: '{message}',
            })
            .add(1, {
              'messaging.destination.name': destination,
              'messaging.destination.subscription.name': handlerName,
            });

          this.logger.error(
            `Error handling event ${event.name} (retryCount: ${retryCount}): ${String(err)}`,
            err instanceof Error ? err.stack : undefined,
          );

          if (dropped) {
            this.logger.error(
              `Event ${event.name} failed permanently after ${retryCount} retries: ${String(err)}`,
              err instanceof Error ? err.stack : undefined,
            );
            await settle('ack', () => channel.ack(message));
          } else {
            await settle('nack', () => channel.nack(message, false, false));
          }
        })
        .catch((err) => {
          // The channel is gone, the broker redelivers the message
          this.logger.error(
            `Failed to settle event ${event.name}: ${String(err)}`,
            err instanceof Error ? err.stack : undefined,
          );
        });
    };

    await channel.consume(queue, (message) => handler(message), {
      noAck: false,
    });
  }

  private publishEvent(eventName: string, payload: EventDto): Promise<void> {
    const channel = this.getChannel();

    this.logger.log(`Publishing event ${eventName}`);
    this.logger.verbose(
      `Event ${eventName} payload: ${JSON.stringify(payload)}`,
    );

    return new Promise<void>((resolve, reject) => {
      const published = channel.publish(
        this.exchange,
        eventName,
        Buffer.from(JSON.stringify(payload)),
        { messageId: payload.id, persistent: true },
        (err) => {
          if (err) {
            const error = err instanceof Error ? err : new Error(String(err));
            this.logger.error(
              `Event ${eventName} was nacked by the broker`,
              error.stack,
            );
            reject(error);
          } else {
            this.logger.debug(`Event ${eventName} confirmed by the broker`);
            resolve();
          }
        },
      );

      if (!published) {
        this.logger.warn(
          `Channel buffer is full while publishing ${eventName}, waiting for drain`,
        );
        channel.once('drain', () => this.logger.warn(`Channel drained`));
      }
    });
  }

  private async assertQueue(
    eventName: string,
    handlerName: string,
  ): Promise<string> {
    const channel = this.getChannel();

    const routingKey = eventName;
    const queueName = this.getQueueName(eventName, handlerName);
    const delayQueueName = `${queueName}.delay`;

    const queue = await channel.assertQueue(queueName, {
      durable: true,
      autoDelete: false,
      arguments: {
        'x-dead-letter-exchange': `${this.exchange}.delay`,
        'x-dead-letter-routing-key': delayQueueName,
      },
    });

    await channel.bindQueue(queue.queue, this.exchange, routingKey);
    await channel.bindQueue(queue.queue, this.exchange, queueName);

    const delayQueue = await channel.assertQueue(delayQueueName, {
      durable: true,
      autoDelete: false,
      arguments: {
        'x-dead-letter-exchange': this.exchange,
        'x-dead-letter-routing-key': queueName,
        'x-message-ttl': this.getRetries().delay,
      },
    });

    await channel.bindQueue(
      delayQueue.queue,
      `${this.exchange}.delay`,
      delayQueueName,
    );

    return queue.queue;
  }

  private getQueueName(eventName: string, handlerName: string): string {
    return `${this.config.group}.${handlerName}.${eventName}`;
  }

  private getRetries(): { max: number; delay: number } {
    return {
      max: this.config.retries?.max ?? 5,
      delay: this.config.retries?.delay ?? 5_000,
    };
  }

  private getRetryCount(message: ConsumeMessage): number {
    const xDeath = message.properties.headers?.['x-death'];

    if (!Array.isArray(xDeath)) {
      return 0;
    }

    return xDeath
      .filter((entry) => entry.reason === 'rejected')
      .reduce((sum, entry) => sum + (entry.count ?? 0), 0);
  }

  private getChannel(): ConfirmChannel {
    if (!this.channel) {
      throw new Error(`${this.constructor.name} not initialized`);
    }

    return this.channel;
  }

  private getConnection(): ChannelModel {
    if (!this.connection) {
      throw new Error(`${this.constructor.name} not initialized`);
    }

    return this.connection;
  }
}
