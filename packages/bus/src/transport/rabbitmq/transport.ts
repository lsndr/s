import { Logger } from '@nestjs/common';
import { type EventConstructor } from '../../types.js';
import { type Transport } from '../transport.js';
import { type TransportProvider } from '../provider.js';
import { type OnBusInit, type OnBusShutdown } from '../../lifecycle.js';
import {
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
} from 'amqplib';
import { type EventCallback, type Subscription } from '../subscriber.js';
import { type PublishEvent, type PublishOptions } from '../dispatcher.js';
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

export class RabbitmqTransport implements Transport, OnBusInit, OnBusShutdown {
  private readonly exchange = 'events';

  private connection?: ChannelModel;
  private channel: ConfirmChannel | undefined;
  private readonly logger = new Logger(RabbitmqTransport.name);

  public constructor(private readonly config: RabbitmqTransportConfig) {}

  /** Provides the transport with the given config. */
  public static withConfig(
    config: RabbitmqTransportConfig,
  ): TransportProvider<RabbitmqTransport> {
    return { useFactory: () => new RabbitmqTransport(config) };
  }

  public async onBusShutdown(): Promise<void> {
    await this.getConnection().close();
  }

  public async onBusInit(): Promise<void> {
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

  public async assertHealthy(): Promise<void> {
    await this.getChannel().checkExchange(this.exchange);
  }

  public publish(events: PublishEvent[]): Promise<void>;
  public publish(event: object, options?: PublishOptions): Promise<void>;
  public async publish(
    eventOrEvents: object | PublishEvent[],
    options?: PublishOptions,
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

    const handler = (message: ConsumeMessage | null) => {
      if (!message) {
        return;
      }

      const content = message.content.toString();

      this.logger.log(`Received message for event ${event.name}`);
      this.logger.verbose(`Event ${event.name} payload: ${content}`);

      Promise.resolve()
        .then(() => callback(parseEventPayload(event, content)))
        .then(() => {
          channel.ack(message);

          this.logger.log(`Event ${event.name} handled successfully`);
        })
        .catch((err) => {
          const retryCount = this.getRetryCount(message);

          this.logger.error(
            `Error handling event ${event.name} (retryCount: ${retryCount}): ${String(err)}`,
            err instanceof Error ? err.stack : undefined,
          );

          if (retryCount >= this.getRetries().max) {
            this.logger.error(
              `Event ${event.name} failed permanently after ${retryCount} retries: ${String(err)}`,
              err instanceof Error ? err.stack : undefined,
            );
            channel.ack(message);
          } else {
            channel.nack(message, false, false);
          }
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
    const queueName = `${this.config.group}.${handlerName}.${eventName}`;
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
