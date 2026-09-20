import {
  Inject,
  Injectable,
  Logger,
  BeforeApplicationShutdown,
  type OnModuleInit,
} from '@nestjs/common';
import { plainToInstance, instanceToPlain } from 'class-transformer';
import { type EventConstructor, BusConfig } from '../../types.js';
import { Transport } from '../transport.js';
import {
  connect,
  Connection,
  type ConfirmChannel,
  type ConsumeMessage,
} from 'amqplib';
import { type EventCallback } from '../subscriber.js';
import { PublishOptions } from '../dispatcher.js';

export interface EventDto {
  id: string;
  name: string;
  event: Record<string, string | number | undefined | null>;
  meta: Record<string, string | number | undefined | null>;
  publishedAt: Date;
}

export interface RabbitmqTransportConfig {
  url: string;
}

@Injectable()
export class RabbitmqTransport
  extends Transport<RabbitmqTransportConfig>
  implements OnModuleInit, BeforeApplicationShutdown
{
  private readonly exchange = 'events';

  private connection?: Connection;
  private channel: ConfirmChannel | undefined;
  private readonly logger = new Logger(RabbitmqTransport.name);

  public constructor(@Inject(BusConfig) private readonly busConfig: BusConfig) {
    super();
  }

  public async beforeApplicationShutdown(): Promise<void> {
    await this.getConnection().close();
  }

  public async onModuleInit(): Promise<void> {
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

  public publish<E extends object>(
    event: E,
    options?: PublishOptions,
  ): Promise<void> {
    const eventName = event.constructor.name;

    const payload: EventDto = {
      id: options?.id ?? crypto.randomUUID(),
      name: eventName,
      event: instanceToPlain(event),
      meta: options?.meta ?? {},
      publishedAt: new Date(),
    };

    return this.publishEvent(eventName, payload);
  }

  public async subscribe<
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
        .then(() => {
          const dto = JSON.parse(content) as EventDto;
          const eventInstance = plainToInstance(event, dto.event, {
            enableImplicitConversion: true,
          });
          return callback({
            ...dto,
            event: eventInstance,
            publishedAt: new Date(dto.publishedAt),
          });
        })
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

          if (retryCount >= this.busConfig.retries.max) {
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

  public async assertHealthy(): Promise<void> {
    await this.getChannel().checkExchange(this.exchange);
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
    const queueName = `${this.busConfig.group}.${handlerName}.${eventName}`;
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
        'x-message-ttl': this.busConfig.retries.delay,
      },
    });

    await channel.bindQueue(
      delayQueue.queue,
      `${this.exchange}.delay`,
      delayQueueName,
    );

    return queue.queue;
  }

  private getRetryCount(message: ConsumeMessage): number {
    const xDeath = message.properties.headers?.['x-death'];

    if (!Array.isArray(xDeath)) {
      return 0;
    }

    return xDeath
      .filter((entry: any) => entry.reason === 'rejected')
      .reduce((sum: number, entry: any) => sum + (entry.count ?? 0), 0);
  }

  private getChannel(): ConfirmChannel {
    if (!this.channel) {
      throw new Error(`${this.constructor.name} not initialized`);
    }

    return this.channel;
  }

  private getConnection(): Connection {
    if (!this.connection) {
      throw new Error(`${this.constructor.name} not initialized`);
    }

    return this.connection;
  }
}
