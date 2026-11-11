import { Inject, Logger } from '@nestjs/common';
import { type EventConstructor } from '../../types.js';
import { type Transport } from '../transport.js';
import { type BusProvider } from '../../provider.js';
import { type OnBusInit, type OnBusShutdown } from '../../lifecycle.js';
import {
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
} from 'amqplib';
import { type EventCallback } from '../subscriber.js';
import { type PublishOptions } from '../dispatcher.js';
import {
  type SerializedMessage,
  Serializer,
} from '../../serialization/serializer.js';
import {
  Telemetry,
  type TelemetryDestination,
  type TelemetryMessage,
  type TelemetryServer,
} from '../../telemetry/telemetry.js';
import { parseServer } from '../../telemetry/server.js';
import { Instrumentation } from '../../telemetry/instrumentation.js';
import { type Settlement } from '../../telemetry/metrics.js';

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

const RabbitmqTransportConfigToken = Symbol('RabbitmqTransportConfig');

/** Transport that publishes and consumes events through RabbitMQ. */
export class RabbitmqTransport implements Transport, OnBusInit, OnBusShutdown {
  private readonly exchange = 'events';

  private connection?: ChannelModel;
  private channel?: ConfirmChannel;
  private readonly logger = new Logger(RabbitmqTransport.name);
  private readonly server?: TelemetryServer;
  private readonly instrumentation: Instrumentation;

  public constructor(
    @Inject(RabbitmqTransportConfigToken)
    private readonly config: RabbitmqTransportConfig,
    @Inject(Serializer)
    private readonly serializer: Serializer,
    @Inject(Telemetry)
    telemetry: Telemetry,
  ) {
    this.instrumentation = new Instrumentation(telemetry);
    this.server = parseServer(config.url, { 'amqp:': 5672, 'amqps:': 5671 });
  }

  public static withConfig(
    config: RabbitmqTransportConfig,
  ): BusProvider<RabbitmqTransport> {
    return {
      useClass: RabbitmqTransport,
      providers: [{ provide: RabbitmqTransportConfigToken, useValue: config }],
    };
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

  public publish(event: object, options?: PublishOptions): Promise<void> {
    const {
      id,
      name: eventName,
      data,
      headers,
    } = this.serializer.serialize(event, options);

    return this.instrumentation.send(
      {
        system: 'rabbitmq',
        name: `${this.exchange}:${eventName}`,
        server: this.server,
        attributes: [
          {
            name: ['rabbitmq', 'destination', 'routing_key'],
            value: eventName,
          },
        ],
      },
      { id, name: eventName, headers, bodySize: Buffer.byteLength(data) },
      () => this.sendToBroker(id, eventName, data, headers),
    );
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

      this.logger.log(`Received message for event ${event.name}`);

      const { routingKey, deliveryTag, consumerTag } = message.fields;
      const retryCount = this.getRetryCount(message);
      const destination: TelemetryDestination = {
        system: 'rabbitmq',
        name: queue,
        consumerGroup: this.config.group,
        subscription: handlerName,
        clientId: consumerTag,
        server: this.server,
      };
      const settle = (outcome: Settlement) =>
        this.settle(channel, destination, event.name, message, outcome);

      Promise.resolve()
        .then(() => {
          const payload = this.serializer.deserialize(event, message.content);

          this.logger.verbose(`Event ${event.name} (${payload.id}):`, payload);

          // a retry would only measure the delay between the retries
          if (retryCount === 0) {
            this.instrumentation.deliver(destination, payload.publishedAt);
          }

          return this.instrumentation.process(
            destination,
            {
              id: payload.id,
              name: event.name,
              headers: this.toStringHeaders(message),
              bodySize: message.content.length,
              attempt: retryCount + 1,
              attributes: [
                ...(routingKey
                  ? [
                      {
                        name: ['rabbitmq', 'destination', 'routing_key'],
                        value: routingKey,
                      },
                    ]
                  : []),
                {
                  name: ['rabbitmq', 'message', 'delivery_tag'],
                  value: deliveryTag,
                },
              ],
            },
            async () => callback(payload),
          );
        })
        .then(
          async () => {
            await settle('acked');

            this.logger.log(`Event ${event.name} handled successfully`);
          },
          async (err) => {
            this.logger.error(
              `Error handling event ${event.name} (retryCount: ${retryCount}): ${String(err)}`,
              err instanceof Error ? err.stack : undefined,
            );

            if (retryCount >= this.getRetries().max) {
              this.logger.error(
                `Event ${event.name} failed permanently after ${retryCount} retries: ${String(err)}`,
                err instanceof Error ? err.stack : undefined,
              );
              await settle('dropped');
            } else {
              await settle('retried');
            }
          },
        )
        .catch((err) => {
          this.logger.warn(
            `Failed to settle event ${event.name}, the broker delivers it again: ${String(err)}`,
            err instanceof Error ? err.stack : undefined,
          );
        });
    };

    await channel.consume(queue, (message) => handler(message), {
      noAck: false,
    });
  }

  /**
   * Tells the broker how a delivery ended: a retry is a nack, anything else an
   * ack. The traced message is built from the AMQP message, not the payload,
   * because a message that fails to deserialize is settled too.
   */
  private settle(
    channel: ConfirmChannel,
    destination: TelemetryDestination,
    eventName: string,
    message: ConsumeMessage,
    outcome: Settlement,
  ): Promise<void> {
    const retried = outcome === 'retried';
    const traced: TelemetryMessage = {
      id: message.properties.messageId,
      name: eventName,
      headers: this.toStringHeaders(message),
      attempt: this.getRetryCount(message) + 1,
      attributes: [
        {
          name: ['rabbitmq', 'message', 'delivery_tag'],
          value: message.fields.deliveryTag,
        },
      ],
    };

    return this.instrumentation.settle(
      { ...destination, operation: retried ? 'nack' : 'ack' },
      traced,
      outcome,
      // a throw on a closed channel has to be a rejection
      () =>
        new Promise<void>((resolve) => {
          if (retried) {
            channel.nack(message, false, false);
          } else {
            channel.ack(message);
          }

          resolve();
        }),
    );
  }

  private sendToBroker(
    id: string,
    eventName: string,
    data: SerializedMessage['data'],
    headers: Record<string, string>,
  ): Promise<void> {
    const channel = this.getChannel();

    this.logger.log(`Publishing event ${eventName} (${id})`);
    this.logger.verbose(`Event ${eventName} payload:`, data);

    return new Promise<void>((resolve, reject) => {
      const published = channel.publish(
        this.exchange,
        eventName,
        Buffer.from(data),
        { messageId: id, persistent: true, headers },
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

  private toStringHeaders(message: ConsumeMessage): Record<string, string> {
    const headers: Record<string, string> = {};

    for (const [key, value] of Object.entries(
      message.properties.headers ?? {},
    )) {
      if (typeof value === 'string') {
        headers[key] = value;
      }
    }

    return headers;
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
