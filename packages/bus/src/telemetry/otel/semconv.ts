/**
 * Names of the OpenTelemetry semantic conventions for messaging that are
 * used. Messaging conventions are still in development, so they are kept here
 * instead of coming from `@opentelemetry/semantic-conventions`.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/messaging/
 */
export const Attr = {
  system: 'messaging.system',
  operationName: 'messaging.operation.name',
  operationType: 'messaging.operation.type',
  destinationName: 'messaging.destination.name',
  subscriptionName: 'messaging.destination.subscription.name',
  consumerGroup: 'messaging.consumer.group.name',
  messageId: 'messaging.message.id',
  batchCount: 'messaging.batch.message_count',
  clientId: 'messaging.client.id',
  serverAddress: 'server.address',
  serverPort: 'server.port',
  errorType: 'error.type',
  exceptionType: 'exception.type',
  exceptionMessage: 'exception.message',
  exceptionStacktrace: 'exception.stacktrace',
} as const;

export const Metric = {
  operationDuration: 'messaging.client.operation.duration',
  sentMessages: 'messaging.client.sent.messages',
  consumedMessages: 'messaging.client.consumed.messages',
  processDuration: 'messaging.process.duration',
} as const;

/** Bucket boundaries the conventions advise for duration histograms. */
export const DURATION_BUCKETS = [
  0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
];

/** Value of `error.type` when there is no better one. */
export const ERROR_TYPE_OTHER = '_OTHER';
