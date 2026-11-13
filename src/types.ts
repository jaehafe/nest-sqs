import type {
  Message as SqsMessage,
  MessageAttributeValue,
  MessageSystemAttributeName,
  QueueAttributeName,
  SQSClient,
} from '@aws-sdk/client-sqs';
import type { LoggerService, ModuleMetadata, Provider, Type } from '@nestjs/common';
import type { SqsMiddleware } from './consumer.ts';

/** Ties an entry in `consumers`/`producers` to its handler and to `SqsService` calls. */
export type QueueName = string;

export interface StopOptions {
  /**
   * Do not wait for in-flight handlers: abort their signals and the outstanding long poll at once.
   * Their messages are redelivered after the visibility timeout.
   * @default false
   */
  abort?: boolean;
}

export interface PollingErrorBackoff {
  /** @default 1000 */
  initialMs?: number;
  /** @default 2 */
  multiplier?: number;
  /** @default 10000 */
  maxMs?: number;
}

/**
 * Field names follow `sqs-consumer`'s `ConsumerOptions`, which `@ssut/nestjs-sqs` passed through,
 * so existing module options type-check unchanged. Behavior differences are in `docs/design.md`.
 */
export interface SqsConsumerOptions {
  name: QueueName;
  queueUrl: string;
  sqs?: SQSClient;
  region?: string;
  useQueueUrlAsEndpoint?: boolean;
  attributeNames?: QueueAttributeName[];
  messageAttributeNames?: string[];
  messageSystemAttributeNames?: MessageSystemAttributeName[];
  /**
   * Most messages per `ReceiveMessage` call, 1 to 10.
   * @default `concurrency` capped at 10, or 1 for a batch handler
   */
  batchSize?: number;
  /** Seconds. Defaults to the queue's own setting. */
  visibilityTimeout?: number;
  /** Long poll seconds, 0 to 20. @default 20 */
  waitTimeSeconds?: number;
  /** Milliseconds between polls while healthy. @default 0 */
  pollingWaitTimeMs?: number;
  /** Milliseconds to wait for polling to stop. Kept for compatibility. */
  pollingCompleteWaitTimeMs?: number;
  /** Milliseconds to wait before retrying after an authentication error. @default 10000 */
  authenticationErrorTimeout?: number;
  /** Seconds. Extends visibility while a handler runs. Requires `visibilityTimeout`. */
  heartbeatInterval?: number;
  /** Milliseconds before a handler is given up on and its message left for redelivery. */
  handleMessageTimeout?: number;
  /** Visibility to set when a handler fails, so the retry comes sooner than the queue timeout. */
  terminateVisibilityTimeout?: boolean | number | ((messages: SqsMessage[]) => number);
  /** @default true */
  shouldDeleteMessages?: boolean;
  /** Delete even when the handler throws. @default false */
  alwaysAcknowledge?: boolean;
  preReceiveMessageCallback?(): Promise<void>;
  postReceiveMessageCallback?(): Promise<void>;
  extendedAWSErrors?: boolean;
  suppressFifoWarning?: boolean;
  /** Accepted for compatibility. Acknowledgement no longer depends on the return value. */
  strictReturn?: boolean;
  stopOptions?: StopOptions;
  /** Runs around this consumer's handler, inside the module-wide `middleware`. */
  middleware?: SqsMiddleware[];

  /**
   * Handlers running at once for this consumer, independent of `batchSize`.
   * @default batchSize
   */
  concurrency?: number;
  /** Delay between polls after a failed `ReceiveMessage`. `false` retries immediately. */
  pollingErrorBackoff?: PollingErrorBackoff | false;
  /**
   * Milliseconds to wait for in-flight handlers on shutdown before giving up on them.
   * @default 15000
   */
  drainTimeoutMs?: number;
}

/** Field names follow `sqs-producer`, which `@ssut/nestjs-sqs` passed through. */
export interface SqsProducerOptions {
  name: QueueName;
  queueUrl: string;
  sqs?: SQSClient;
  region?: string;
  useQueueUrlAsEndpoint?: boolean;
  /** Messages per `SendMessageBatch` call, 1 to 10. @default 10 */
  batchSize?: number;
}

export interface SqsOptions {
  consumers?: SqsConsumerOptions[];
  producers?: SqsProducerOptions[];
  logger?: LoggerService;
  globalStopOptions?: StopOptions;
  /**
   * Runs around every handler, outermost first, before each consumer's own `middleware`. To use
   * providers here (a CLS service, a tracer), build it in a `registerAsync` factory.
   */
  middleware?: SqsMiddleware[];
}

export interface SqsModuleOptionsFactory {
  createOptions(): Promise<SqsOptions> | SqsOptions;
}

export interface SqsModuleAsyncOptions extends Pick<ModuleMetadata, 'imports'> {
  useExisting?: Type<SqsModuleOptionsFactory>;
  useClass?: Type<SqsModuleOptionsFactory>;
  useFactory?: (...args: any[]) => Promise<SqsOptions> | SqsOptions;
  inject?: any[];
  /** Providers the factory can inject without a separate module. */
  extraProviders?: Provider[];
}

/** A message to send. Same shape as `@ssut/nestjs-sqs`. */
export interface Message<T = any> {
  id: string;
  body: T;
  groupId?: string;
  deduplicationId?: string;
  delaySeconds?: number;
  messageAttributes?: Record<string, MessageAttributeValue>;
}

export type SqsConsumerEventName =
  | 'response_processed'
  | 'empty'
  | 'message_received'
  | 'message_processed'
  | 'error'
  | 'timeout_error'
  | 'processing_error'
  | 'aborted'
  | 'started'
  | 'stopped'
  | 'option_updated'
  | 'waiting_for_polling_to_complete'
  | 'waiting_for_polling_to_complete_timeout_exceeded';

export interface SqsMessageHandlerMeta {
  name: QueueName;
  batch?: boolean;
}

export interface SqsConsumerEventHandlerMeta {
  name: QueueName;
  eventName: SqsConsumerEventName;
}
