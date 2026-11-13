import { GetQueueAttributesCommand, SQSClient, SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import type {
  SendMessageBatchRequestEntry,
  SendMessageBatchResultEntry,
} from '@aws-sdk/client-sqs';
import type { Message, SqsProducerOptions } from './types.ts';

/** Thrown after every batch was sent when SQS rejected some entries. */
export class FailedMessagesError extends Error {
  readonly failedMessages: string[];

  constructor(failedMessages: string[]) {
    super(`Failed to send messages: ${failedMessages.join(', ')}`);
    this.name = 'FailedMessagesError';
    this.failedMessages = failedMessages;
  }
}

/** A message whose body is already a string, as `SendMessageBatch` takes it. */
export type ProducerMessage = string | Message<string>;

/** Sends messages in `SendMessageBatch` calls. Same behavior as `sqs-producer`'s `Producer`. */
export class SqsProducer {
  readonly queueUrl: string;
  readonly sqs: SQSClient;
  private readonly batchSize: number;

  constructor(options: Omit<SqsProducerOptions, 'name'>) {
    if (!options.queueUrl) throw new Error('SQS producer option `queueUrl` is required');
    const { batchSize } = options;
    if (
      batchSize !== undefined &&
      !(Number.isInteger(batchSize) && batchSize >= 1 && batchSize <= 10)
    ) {
      throw new Error('SQS producer option `batchSize` must be an integer from 1 to 10');
    }
    this.queueUrl = options.queueUrl;
    this.batchSize = batchSize ?? 10;
    this.sqs =
      options.sqs ??
      new SQSClient({
        region: options.region ?? process.env.AWS_REGION,
        useQueueUrlAsEndpoint: options.useQueueUrlAsEndpoint ?? true,
      });
  }

  /** `ApproximateNumberOfMessages` of the queue. */
  async queueSize(): Promise<number> {
    const result = await this.sqs.send(
      new GetQueueAttributesCommand({
        QueueUrl: this.queueUrl,
        AttributeNames: ['ApproximateNumberOfMessages'],
      }),
    );
    return Number(result.Attributes?.ApproximateNumberOfMessages);
  }

  /**
   * Sends every batch, then throws `FailedMessagesError` with the ids SQS rejected, if any.
   * Resolves to the successful entries.
   */
  async send(
    messages: ProducerMessage | ProducerMessage[],
  ): Promise<SendMessageBatchResultEntry[]> {
    const list = Array.isArray(messages) ? messages : [messages];
    const entries = list.map(toEntry);
    const failed: string[] = [];
    const successful: SendMessageBatchResultEntry[] = [];
    for (let start = 0; start < entries.length; start += this.batchSize) {
      const result = await this.sqs.send(
        new SendMessageBatchCommand({
          QueueUrl: this.queueUrl,
          Entries: entries.slice(start, start + this.batchSize),
        }),
      );
      for (const entry of result.Failed ?? []) if (entry.Id !== undefined) failed.push(entry.Id);
      successful.push(...(result.Successful ?? []));
    }
    if (failed.length > 0) throw new FailedMessagesError(failed);
    return successful;
  }
}

// Same checks and messages as sqs-producer's `toEntry` (src/format.ts).
function toEntry(message: ProducerMessage): SendMessageBatchRequestEntry {
  if (typeof message === 'string') return { Id: message, MessageBody: message };
  if (typeof message !== 'object' || message === null) {
    throw new Error('A message can either be an object or a string');
  }
  if (!message.body) throw new Error(`Object messages must have 'body' prop`);
  if (!message.groupId && !message.deduplicationId && !message.id) {
    throw new Error(`Object messages must have 'id' prop`);
  }
  if (message.deduplicationId && !message.groupId) {
    throw new Error(`FIFO Queue messages must have 'groupId' prop`);
  }
  if (message.id !== undefined && typeof message.id !== 'string') {
    throw new Error('Message.id value must be a string');
  }
  const entry: SendMessageBatchRequestEntry = { Id: message.id, MessageBody: message.body };
  if (message.delaySeconds) {
    if (!(message.delaySeconds >= 0 && message.delaySeconds <= 900)) {
      throw new Error('Message.delaySeconds value must be a number contained within [0 - 900]');
    }
    entry.DelaySeconds = message.delaySeconds;
  }
  if (message.messageAttributes) {
    if (typeof message.messageAttributes !== 'object') {
      throw new Error('Message.messageAttributes must be an object');
    }
    entry.MessageAttributes = message.messageAttributes;
  }
  if (message.groupId) {
    if (typeof message.groupId !== 'string')
      throw new Error('Message.groupId value must be a string');
    entry.MessageGroupId = message.groupId;
  }
  if (message.deduplicationId) {
    if (typeof message.deduplicationId !== 'string') {
      throw new Error('Message.deduplicationId value must be a string');
    }
    entry.MessageDeduplicationId = message.deduplicationId;
  }
  return entry;
}
