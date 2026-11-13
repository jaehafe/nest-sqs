import { randomUUID } from 'node:crypto';
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';

/** ElasticMQ endpoint. Start one with `docker run -p 9324:9324 softwaremill/elasticmq-native`. */
export const ENDPOINT: string = process.env.ELASTICMQ_URL ?? 'http://localhost:9324';

export function createClient(): SQSClient {
  return new SQSClient({
    endpoint: ENDPOINT,
    region: 'us-east-1',
    credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
  });
}

export async function createQueue(
  sqs: SQSClient,
  options: { fifo?: boolean; visibilityTimeout?: number } = {},
): Promise<string> {
  const name = `test-${randomUUID()}${options.fifo ? '.fifo' : ''}`;
  const attributes: Record<string, string> = {
    VisibilityTimeout: String(options.visibilityTimeout ?? 30),
  };
  if (options.fifo) {
    attributes.FifoQueue = 'true';
    attributes.ContentBasedDeduplication = 'true';
  }
  const { QueueUrl } = await sqs.send(
    new CreateQueueCommand({ QueueName: name, Attributes: attributes }),
  );
  return QueueUrl!;
}

export async function deleteQueue(sqs: SQSClient, queueUrl: string): Promise<void> {
  await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
}

/** Visible plus in-flight messages. */
export async function countMessages(sqs: SQSClient, queueUrl: string): Promise<number> {
  const { Attributes } = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    }),
  );
  return (
    Number(Attributes?.ApproximateNumberOfMessages) +
    Number(Attributes?.ApproximateNumberOfMessagesNotVisible)
  );
}
