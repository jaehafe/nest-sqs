import type { SQSClient } from '@aws-sdk/client-sqs';
import { describe, expect, test } from 'vitest';
import { FailedMessagesError, SqsProducer } from '../src/index.ts';

const QUEUE = 'https://sqs.us-east-1.amazonaws.com/000000000000/orders';

function fakeClient(failIds: string[] = []) {
  const batches: any[][] = [];
  const client = {
    send: async (command: { input: any }) => {
      const entries = command.input.Entries;
      batches.push(entries);
      return {
        Successful: entries
          .filter((entry: any) => !failIds.includes(entry.Id))
          .map((entry: any) => ({ Id: entry.Id, MessageId: `sqs-${entry.Id}` })),
        Failed: entries
          .filter((entry: any) => failIds.includes(entry.Id))
          .map((entry: any) => ({ Id: entry.Id, Code: 'InternalError', SenderFault: false })),
      };
    },
  } as unknown as SQSClient;
  return { client, batches };
}

describe('SqsProducer', () => {
  test('splits messages into batches of batchSize', async () => {
    const { client, batches } = fakeClient();
    const producer = new SqsProducer({ queueUrl: QUEUE, sqs: client, batchSize: 2 });
    const result = await producer.send(['a', 'b', 'c']);

    expect(batches.map((batch) => batch.map((entry) => entry.Id))).toEqual([['a', 'b'], ['c']]);
    expect(result.map((entry) => entry.Id)).toEqual(['a', 'b', 'c']);
  });

  test('sends every batch, then throws with the ids SQS rejected', async () => {
    const { client, batches } = fakeClient(['a', 'c']);
    const producer = new SqsProducer({ queueUrl: QUEUE, sqs: client, batchSize: 2 });
    const error = await producer.send(['a', 'b', 'c']).catch((caught: unknown) => caught);

    expect(batches).toHaveLength(2);
    expect(error).toBeInstanceOf(FailedMessagesError);
    expect((error as FailedMessagesError).failedMessages).toEqual(['a', 'c']);
  });

  test('maps message fields to SendMessageBatch entries', async () => {
    const { client, batches } = fakeClient();
    const producer = new SqsProducer({ queueUrl: QUEUE, sqs: client });
    await producer.send({
      id: '1',
      body: '{}',
      groupId: 'g',
      deduplicationId: 'd',
      delaySeconds: 5,
      messageAttributes: { kind: { DataType: 'String', StringValue: 'order' } },
    });

    expect(batches[0]![0]).toEqual({
      Id: '1',
      MessageBody: '{}',
      MessageGroupId: 'g',
      MessageDeduplicationId: 'd',
      DelaySeconds: 5,
      MessageAttributes: { kind: { DataType: 'String', StringValue: 'order' } },
    });
  });

  test.each([
    [{ id: '1', body: '' }, `must have 'body'`],
    [{ body: 'x' }, `must have 'id'`],
    [{ id: '1', body: 'x', deduplicationId: 'd' }, `must have 'groupId'`],
    [{ id: '1', body: 'x', delaySeconds: 901 }, 'delaySeconds'],
  ])('rejects %j before sending', async (message, error) => {
    const { client, batches } = fakeClient();
    const producer = new SqsProducer({ queueUrl: QUEUE, sqs: client });

    await expect(producer.send(message as never)).rejects.toThrow(error);
    expect(batches).toEqual([]);
  });
});
