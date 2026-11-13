import { SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import type { Message, SQSClient } from '@aws-sdk/client-sqs';
import { Injectable } from '@nestjs/common';
import type { INestApplicationContext } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { afterEach, beforeAll, describe, expect, test } from 'vitest';
import { SqsConsumer, SqsMessageHandler, SqsModule, SqsService } from '../../src/index.ts';
import type { SqsConsumerConfig } from '../../src/index.ts';
import { waitFor } from '../fake-sqs.ts';
import { countMessages, createClient, createQueue, deleteQueue } from './elasticmq.ts';

let sqs: SQSClient;
const cleanup: Array<() => Promise<unknown>> = [];

beforeAll(() => {
  sqs = createClient();
});

afterEach(async () => {
  for (const run of cleanup.splice(0).toReversed()) await run();
});

async function queue(options?: Parameters<typeof createQueue>[1]) {
  const url = await createQueue(sqs, options);
  cleanup.push(() => deleteQueue(sqs, url));
  return url;
}

function consumer(config: Partial<SqsConsumerConfig> & { queueUrl: string }) {
  const instance = new SqsConsumer({ sqs, waitTimeSeconds: 1, ...config } as SqsConsumerConfig);
  cleanup.push(() => instance.stop({ abort: true }));
  return instance;
}

async function sendBodies(queueUrl: string, bodies: string[], groupId?: string) {
  for (let i = 0; i < bodies.length; i += 10) {
    await sqs.send(
      new SendMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: bodies.slice(i, i + 10).map((body, index) => ({
          Id: String(index),
          MessageBody: body,
          MessageGroupId: groupId,
        })),
      }),
    );
  }
}

describe('against ElasticMQ', () => {
  test('module: produce, consume and delete', async () => {
    const url = await queue();

    @Injectable()
    class Handler {
      readonly bodies: string[] = [];

      @SqsMessageHandler('orders')
      async handle(message: Message) {
        this.bodies.push(message.Body!);
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        SqsModule.register({
          consumers: [{ name: 'orders', queueUrl: url, sqs, waitTimeSeconds: 1 }],
          producers: [{ name: 'orders', queueUrl: url, sqs }],
        }),
      ],
      providers: [Handler],
    }).compile();
    const app: INestApplicationContext = await moduleRef.init();
    cleanup.push(() => app.close());

    await app.get(SqsService).send('orders', [
      { id: '1', body: { orderId: 1 } },
      { id: '2', body: { orderId: 2 } },
    ]);

    await waitFor(() => app.get(Handler).bodies.length === 2, 10_000);
    expect(app.get(Handler).bodies.toSorted()).toEqual(['{"orderId":1}', '{"orderId":2}']);
    expect(await countMessages(sqs, url)).toBe(0);
  });

  test('a failed message comes back after the visibility timeout', async () => {
    const url = await queue({ visibilityTimeout: 1 });
    await sendBodies(url, ['flaky']);
    const receiveCounts: string[] = [];
    const instance = consumer({
      queueUrl: url,
      messageSystemAttributeNames: ['ApproximateReceiveCount'],
      handleMessage: async (message) => {
        receiveCounts.push(message.Attributes!.ApproximateReceiveCount!);
        if (receiveCounts.length === 1) throw new Error('first attempt fails');
      },
    });
    instance.on('processing_error', () => {});
    instance.start();

    await waitFor(() => receiveCounts.length === 2, 10_000);
    expect(receiveCounts).toEqual(['1', '2']);
    await waitFor(() => instance.inFlightCount === 0);
    expect(await countMessages(sqs, url)).toBe(0);
  });

  test('stopping under load loses no message', async () => {
    const url = await queue({ visibilityTimeout: 2 });
    const bodies = Array.from({ length: 200 }, (_, i) => `m${i}`);
    await sendBodies(url, bodies);
    const processed = new Set<string>();
    const instance = consumer({
      queueUrl: url,
      batchSize: 10,
      concurrency: 20,
      handleMessage: async (message) => {
        await new Promise((resolve) => setTimeout(resolve, 20));
        processed.add(message.Body!);
      },
    });
    instance.start();
    await waitFor(() => processed.size >= 50, 10_000);

    await instance.stop();
    const processedAtStop = processed.size;
    // Messages handed back while stopping are visible again at once; the rest after 2 s.
    await new Promise((resolve) => setTimeout(resolve, 2_500));
    const left = await countMessages(sqs, url);

    expect(processedAtStop).toBeLessThan(200);
    expect(processedAtStop + left).toBe(200);
  });

  test('stop does not wait out the long poll', async () => {
    const url = await queue();
    const instance = consumer({
      queueUrl: url,
      waitTimeSeconds: 20,
      handleMessage: async () => {},
    });
    instance.start();
    await new Promise((resolve) => setTimeout(resolve, 200));

    const started = Date.now();
    await instance.stop();
    expect(Date.now() - started).toBeLessThan(1_000);
  });

  test('heartbeat keeps a long handler message from being redelivered', async () => {
    const url = await queue({ visibilityTimeout: 2 });
    await sendBodies(url, ['long']);
    let calls = 0;
    const instance = consumer({
      queueUrl: url,
      visibilityTimeout: 2,
      heartbeatInterval: 1,
      concurrency: 2,
      handleMessage: async () => {
        calls++;
        await new Promise((resolve) => setTimeout(resolve, 4_000));
      },
    });
    instance.start();

    await waitFor(() => calls === 1);
    await waitFor(() => instance.inFlightCount === 0, 10_000);
    expect(calls).toBe(1);
    expect(await countMessages(sqs, url)).toBe(0);
  });

  test('FIFO: one group runs in send order', async () => {
    const url = await queue({ fifo: true });
    await sendBodies(url, ['1', '2', '3', '4', '5'], 'customer-1');
    const seen: string[] = [];
    const instance = consumer({
      queueUrl: url,
      batchSize: 5,
      concurrency: 5,
      handleMessage: async (message) => {
        // Earlier messages take longer, so without ordering later ones would finish first.
        await new Promise((resolve) => setTimeout(resolve, 50 - Number(message.Body) * 10));
        seen.push(message.Body!);
      },
    });
    instance.start();

    await waitFor(() => seen.length === 5, 10_000);
    expect(seen).toEqual(['1', '2', '3', '4', '5']);
  });
});
