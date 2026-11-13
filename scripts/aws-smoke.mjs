// Smoke test against real AWS SQS. ElasticMQ can differ from SQS on error codes and limits, so this
// checks the behavior that depends on them.
//
//   pnpm build && AWS_PROFILE=<profile> AWS_REGION=<region> node scripts/aws-smoke.mjs
//
// It creates two queues named `nest-sqs-smoke-<random>` (one FIFO), and deletes them at the end,
// also on failure. Set SQS_ENDPOINT to run it against ElasticMQ instead. Cost: a few hundred
// requests.
import { randomUUID } from 'node:crypto';
import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { SqsConsumer, SqsProducer } from '../dist/index.mjs';

const endpoint = process.env.SQS_ENDPOINT;
const sqs = new SQSClient(
  endpoint
    ? { endpoint, region: 'us-east-1', credentials: { accessKeyId: 'x', secretAccessKey: 'x' } }
    : {},
);
const prefix = `nest-sqs-smoke-${randomUUID().slice(0, 8)}`;
const queues = [];
const results = [];

async function createQueue(suffix, attributes) {
  const { QueueUrl } = await sqs.send(
    new CreateQueueCommand({ QueueName: `${prefix}-${suffix}`, Attributes: attributes }),
  );
  queues.push(QueueUrl);
  return QueueUrl;
}

async function count(queueUrl) {
  const { Attributes } = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    }),
  );
  return (
    Number(Attributes.ApproximateNumberOfMessages) +
    Number(Attributes.ApproximateNumberOfMessagesNotVisible)
  );
}

async function waitFor(check, timeoutMs, what) {
  const start = Date.now();
  while (!(await check())) {
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

async function check(name, run) {
  const started = Date.now();
  try {
    const detail = await run();
    results.push({
      check: name,
      result: 'pass',
      detail: detail ?? '',
      seconds: (Date.now() - started) / 1000,
    });
  } catch (error) {
    results.push({
      check: name,
      result: 'FAIL',
      detail: error.message,
      seconds: (Date.now() - started) / 1000,
    });
  }
}

function consumer(queueUrl, config) {
  const instance = new SqsConsumer({ queueUrl, sqs, waitTimeSeconds: 2, ...config });
  instance.on('error', () => {});
  instance.on('processing_error', () => {});
  return instance;
}

try {
  const standard = await createQueue('standard', { VisibilityTimeout: '2' });
  const fifo = await createQueue('order.fifo', {
    FifoQueue: 'true',
    ContentBasedDeduplication: 'true',
    VisibilityTimeout: '2',
  });

  await check('produce 25 and consume with concurrency 5', async () => {
    const producer = new SqsProducer({ queueUrl: standard, sqs });
    await producer.send(Array.from({ length: 25 }, (_, i) => ({ id: String(i), body: String(i) })));
    const seen = new Set();
    const instance = consumer(standard, {
      batchSize: 10,
      concurrency: 5,
      handleMessage: async (message) => {
        seen.add(message.Body);
      },
    });
    instance.start();
    await waitFor(async () => seen.size === 25 && (await count(standard)) === 0, 60_000, 'all 25');
    await instance.stop();
    return '25 handled, queue empty';
  });

  await check('a failed message is redelivered and then deleted', async () => {
    await new SqsProducer({ queueUrl: standard, sqs }).send({ id: 'flaky', body: 'flaky' });
    const counts = [];
    const instance = consumer(standard, {
      messageSystemAttributeNames: ['ApproximateReceiveCount'],
      handleMessage: async (message) => {
        counts.push(message.Attributes.ApproximateReceiveCount);
        if (counts.length === 1) throw new Error('first attempt fails');
      },
    });
    instance.start();
    await waitFor(
      async () => counts.length === 2 && (await count(standard)) === 0,
      30_000,
      'retry',
    );
    await instance.stop();
    return `receive counts ${counts.join(', ')}`;
  });

  await check('heartbeat keeps a 5 s handler from being redelivered (visibility 2 s)', async () => {
    await new SqsProducer({ queueUrl: standard, sqs }).send({ id: 'long', body: 'long' });
    let calls = 0;
    const instance = consumer(standard, {
      visibilityTimeout: 2,
      heartbeatInterval: 1,
      concurrency: 2,
      handleMessage: async () => {
        calls++;
        await new Promise((resolve) => setTimeout(resolve, 5_000));
      },
    });
    instance.start();
    await waitFor(async () => calls === 1 && instance.inFlightCount === 0, 30_000, 'handler');
    await new Promise((resolve) => setTimeout(resolve, 3_000));
    await instance.stop();
    if (calls !== 1) throw new Error(`handler ran ${calls} times`);
    return 'ran once';
  });

  await check('FIFO: one group in send order', async () => {
    const producer = new SqsProducer({ queueUrl: fifo, sqs });
    await producer.send(
      ['1', '2', '3', '4', '5'].map((body) => ({ id: body, body, groupId: 'customer-1' })),
    );
    const seen = [];
    const instance = consumer(fifo, {
      batchSize: 5,
      concurrency: 5,
      handleMessage: async (message) => {
        await new Promise((resolve) => setTimeout(resolve, 50 - Number(message.Body) * 10));
        seen.push(message.Body);
      },
    });
    instance.start();
    await waitFor(async () => seen.length === 5, 30_000, 'five messages');
    await instance.stop();
    if (seen.join() !== '1,2,3,4,5') throw new Error(`order ${seen.join()}`);
    return seen.join(' ');
  });

  // Delete retries skip error names that cannot succeed; check SQS reports the name we list.
  await check('an expired receipt handle is reported as ReceiptHandleIsInvalid', async () => {
    await new SqsProducer({ queueUrl: standard, sqs }).send({ id: 'stale', body: 'stale' });
    const { Messages } = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl: standard, WaitTimeSeconds: 2, VisibilityTimeout: 1 }),
    );
    const receipt = Messages[0].ReceiptHandle;
    await sqs.send(new DeleteMessageCommand({ QueueUrl: standard, ReceiptHandle: receipt }));
    try {
      await sqs.send(
        new DeleteMessageCommand({ QueueUrl: standard, ReceiptHandle: `${receipt}tampered` }),
      );
    } catch (error) {
      if (error.name !== 'ReceiptHandleIsInvalid') throw new Error(`got ${error.name}`);
      return error.name;
    }
    throw new Error('no error for a tampered receipt handle');
  });

  // Receive errors for a missing queue back off for authenticationErrorTimeout; check the name.
  await check('a missing queue is reported with a known connection error name', async () => {
    const missing = standard.replace(/[^/]+$/, `${prefix}-missing`);
    try {
      await sqs.send(new ReceiveMessageCommand({ QueueUrl: missing, WaitTimeSeconds: 0 }));
    } catch (error) {
      const known = ['QueueDoesNotExist', 'AWS.SimpleQueueService.NonExistentQueue'];
      if (!known.includes(error.name)) throw new Error(`got ${error.name}`);
      return error.name;
    }
    throw new Error('no error for a missing queue');
  });
} finally {
  for (const queueUrl of queues) {
    await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl })).catch(() => {});
  }
  console.table(results);
  console.log(
    `queues deleted: ${queues.map((url) => url.slice(url.lastIndexOf('/') + 1)).join(', ')}`,
  );
}
process.exitCode = results.every((result) => result.result === 'pass') ? 0 : 1;
