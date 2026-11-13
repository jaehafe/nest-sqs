// One failure scenario against one library: `node fault-worker.mjs <library> <scenario>`.
// Prints one JSON line. Run through faults.mjs, which needs ElasticMQ on :9324.
// oxlint-disable-next-line import/no-unassigned-import -- Nest needs the Reflect polyfill loaded first.
import 'reflect-metadata';
import { randomUUID } from 'node:crypto';
import net from 'node:net';
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  SQSClient,
  SendMessageBatchCommand,
} from '@aws-sdk/client-sqs';
import { Injectable, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';

const [library, scenario] = process.argv.slice(2);
// Many production apps log unhandled rejections instead of crashing; SURVIVE=1 runs that way.
if (process.env.SURVIVE) process.on('unhandledRejection', () => {});
const lib = await import(library === 'nest-sqs' ? 'nest-sqs' : '@ssut/nestjs-sqs');
const ELASTICMQ_PORT = 9324;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const waitUntil = async (check, ms) => {
  const end = Date.now() + ms;
  while (!check() && Date.now() < end) await sleep(20);
  return check();
};

function client(port = ELASTICMQ_PORT) {
  return new SQSClient({
    endpoint: `http://localhost:${port}`,
    region: 'us-east-1',
    credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
    maxAttempts: 1,
  });
}
const admin = client();

async function queue({ fifo = false, visibility = 2 } = {}) {
  const attributes = { VisibilityTimeout: String(visibility) };
  if (fifo) Object.assign(attributes, { FifoQueue: 'true', ContentBasedDeduplication: 'true' });
  const { QueueUrl } = await admin.send(
    new CreateQueueCommand({
      QueueName: `fault-${randomUUID()}${fifo ? '.fifo' : ''}`,
      Attributes: attributes,
    }),
  );
  return QueueUrl;
}

async function send(queueUrl, bodies, groupId) {
  for (let i = 0; i < bodies.length; i += 10) {
    await admin.send(
      new SendMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: bodies
          .slice(i, i + 10)
          .map((body, j) => ({ Id: String(j), MessageBody: body, MessageGroupId: groupId })),
      }),
    );
  }
}

async function left(queueUrl) {
  const { Attributes } = await admin.send(
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

// Builds a Nest app with one consumer named 'q' handled by `handle`, plus extra providers.
async function app(consumer, handle, extraProviders = []) {
  class Handler {
    async handle(message) {
      return handle.call(this, message);
    }
  }
  Injectable()(Handler);
  lib.SqsMessageHandler('q', consumer.batch === true)(
    Handler.prototype,
    'handle',
    Object.getOwnPropertyDescriptor(Handler.prototype, 'handle'),
  );
  // ERROR_HANDLER=1 registers an `error` event handler, the documented way to keep a consumer
  // polling after SQS call failures with @ssut/nestjs-sqs.
  if (process.env.ERROR_HANDLER) {
    class ErrorListener {
      onError() {}
    }
    Injectable()(ErrorListener);
    lib.SqsConsumerEventHandler('q', 'error')(
      ErrorListener.prototype,
      'onError',
      Object.getOwnPropertyDescriptor(ErrorListener.prototype, 'onError'),
    );
    extraProviders = [...extraProviders, ErrorListener];
  }
  // oxlint-disable-next-line typescript/no-extraneous-class -- a Nest module class is empty by design.
  class AppModule {}
  const { batch: _batch, ...options } = consumer;
  Module({
    imports: [
      lib.SqsModule.register({ consumers: [{ name: 'q', waitTimeSeconds: 1, ...options }] }),
    ],
    providers: [Handler, ...extraProviders],
  })(AppModule);
  const instance = await NestFactory.createApplicationContext(AppModule, { logger: false });
  return { instance, handler: instance.get(Handler) };
}

const scenarios = {
  // app.close() while handlers run: how many handlers are still running after close returns, that
  // is, after the app would have closed its database connections.
  async shutdown() {
    const queueUrl = await queue({ visibility: 2 });
    await send(
      queueUrl,
      Array.from({ length: 30 }, (_, i) => String(i)),
    );
    let started = 0;
    let finishedAfterClose = 0;
    let closed = false;
    const { instance } = await app(
      { queueUrl, sqs: admin, batchSize: 10, concurrency: 10 },
      async () => {
        started++;
        await sleep(500);
        if (closed) finishedAfterClose++;
      },
    );
    await waitUntil(() => started >= 5, 5_000);
    await instance.close();
    closed = true;
    await sleep(1_000);
    await sleep(2_500); // every message not deleted is visible again
    const remaining = await left(queueUrl);
    await admin.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    return { handlersRunningAfterClose: finishedAfterClose, startedHandlers: started, remaining };
  },

  // SQS unreachable for 3 s, then back: receive calls made during the outage, and whether a message
  // sent after it is consumed within 15 s.
  async outage() {
    const queueUrl = await queue();
    let up = true;
    const sockets = new Set();
    const proxy = net.createServer((socket) => {
      if (!up) return socket.destroy();
      const upstream = net.connect(ELASTICMQ_PORT, 'localhost');
      sockets.add(socket);
      socket.pipe(upstream).pipe(socket);
      socket.on('error', () => {});
      upstream.on('error', () => socket.destroy());
    });
    await new Promise((resolve) => proxy.listen(0, resolve));
    const sqs = client(proxy.address().port);
    let receives = 0;
    sqs.middlewareStack.add(
      (next, context) => (args) => {
        if (context.commandName === 'ReceiveMessageCommand' && !up) receives++;
        return next(args);
      },
      { step: 'initialize' },
    );
    let consumed = 0;
    const { instance } = await app({ queueUrl, sqs, waitTimeSeconds: 0 }, async () => {
      consumed++;
    });
    await sleep(300);
    up = false;
    for (const socket of sockets) socket.destroy();
    await sleep(3_000);
    up = true;
    await send(queueUrl, ['after outage']);
    const recovered = await waitUntil(() => consumed === 1, 15_000);
    await instance.close();
    proxy.close();
    await admin.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    return { receiveCallsDuringOutage: receives, recoveredWithin15s: recovered };
  },

  // The first delete fails like a DNS hiccup: how many times the handler runs for one message.
  async deleteHiccup() {
    const queueUrl = await queue({ visibility: 1 });
    const sqs = client();
    let failNextDelete = true;
    sqs.middlewareStack.add(
      (next, context) => async (args) => {
        if (context.commandName.startsWith('DeleteMessage') && failNextDelete) {
          failNextDelete = false;
          const error = new Error('getaddrinfo EAI_AGAIN sqs.us-east-1.amazonaws.com');
          error.name = 'Error';
          error.code = 'EAI_AGAIN';
          throw error;
        }
        return next(args);
      },
      { step: 'initialize' },
    );
    let runs = 0;
    const { instance } = await app({ queueUrl, sqs }, async () => {
      runs++;
    });
    await send(queueUrl, ['once']);
    await waitUntil(() => runs >= 1, 5_000);
    await sleep(3_000); // past the 1 s visibility timeout
    const runsForFirst = runs;
    // Is the consumer still polling after the failed delete?
    await send(queueUrl, ['later']);
    const stillConsuming = await waitUntil(() => runs > runsForFirst, 10_000);
    await instance.close();
    await admin.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    return { handlerRuns: runsForFirst, stillConsuming };
  },

  // One FIFO group of 5 in one receive; earlier messages are slower.
  async fifo() {
    const queueUrl = await queue({ fifo: true });
    await send(queueUrl, ['1', '2', '3', '4', '5'], 'customer-1');
    const order = [];
    const { instance } = await app(
      { queueUrl, sqs: admin, batchSize: 5, concurrency: 5 },
      async (message) => {
        await sleep(60 - Number(message.Body) * 10);
        order.push(message.Body);
      },
    );
    await waitUntil(() => order.length === 5, 10_000);
    await instance.close();
    await admin.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    return { order: order.join(' '), inOrder: order.join() === '1,2,3,4,5' };
  },

  // A method wrapped after boot, as tracing or transaction decorators do: which one runs.
  async wrappedAfterBoot() {
    const queueUrl = await queue();
    let ran = 'none';
    const { instance, handler } = await app({ queueUrl, sqs: admin }, async () => {
      if (ran === 'none') ran = 'original';
    });
    const original = handler.handle.bind(handler);
    handler.handle = async (message) => {
      ran = 'wrapped';
      return original(message);
    };
    await send(queueUrl, ['m']);
    await waitUntil(() => ran !== 'none', 5_000);
    await instance.close();
    await admin.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    return { ran };
  },

  // An error event handler in a different provider than the message handler, using `this`.
  async eventHandlerInstance() {
    const queueUrl = await queue();
    class Monitor {
      errors = [];
      onError(error) {
        this.errors.push(error.message);
      }
    }
    Injectable()(Monitor);
    lib.SqsConsumerEventHandler('q', 'processing_error')(
      Monitor.prototype,
      'onError',
      Object.getOwnPropertyDescriptor(Monitor.prototype, 'onError'),
    );
    const { instance } = await app(
      { queueUrl, sqs: admin },
      async () => {
        throw new Error('boom');
      },
      [Monitor],
    );
    await send(queueUrl, ['m']);
    const monitor = instance.get(Monitor);
    const recorded = await waitUntil(() => monitor.errors.length > 0, 3_000);
    await instance.close();
    await admin.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    return { monitorRecordedError: recorded };
  },
};

const result = await scenarios[scenario]();
console.log(JSON.stringify(result));
process.exit(0);
