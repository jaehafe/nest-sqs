import type { Message } from '@aws-sdk/client-sqs';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { SqsConsumer } from '../src/consumer.ts';
import type { SqsConsumerConfig } from '../src/consumer.ts';
import { FakeSqs, deferred, waitFor } from './fake-sqs.ts';

const QUEUE = 'https://sqs.us-east-1.amazonaws.com/000000000000/orders';
const consumers: SqsConsumer[] = [];

function createConsumer(sqs: FakeSqs, config: Partial<SqsConsumerConfig>) {
  const consumer = new SqsConsumer({
    queueUrl: QUEUE,
    sqs: sqs.asClient(),
    ...config,
  } as SqsConsumerConfig);
  consumers.push(consumer);
  return consumer;
}

afterEach(async () => {
  await Promise.all(consumers.splice(0).map((consumer) => consumer.stop({ abort: true })));
});

describe('acknowledgement', () => {
  test('deletes a message when the handler resolves, whatever it returns', async () => {
    const sqs = new FakeSqs();
    sqs.add('returns undefined');
    sqs.add('returns a string');
    const results: Record<string, unknown> = {
      'returns undefined': undefined,
      'returns a string': 'ok',
    };
    const consumer = createConsumer(sqs, {
      batchSize: 2,
      handleMessage: async (message) => results[message.Body!],
    });
    consumer.start();

    await waitFor(() => sqs.deleted.length === 2);
    expect(sqs.remaining).toEqual([]);
  });

  test('keeps a message when the handler throws and reports processing_error', async () => {
    const sqs = new FakeSqs();
    sqs.add('bad');
    const errors: Array<[Error, Message | Message[]]> = [];
    const consumer = createConsumer(sqs, {
      handleMessage: async () => {
        throw new Error('boom');
      },
      terminateVisibilityTimeout: true,
    });
    consumer.on('processing_error', (error, message) => errors.push([error, message]));
    consumer.start();

    await waitFor(() => errors.length > 0);
    expect(errors[0]![0].message).toBe('boom');
    expect((errors[0]![1] as Message).Body).toBe('bad');
    await waitFor(() => sqs.callsOf('ChangeMessageVisibilityBatchCommand').length > 0);
    expect(
      sqs.callsOf('ChangeMessageVisibilityBatchCommand')[0]!.input.Entries[0].VisibilityTimeout,
    ).toBe(0);
    expect(sqs.deleted).toEqual([]);
  });

  test('alwaysAcknowledge deletes a message even when the handler throws', async () => {
    const sqs = new FakeSqs();
    sqs.add('bad');
    const consumer = createConsumer(sqs, {
      alwaysAcknowledge: true,
      handleMessage: async () => {
        throw new Error('boom');
      },
    });
    consumer.start();

    await waitFor(() => sqs.deleted.length === 1);
    expect(sqs.remaining).toEqual([]);
  });

  test('a batch handler that returns an array deletes only those messages', async () => {
    const sqs = new FakeSqs();
    sqs.add('a');
    sqs.add('b');
    sqs.add('c');
    const consumer = createConsumer(sqs, {
      batchSize: 3,
      handleMessageBatch: async (messages) => messages.filter((message) => message.Body !== 'b'),
    });
    consumer.start();

    await waitFor(() => sqs.deleted.length === 2);
    expect(sqs.deleted.toSorted()).toEqual(['a', 'c']);
    expect(sqs.remaining).toEqual(['b']);
  });
});

describe('concurrency', () => {
  test('a slow message does not hold back the next receive', async () => {
    const sqs = new FakeSqs();
    sqs.add('slow');
    sqs.add('fast-1');
    sqs.add('fast-2');
    const slow = deferred();
    const consumer = createConsumer(sqs, {
      batchSize: 1,
      concurrency: 2,
      handleMessage: async (message) => {
        if (message.Body === 'slow') await slow.promise;
      },
    });
    consumer.start();

    await waitFor(() => sqs.deleted.length === 2);
    expect(sqs.deleted).toEqual(['fast-1', 'fast-2']);
    slow.resolve();
    await waitFor(() => sqs.deleted.length === 3);
  });

  test('never runs more handlers than concurrency and asks only for free slots', async () => {
    const sqs = new FakeSqs();
    for (let i = 0; i < 20; i++) sqs.add(`m${i}`);
    let running = 0;
    let peak = 0;
    const consumer = createConsumer(sqs, {
      batchSize: 10,
      concurrency: 3,
      handleMessage: async () => {
        running++;
        peak = Math.max(peak, running);
        await new Promise((resolve) => setTimeout(resolve, 10));
        running--;
      },
    });
    consumer.start();

    await waitFor(() => sqs.deleted.length === 20);
    expect(peak).toBe(3);
    const asked = sqs
      .callsOf('ReceiveMessageCommand')
      .map((call) => call.input.MaxNumberOfMessages);
    expect(Math.max(...asked)).toBe(3);
  });
});

describe('receive errors', () => {
  test('keeps polling after a receive error even without an error listener', async () => {
    const sqs = new FakeSqs();
    sqs.receiveErrors.push(new Error('connection refused'));
    sqs.add('after error');
    const consumer = createConsumer(sqs, {
      pollingErrorBackoff: { initialMs: 10 },
      handleMessage: async () => {},
    });
    consumer.start();

    await waitFor(() => sqs.deleted.length === 1);
    expect(sqs.deleted).toEqual(['after error']);
  });

  test('backs off between failed receives and resets after a success', async () => {
    const sqs = new FakeSqs();
    sqs.receiveDelayMs = 0;
    for (let i = 0; i < 3; i++) sqs.receiveErrors.push(new Error('connection refused'));
    const errors: Error[] = [];
    const consumer = createConsumer(sqs, {
      pollingErrorBackoff: { initialMs: 20, multiplier: 2, maxMs: 40 },
      handleMessage: async () => {},
    });
    consumer.on('error', (error) => errors.push(error));
    consumer.start();

    await waitFor(() => sqs.callsOf('ReceiveMessageCommand').length >= 5);
    const at = sqs.callsOf('ReceiveMessageCommand').map((call) => call.at);
    const gaps = at.slice(1).map((time, i) => time - at[i]!);
    expect(errors).toHaveLength(3);
    // 20 ms, then 40 ms, then capped at 40 ms; after the success the next poll is immediate.
    expect(gaps[0]).toBeGreaterThanOrEqual(18);
    expect(gaps[1]).toBeGreaterThanOrEqual(38);
    expect(gaps[2]).toBeGreaterThanOrEqual(38);
    expect(gaps[3]).toBeLessThan(15);
  });
});

describe('shutdown', () => {
  test('stop waits for in-flight handlers before resolving', async () => {
    const sqs = new FakeSqs();
    sqs.add('in flight');
    const handler = deferred();
    const consumer = createConsumer(sqs, { handleMessage: () => handler.promise });
    consumer.start();
    await waitFor(() => consumer.inFlightCount === 1);

    let stopped = false;
    const stopping = consumer.stop().then(() => (stopped = true));
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(stopped).toBe(false);

    handler.resolve();
    await stopping;
    expect(sqs.deleted).toEqual(['in flight']);
  });

  test('aborts the handler signal when the drain times out', async () => {
    const sqs = new FakeSqs();
    sqs.add('stuck');
    let signal: AbortSignal | undefined;
    const consumer = createConsumer(sqs, {
      drainTimeoutMs: 20,
      handleMessage: (_message, handlerSignal) => {
        signal = handlerSignal;
        return new Promise(() => {});
      },
    });
    consumer.start();
    await waitFor(() => consumer.inFlightCount === 1);

    await consumer.stop();
    expect(signal?.aborted).toBe(true);
    expect(sqs.deleted).toEqual([]);
  });

  test('hands back messages received while stopping', async () => {
    const sqs = new FakeSqs();
    sqs.receiveDelayMs = 30;
    const handler = deferred();
    sqs.add('first');
    const consumer = createConsumer(sqs, {
      concurrency: 2,
      handleMessage: (message) => (message.Body === 'first' ? handler.promise : Promise.resolve()),
    });
    consumer.start();
    await waitFor(() => consumer.inFlightCount === 1);
    sqs.add('arrives during shutdown');

    const stopping = consumer.stop();
    await new Promise((resolve) => setTimeout(resolve, 40));
    handler.resolve();
    await stopping;

    const released = sqs
      .callsOf('ChangeMessageVisibilityBatchCommand')
      .flatMap((call) => call.input.Entries);
    expect(released).toHaveLength(1);
    expect(released[0].VisibilityTimeout).toBe(0);
    expect(sqs.deleted).toEqual(['first']);
  });
});

describe('FIFO queues', () => {
  const FIFO_QUEUE = `${QUEUE}.fifo`;

  test('runs one group in order while other groups run in parallel', async () => {
    const sqs = new FakeSqs();
    sqs.add('a1', { MessageGroupId: 'a' });
    sqs.add('a2', { MessageGroupId: 'a' });
    sqs.add('b1', { MessageGroupId: 'b' });
    const a1 = deferred();
    const order: string[] = [];
    const consumer = createConsumer(sqs, {
      queueUrl: FIFO_QUEUE,
      batchSize: 3,
      concurrency: 3,
      handleMessage: async (message) => {
        order.push(`start ${message.Body}`);
        if (message.Body === 'a1') await a1.promise;
        order.push(`end ${message.Body}`);
      },
    });
    consumer.start();

    await waitFor(() => order.includes('end b1'));
    expect(order).not.toContain('start a2');
    a1.resolve();
    await waitFor(() => sqs.deleted.length === 3);
    expect(order.indexOf('end a1')).toBeLessThan(order.indexOf('start a2'));
    expect(sqs.callsOf('ReceiveMessageCommand')[0]!.input.MessageSystemAttributeNames).toContain(
      'MessageGroupId',
    );
  });

  test('does not run later messages of a group after one fails', async () => {
    const sqs = new FakeSqs();
    sqs.add('a1', { MessageGroupId: 'a' });
    sqs.add('a2', { MessageGroupId: 'a' });
    const seen: string[] = [];
    const consumer = createConsumer(sqs, {
      queueUrl: FIFO_QUEUE,
      batchSize: 2,
      handleMessage: async (message) => {
        seen.push(message.Body!);
        throw new Error('fail');
      },
    });
    consumer.start();

    await waitFor(() => sqs.callsOf('ChangeMessageVisibilityBatchCommand').length === 1);
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(seen).toEqual(['a1']);
    expect(sqs.remaining).toEqual(['a1', 'a2']);
    // a2 is handed back at once so it does not hold the group after a1's retry.
    const released = sqs.callsOf('ChangeMessageVisibilityBatchCommand')[0]!.input.Entries;
    expect(released).toEqual([expect.objectContaining({ VisibilityTimeout: 0 })]);
  });
});

describe('handler timeout', () => {
  test('gives up on a handler that never settles and aborts its signal', async () => {
    const sqs = new FakeSqs();
    sqs.add('hangs');
    let signal: AbortSignal | undefined;
    const timeouts: Error[] = [];
    const consumer = createConsumer(sqs, {
      handleMessageTimeout: 20,
      handleMessage: (_message, handlerSignal) => {
        signal = handlerSignal;
        return new Promise(() => {});
      },
    });
    consumer.on('timeout_error', (error) => timeouts.push(error));
    consumer.start();

    await waitFor(() => timeouts.length === 1);
    expect(signal?.aborted).toBe(true);
    expect(consumer.inFlightCount).toBe(0);
    expect(sqs.deleted).toEqual([]);
  });
});

describe('options', () => {
  test.each([
    [{ batchSize: 11 }, '`batchSize`'],
    [{ concurrency: 0 }, '`concurrency`'],
    [{ waitTimeSeconds: 21 }, '`waitTimeSeconds`'],
    [{ heartbeatInterval: 10 }, 'requires `visibilityTimeout`'],
    [{ heartbeatInterval: 30, visibilityTimeout: 30 }, 'less than `visibilityTimeout`'],
  ])('rejects %j', (options, message) => {
    expect(() =>
      createConsumer(new FakeSqs(), { handleMessage: async () => {}, ...options }),
    ).toThrow(message);
  });
});

describe('heartbeat', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  test('never extends visibility past 12 hours from receipt', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval', 'Date'] });
    const sqs = new FakeSqs();
    sqs.add('very long');
    const consumer = createConsumer(sqs, {
      visibilityTimeout: 30,
      heartbeatInterval: 10,
      handleMessage: () => new Promise(() => {}),
    });
    consumer.start();
    await waitFor(() => consumer.inFlightCount === 1);

    vi.advanceTimersByTime(10_000);
    // 10 s + jump + 10 s = 43,185 s since receipt, so 15 s are left at the next tick.
    vi.setSystemTime(Date.now() + 43_165_000);
    vi.advanceTimersByTime(10_000);
    vi.advanceTimersByTime(10_000);

    const timeouts = sqs
      .callsOf('ChangeMessageVisibilityBatchCommand')
      .map((call) => call.input.Entries[0].VisibilityTimeout);
    // A normal tick, then the last 15 s that are left, then nothing.
    expect(timeouts).toEqual([30, 15]);
  });
});

describe('delete retries', () => {
  test('retries a transient delete failure', async () => {
    const sqs = new FakeSqs();
    sqs.add('m');
    sqs.deleteFailures.push(new Error('getaddrinfo EAI_AGAIN'));
    const errors: Error[] = [];
    const consumer = createConsumer(sqs, { handleMessage: async () => {} });
    consumer.on('error', (error) => errors.push(error));
    consumer.start();

    await waitFor(() => sqs.deleted.length === 1);
    expect(sqs.callsOf('DeleteMessageBatchCommand')).toHaveLength(2);
    expect(errors).toEqual([]);
  });

  test('gives up after three attempts and reports the error', async () => {
    const sqs = new FakeSqs();
    sqs.add('m');
    for (let i = 0; i < 3; i++) sqs.deleteFailures.push(new Error('getaddrinfo EAI_AGAIN'));
    const errors: Error[] = [];
    const consumer = createConsumer(sqs, { handleMessage: async () => {} });
    consumer.on('error', (error) => errors.push(error));
    consumer.start();

    await waitFor(() => errors.length === 1, 3_000);
    expect(sqs.callsOf('DeleteMessageBatchCommand')).toHaveLength(3);
  });

  test('does not retry an expired receipt handle', async () => {
    const sqs = new FakeSqs();
    sqs.add('m');
    sqs.deleteFailures.push({ Code: 'ReceiptHandleIsInvalid', SenderFault: true });
    const errors: Error[] = [];
    const consumer = createConsumer(sqs, { handleMessage: async () => {} });
    consumer.on('error', (error) => errors.push(error));
    consumer.start();

    await waitFor(() => errors.length === 1);
    expect(errors[0]!.name).toBe('ReceiptHandleIsInvalid');
    expect(sqs.callsOf('DeleteMessageBatchCommand')).toHaveLength(1);
  });
});

describe('manual acknowledgement', () => {
  test('deleteMessage deletes at once and the automatic delete skips the message', async () => {
    const sqs = new FakeSqs();
    sqs.add('early ack');
    const processed: string[] = [];
    let consumer!: SqsConsumer;
    consumer = createConsumer(sqs, {
      handleMessage: async (message) => {
        await consumer.deleteMessage(message);
        expect(sqs.deleted).toEqual(['early ack']);
      },
    });
    consumer.on('message_processed', (message) => processed.push(message.Body!));
    consumer.start();

    await waitFor(() => processed.length === 1);
    expect(sqs.callsOf('DeleteMessageCommand')).toHaveLength(1);
    expect(sqs.callsOf('DeleteMessageBatchCommand')).toHaveLength(0);
  });

  test('changeMessageVisibility sets the timeout of one message', async () => {
    const sqs = new FakeSqs();
    sqs.add('retry later');
    let consumer!: SqsConsumer;
    consumer = createConsumer(sqs, {
      handleMessage: async (message) => {
        await consumer.changeMessageVisibility(message, 120);
        throw new Error('not yet');
      },
    });
    consumer.start();

    await waitFor(() => sqs.callsOf('ChangeMessageVisibilityCommand').length === 1);
    expect(sqs.callsOf('ChangeMessageVisibilityCommand')[0]!.input.VisibilityTimeout).toBe(120);
  });
});
