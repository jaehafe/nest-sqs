import { EventEmitter } from 'node:events';
import {
  ChangeMessageVisibilityBatchCommand,
  DeleteMessageBatchCommand,
  ReceiveMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import type { Message, MessageSystemAttributeName } from '@aws-sdk/client-sqs';
import type { SqsConsumerOptions, StopOptions } from './types.ts';

/** Resolve to delete the message. Throw to leave it for redelivery. */
export type MessageHandler = (message: Message, signal: AbortSignal) => Promise<unknown>;

/**
 * Resolve to delete the messages. Return an array to delete only those (partial success).
 * Throw to leave all of them for redelivery.
 */
export type BatchMessageHandler = (messages: Message[], signal: AbortSignal) => Promise<unknown>;

export interface SqsConsumerEvents {
  started: [];
  stopped: [];
  empty: [];
  response_processed: [];
  message_received: [Message];
  message_processed: [Message];
  /** SQS call failed. `message` is set for delete and visibility errors, unset for receive errors. */
  error: [Error, (Message | Message[])?];
  processing_error: [Error, Message | Message[]];
  timeout_error: [Error, Message];
  aborted: [];
}

export type SqsConsumerConfig = Omit<SqsConsumerOptions, 'name' | 'stopOptions'> &
  (
    | { handleMessage: MessageHandler; handleMessageBatch?: never }
    | { handleMessage?: never; handleMessageBatch: BatchMessageHandler }
  );

export class HandlerTimeoutError extends Error {
  override name = 'TimeoutError';
}

// Errors that will not go away by retrying right away. sqs-consumer waits
// `authenticationErrorTimeout` after these (src/errors.ts `CONNECTION_ERRORS`).
const CONNECTION_ERRORS = new Set([
  'CredentialsError',
  'UnknownEndpoint',
  'AWS.SimpleQueueService.NonExistentQueue',
  'CredentialsProviderError',
  'InvalidAddress',
  'InvalidSecurity',
  'QueueDoesNotExist',
  'RequestThrottled',
  'OverLimit',
]);

// Spring Cloud AWS default poll back-off (AbstractContainerOptions: 1000 ms, x2, 10000 ms).
const DEFAULT_BACKOFF = { initialMs: 1_000, multiplier: 2, maxMs: 10_000 };
const DEFAULT_DRAIN_TIMEOUT_MS = 15_000;
const SQS_MAX_BATCH = 10;

/**
 * Polls one queue and runs a handler per message (or per batch) with at most `concurrency`
 * handlers in flight. It only asks SQS for as many messages as it has free slots, so it never holds
 * more messages than it can run.
 */
export class SqsConsumer extends EventEmitter<SqsConsumerEvents> {
  readonly queueUrl: string;
  readonly sqs: SQSClient;
  readonly isFifo: boolean;

  private readonly config: SqsConsumerConfig;
  private readonly batchSize: number;
  private readonly concurrency: number;
  private readonly waitTimeSeconds: number;
  private readonly drainTimeoutMs: number;
  private readonly backoff: Required<NonNullable<SqsConsumerOptions['pollingErrorBackoff']>> | null;

  private running = false;
  private loopDone: Promise<void> = Promise.resolve();
  private receiveController = new AbortController();
  private handlerController = new AbortController();
  private readonly inFlight = new Set<Promise<void>>();
  private slotWaiters: Array<() => void> = [];
  private nextBackoffMs = 0;

  constructor(config: SqsConsumerConfig) {
    super();
    validate(config);
    this.config = config;
    this.queueUrl = config.queueUrl;
    this.isFifo = config.queueUrl.endsWith('.fifo');
    this.batchSize = config.batchSize ?? 1;
    this.concurrency = config.concurrency ?? (config.handleMessageBatch ? 1 : this.batchSize);
    this.waitTimeSeconds = config.waitTimeSeconds ?? 20;
    this.drainTimeoutMs = config.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS;
    this.backoff =
      config.pollingErrorBackoff === false
        ? null
        : { ...DEFAULT_BACKOFF, ...config.pollingErrorBackoff };
    this.sqs =
      config.sqs ??
      new SQSClient({
        region: config.region ?? process.env.AWS_REGION,
        useQueueUrlAsEndpoint: config.useQueueUrlAsEndpoint ?? true,
      });
  }

  get isRunning(): boolean {
    return this.running;
  }

  /** Handlers currently running, including ones waiting behind an earlier message of their FIFO group. */
  get inFlightCount(): number {
    return this.inFlight.size;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.receiveController = new AbortController();
    this.handlerController = new AbortController();
    this.safeEmit('started');
    this.loopDone = this.loop();
  }

  /**
   * Stops polling, waits up to `drainTimeoutMs` for in-flight handlers, then aborts the outstanding
   * long poll. With `abort: true` it does not wait: handlers get an aborted signal right away.
   */
  async stop(options: StopOptions = {}): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.releaseSlotWaiters();

    if (options.abort) {
      this.handlerController.abort();
    } else {
      await this.drain();
    }
    this.receiveController.abort();
    if (options.abort) this.safeEmit('aborted');
    await this.loopDone;
    this.safeEmit('stopped');
  }

  private async drain(): Promise<void> {
    if (this.inFlight.size === 0) return;
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<'timeout'>((resolve) => {
      timer = setTimeout(() => resolve('timeout'), this.drainTimeoutMs);
    });
    const result = await Promise.race([
      Promise.allSettled(this.inFlight).then(() => 'drained' as const),
      timeout,
    ]);
    clearTimeout(timer);
    if (result === 'timeout') {
      this.handlerController.abort();
    }
  }

  private async loop(): Promise<void> {
    while (this.running) {
      await this.waitForSlot();
      if (!this.running) break;

      const free = this.concurrency - this.inFlight.size;
      const max = this.config.handleMessageBatch ? this.batchSize : Math.min(this.batchSize, free);

      let messages: Message[];
      try {
        messages = await this.receive(max);
      } catch (error) {
        if (!this.running && this.receiveController.signal.aborted) break;
        this.safeEmit('error', toError(error));
        await this.sleep(this.receiveErrorDelay(error));
        continue;
      }
      this.nextBackoffMs = 0;

      // A receive that completed while stopping: hand the messages back right away instead of
      // leaving them invisible until their visibility timeout runs out.
      if (!this.running) {
        await this.changeVisibility(messages, 0);
        break;
      }

      if (messages.length === 0) {
        this.safeEmit('empty');
      } else {
        this.dispatch(messages);
      }

      if (this.config.pollingWaitTimeMs) await this.sleep(this.config.pollingWaitTimeMs);
    }
  }

  private async receive(max: number): Promise<Message[]> {
    await this.config.preReceiveMessageCallback?.();
    const systemAttributes = new Set<MessageSystemAttributeName>(
      this.config.messageSystemAttributeNames ?? [],
    );
    if (this.isFifo) systemAttributes.add('MessageGroupId');
    const output = await this.sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: this.queueUrl,
        MaxNumberOfMessages: max,
        WaitTimeSeconds: this.waitTimeSeconds,
        VisibilityTimeout: this.config.visibilityTimeout,
        AttributeNames: this.config.attributeNames,
        MessageAttributeNames: this.config.messageAttributeNames,
        MessageSystemAttributeNames: [...systemAttributes],
      }),
      { abortSignal: this.receiveController.signal },
    );
    await this.config.postReceiveMessageCallback?.();
    return output.Messages ?? [];
  }

  private receiveErrorDelay(error: unknown): number {
    let delay = 0;
    if (this.backoff) {
      this.nextBackoffMs = this.nextBackoffMs
        ? Math.min(this.nextBackoffMs * this.backoff.multiplier, this.backoff.maxMs)
        : this.backoff.initialMs;
      delay = this.nextBackoffMs;
    }
    if (isConnectionError(error)) {
      delay = Math.max(delay, this.config.authenticationErrorTimeout ?? 10_000);
    }
    return delay;
  }

  private dispatch(messages: Message[]): void {
    let tasks: Promise<void>[];
    if (this.config.handleMessageBatch) {
      tasks = [this.track(() => this.processBatch(messages))];
    } else if (this.isFifo) {
      tasks = this.dispatchFifo(messages);
    } else {
      tasks = messages.map((message) => this.track(() => this.processMessage(message)));
    }
    void Promise.allSettled(tasks).then(() => this.safeEmit('response_processed'));
  }

  // Messages of one group run one after another in receive order; groups run in parallel. Once a
  // message fails, later messages of its group are not run, so they cannot overtake it. They stay
  // invisible until their visibility timeout ends and come back after the failed one.
  private dispatchFifo(messages: Message[]): Promise<void>[] {
    const lastOfGroup = new Map<string, Promise<boolean>>();
    return messages.map((message) => {
      const groupId = message.Attributes?.MessageGroupId ?? '';
      const previous = lastOfGroup.get(groupId);
      const result = previous
        ? previous.then((ok) => (ok ? this.processMessage(message) : false))
        : this.processMessage(message);
      lastOfGroup.set(groupId, result);
      return this.track(() => result);
    });
  }

  private track(run: () => Promise<unknown>): Promise<void> {
    const task: Promise<void> = run().then(
      () => undefined,
      () => undefined,
    );
    this.inFlight.add(task);
    void task.finally(() => {
      this.inFlight.delete(task);
      this.releaseSlotWaiters();
    });
    return task;
  }

  /** Runs the handler for one message. Resolves `true` when it succeeded. */
  private async processMessage(message: Message): Promise<boolean> {
    this.safeEmit('message_received', message);
    const heartbeat = this.startHeartbeat([message]);
    try {
      await this.runWithTimeout(message, (signal) => this.config.handleMessage!(message, signal));
    } catch (error) {
      const err = toError(error);
      if (err instanceof HandlerTimeoutError) this.safeEmit('timeout_error', err, message);
      else this.safeEmit('processing_error', err, message);
      if (this.config.alwaysAcknowledge) {
        await this.acknowledge([message]);
      } else {
        await this.terminateVisibility([message]);
      }
      return false;
    } finally {
      clearInterval(heartbeat);
    }
    await this.acknowledge([message]);
    return true;
  }

  private async processBatch(messages: Message[]): Promise<void> {
    for (const message of messages) this.safeEmit('message_received', message);
    const heartbeat = this.startHeartbeat(messages);
    let result: unknown;
    try {
      result = await this.config.handleMessageBatch!(messages, this.handlerController.signal);
    } catch (error) {
      this.safeEmit('processing_error', toError(error), messages);
      if (this.config.alwaysAcknowledge) await this.acknowledge(messages);
      else await this.terminateVisibility(messages);
      return;
    } finally {
      clearInterval(heartbeat);
    }
    if (Array.isArray(result) && !this.config.alwaysAcknowledge) {
      const ids = new Set(result.map((message: Message) => message.MessageId));
      await this.acknowledge(messages.filter((message) => ids.has(message.MessageId)));
    } else {
      await this.acknowledge(messages);
    }
  }

  private async runWithTimeout(
    message: Message,
    run: (signal: AbortSignal) => Promise<unknown>,
  ): Promise<void> {
    const timeoutMs = this.config.handleMessageTimeout;
    if (!timeoutMs) {
      await run(this.handlerController.signal);
      return;
    }
    const controller = new AbortController();
    const onStop = () => controller.abort();
    this.handlerController.signal.addEventListener('abort', onStop, { once: true });
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        run(controller.signal),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            controller.abort();
            reject(
              new HandlerTimeoutError(
                `Handler for message ${message.MessageId} timed out after ${timeoutMs}ms`,
              ),
            );
          }, timeoutMs);
        }),
      ]);
    } finally {
      clearTimeout(timer);
      this.handlerController.signal.removeEventListener('abort', onStop);
    }
  }

  private startHeartbeat(messages: Message[]): NodeJS.Timeout | undefined {
    const { heartbeatInterval, visibilityTimeout } = this.config;
    if (!heartbeatInterval || visibilityTimeout === undefined) return undefined;
    return setInterval(() => {
      void this.changeVisibility(messages, visibilityTimeout);
    }, heartbeatInterval * 1000);
  }

  private async terminateVisibility(messages: Message[]): Promise<void> {
    const option = this.config.terminateVisibilityTimeout;
    if (option === undefined || option === false) return;
    const timeout = typeof option === 'function' ? option(messages) : option === true ? 0 : option;
    await this.changeVisibility(messages, timeout);
  }

  private async acknowledge(messages: Message[]): Promise<void> {
    if (messages.length === 0) return;
    if (this.config.shouldDeleteMessages === false) {
      for (const message of messages) this.safeEmit('message_processed', message);
      return;
    }
    for (const chunk of chunks(messages, SQS_MAX_BATCH)) {
      try {
        const output = await this.sqs.send(
          new DeleteMessageBatchCommand({
            QueueUrl: this.queueUrl,
            Entries: chunk.map((message, index) => ({
              Id: String(index),
              ReceiptHandle: message.ReceiptHandle,
            })),
          }),
        );
        const failed = new Set((output.Failed ?? []).map((entry) => entry.Id));
        chunk.forEach((message, index) => {
          if (failed.has(String(index))) {
            this.safeEmit(
              'error',
              new Error(`Failed to delete message ${message.MessageId}`),
              message,
            );
          } else {
            this.safeEmit('message_processed', message);
          }
        });
      } catch (error) {
        this.safeEmit('error', toError(error), chunk);
      }
    }
  }

  private async changeVisibility(messages: Message[], timeout: number): Promise<void> {
    for (const chunk of chunks(messages, SQS_MAX_BATCH)) {
      try {
        await this.sqs.send(
          new ChangeMessageVisibilityBatchCommand({
            QueueUrl: this.queueUrl,
            Entries: chunk.map((message, index) => ({
              Id: String(index),
              ReceiptHandle: message.ReceiptHandle,
              VisibilityTimeout: timeout,
            })),
          }),
        );
      } catch (error) {
        this.safeEmit('error', toError(error), chunk);
      }
    }
  }

  private waitForSlot(): Promise<void> {
    if (this.inFlight.size < this.concurrency || !this.running) return Promise.resolve();
    return new Promise((resolve) => this.slotWaiters.push(resolve));
  }

  private releaseSlotWaiters(): void {
    const waiters = this.slotWaiters;
    this.slotWaiters = [];
    for (const resolve of waiters) resolve();
  }

  private sleep(ms: number): Promise<void> {
    if (ms <= 0 || !this.running) return Promise.resolve();
    const signal = this.receiveController.signal;
    return new Promise((resolve) => {
      const timer = setTimeout(done, ms);
      function done() {
        clearTimeout(timer);
        signal.removeEventListener('abort', done);
        resolve();
      }
      signal.addEventListener('abort', done, { once: true });
    });
  }

  // A Node EventEmitter throws on `error` without a listener, and a throwing listener would break
  // the loop. Neither may stop consumption.
  private safeEmit<K extends keyof SqsConsumerEvents>(
    event: K,
    ...args: SqsConsumerEvents[K]
  ): void {
    if (event === 'error' && this.listenerCount('error') === 0) return;
    try {
      (this.emit as (event: K, ...args: SqsConsumerEvents[K]) => boolean)(event, ...args);
    } catch {
      // Listener errors are the listener's problem.
    }
  }
}

function validate(config: SqsConsumerConfig): void {
  if (!config.queueUrl) throw new Error('SQS consumer option `queueUrl` is required');
  if (!config.handleMessage === !config.handleMessageBatch) {
    throw new Error('SQS consumer needs exactly one of `handleMessage` and `handleMessageBatch`');
  }
  const { batchSize, concurrency, waitTimeSeconds, heartbeatInterval, visibilityTimeout } = config;
  if (
    batchSize !== undefined &&
    !(Number.isInteger(batchSize) && batchSize >= 1 && batchSize <= 10)
  ) {
    throw new Error('SQS consumer option `batchSize` must be an integer from 1 to 10');
  }
  if (concurrency !== undefined && !(Number.isInteger(concurrency) && concurrency >= 1)) {
    throw new Error('SQS consumer option `concurrency` must be a positive integer');
  }
  if (waitTimeSeconds !== undefined && !(waitTimeSeconds >= 0 && waitTimeSeconds <= 20)) {
    throw new Error('SQS consumer option `waitTimeSeconds` must be from 0 to 20');
  }
  if (heartbeatInterval !== undefined) {
    if (visibilityTimeout === undefined) {
      throw new Error('SQS consumer option `heartbeatInterval` requires `visibilityTimeout`');
    }
    if (heartbeatInterval >= visibilityTimeout) {
      throw new Error(
        'SQS consumer option `heartbeatInterval` must be less than `visibilityTimeout`',
      );
    }
  }
}

function isConnectionError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const status = (error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode;
  return status === 403 || CONNECTION_ERRORS.has(error.name);
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function chunks<T>(items: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let i = 0; i < items.length; i += size) result.push(items.slice(i, i + size));
  return result;
}
