import type { Message, SQSClient } from '@aws-sdk/client-sqs';

interface StoredMessage {
  message: Message;
  invisibleUntil: number;
  receipt?: string;
}

interface Call {
  command: string;
  input: any;
  at: number;
}

/**
 * In-memory stand-in for the parts of `SQSClient` the consumer uses. Receives return right away
 * (no long poll) after `receiveDelayMs`, and honor the abort signal like the real client.
 */
export class FakeSqs {
  readonly calls: Call[] = [];
  readonly deleted: string[] = [];
  receiveDelayMs = 5;
  /** Throw this from the next N receives. */
  receiveErrors: Error[] = [];
  /** Fail the next DeleteMessageBatch calls: an Error is thrown, an object marks every entry failed. */
  deleteFailures: Array<Error | { Code: string; SenderFault: boolean }> = [];
  /** Mark every entry of the next ChangeMessageVisibilityBatch calls failed with this code. */
  visibilityFailures: Array<{ Code: string; SenderFault: boolean; Message?: string }> = [];
  private readonly stored: StoredMessage[] = [];
  private receiptSeq = 0;

  constructor(private readonly visibilityMs = 60_000) {}

  add(
    body: string,
    attributes: Record<string, string> = {},
    id: string = `m-${this.stored.length + 1}`,
  ): string {
    this.stored.push({
      message: { MessageId: id, Body: body, Attributes: attributes },
      invisibleUntil: 0,
    });
    return id;
  }

  get remaining(): string[] {
    return this.stored.map((entry) => entry.message.Body!);
  }

  callsOf(command: string): Call[] {
    return this.calls.filter((call) => call.command === command);
  }

  asClient(): SQSClient {
    return { send: (command: any, options?: any) => this.send(command, options) } as SQSClient;
  }

  private async send(command: { constructor: { name: string }; input: any }, options?: any) {
    const name = command.constructor.name;
    this.calls.push({ command: name, input: command.input, at: Date.now() });
    switch (name) {
      case 'ReceiveMessageCommand':
        return this.receive(command.input, options?.abortSignal);
      case 'DeleteMessageBatchCommand':
        return this.deleteBatch(command.input);
      case 'DeleteMessageCommand':
        return this.deleteBatch({
          Entries: [{ Id: '0', ReceiptHandle: command.input.ReceiptHandle }],
        });
      case 'ChangeMessageVisibilityCommand':
        return this.changeVisibility({ Entries: [command.input] });
      case 'ChangeMessageVisibilityBatchCommand':
        return this.changeVisibility(command.input);
      default:
        throw new Error(`FakeSqs does not support ${name}`);
    }
  }

  private async receive(input: any, signal?: AbortSignal) {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(resolve, this.receiveDelayMs);
      signal?.addEventListener('abort', () => {
        clearTimeout(timer);
        const error = new Error('Request aborted');
        error.name = 'AbortError';
        reject(error);
      });
    });
    const error = this.receiveErrors.shift();
    if (error) throw error;
    const now = Date.now();
    const visibleMs =
      input.VisibilityTimeout === undefined ? this.visibilityMs : input.VisibilityTimeout * 1000;
    const messages: Message[] = [];
    // Like SQS FIFO: a group is held back while any of its messages is in flight, and a group's
    // messages come out in order.
    const blockedGroups = new Set<string>();
    for (const entry of this.stored) {
      const group = entry.message.Attributes?.MessageGroupId;
      if (group !== undefined && entry.invisibleUntil > now) blockedGroups.add(group);
    }
    for (const entry of this.stored) {
      if (messages.length >= input.MaxNumberOfMessages) break;
      const group = entry.message.Attributes?.MessageGroupId;
      if (group !== undefined && blockedGroups.has(group)) continue;
      if (entry.invisibleUntil > now) continue;
      entry.receipt = `r-${++this.receiptSeq}`;
      entry.invisibleUntil = now + visibleMs;
      messages.push({ ...entry.message, ReceiptHandle: entry.receipt });
    }
    return { Messages: messages };
  }

  private deleteBatch(input: any) {
    const failure = this.deleteFailures.shift();
    if (failure instanceof Error) throw failure;
    if (failure) {
      return {
        Successful: [],
        Failed: input.Entries.map((entry: any) => ({ Id: entry.Id, ...failure })),
      };
    }
    const failed = [];
    for (const entry of input.Entries) {
      const index = this.stored.findIndex((stored) => stored.receipt === entry.ReceiptHandle);
      if (index === -1) {
        failed.push({ Id: entry.Id, Code: 'ReceiptHandleIsInvalid', SenderFault: true });
        continue;
      }
      this.deleted.push(this.stored[index]!.message.Body!);
      this.stored.splice(index, 1);
    }
    return { Successful: [], Failed: failed };
  }

  private changeVisibility(input: any) {
    const failure = this.visibilityFailures.shift();
    if (failure) {
      return {
        Successful: [],
        Failed: input.Entries.map((entry: any) => ({ Id: entry.Id, ...failure })),
      };
    }
    for (const entry of input.Entries) {
      const stored = this.stored.find((item) => item.receipt === entry.ReceiptHandle);
      if (stored) stored.invisibleUntil = Date.now() + entry.VisibilityTimeout * 1000;
    }
    return { Successful: [], Failed: [] };
  }
}

export interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
}

export function deferred<T = void>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

export async function waitFor(check: () => boolean, timeoutMs = 2_000): Promise<void> {
  const start = Date.now();
  while (!check()) {
    if (Date.now() - start > timeoutMs) throw new Error('waitFor timed out');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
