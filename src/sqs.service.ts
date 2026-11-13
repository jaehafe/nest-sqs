import type { EventEmitter } from 'node:events';
import { GetQueueAttributesCommand, PurgeQueueCommand } from '@aws-sdk/client-sqs';
import type {
  Message as SqsMessage,
  PurgeQueueCommandOutput,
  QueueAttributeName,
  SendMessageBatchResultEntry,
} from '@aws-sdk/client-sqs';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type {
  LoggerService,
  OnApplicationBootstrap,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { DiscoveryService, MetadataScanner, Reflector } from '@nestjs/core';
import { SQS_CONSUMER_EVENT_HANDLER, SQS_CONSUMER_METHOD, SQS_OPTIONS } from './constants.ts';
import { SqsConsumer } from './consumer.ts';
import type { SqsConsumerConfig } from './consumer.ts';
import { SqsProducer } from './producer.ts';
import type {
  Message,
  QueueName,
  SqsConsumerEventHandlerMeta,
  SqsMessageHandlerMeta,
  SqsOptions,
  StopOptions,
} from './types.ts';

export interface SqsConsumerMapValues {
  instance: SqsConsumer;
  stopOptions: StopOptions;
}

interface DiscoveredMethod<T> {
  meta: T;
  instance: Record<string, (...args: any[]) => unknown>;
  methodName: string;
  className: string;
}

@Injectable()
export class SqsService implements OnModuleInit, OnApplicationBootstrap, OnModuleDestroy {
  readonly consumers: Map<QueueName, SqsConsumerMapValues> = new Map();
  readonly producers: Map<QueueName, SqsProducer> = new Map();
  private readonly logger: LoggerService;

  constructor(
    @Inject(SQS_OPTIONS) readonly options: SqsOptions,
    @Inject(DiscoveryService) private readonly discovery: DiscoveryService,
    @Inject(MetadataScanner) private readonly scanner: MetadataScanner,
    @Inject(Reflector) private readonly reflector: Reflector,
  ) {
    this.logger = options.logger ?? new Logger('SqsService', { timestamp: false });
  }

  // Consumers are built here so bad options fail the boot, and started in onApplicationBootstrap,
  // after every module's onModuleInit has opened its connections.
  onModuleInit(): void {
    const handlers = this.discover<SqsMessageHandlerMeta>(SQS_CONSUMER_METHOD);
    const eventHandlers = this.discover<SqsConsumerEventHandlerMeta>(SQS_CONSUMER_EVENT_HANDLER);

    for (const { name, stopOptions, ...options } of this.options.consumers ?? []) {
      if (this.consumers.has(name)) throw new Error(`Consumer already exists: ${name}`);
      const matching = handlers.filter((handler) => handler.meta.name === name);
      if (matching.length > 1) {
        const where = matching.map((handler) => `${handler.className}.${handler.methodName}`);
        throw new Error(`More than one @SqsMessageHandler for ${name}: ${where.join(', ')}`);
      }
      const handler = matching[0];
      if (!handler) {
        this.logger.warn(`No @SqsMessageHandler found for consumer: ${name}`);
        continue;
      }

      // Look the method up on every call instead of binding it now, so wrapping applied after boot
      // (tracing, metrics, transactions) runs (ssut/nestjs-sqs#108).
      const call = (...args: unknown[]) => handler.instance[handler.methodName]!(...args);
      const config = {
        ...options,
        name,
        middleware: [...(this.options.middleware ?? []), ...(options.middleware ?? [])],
        ...(handler.meta.batch ? { handleMessageBatch: call } : { handleMessage: call }),
      } as SqsConsumerConfig;
      const consumer = new SqsConsumer(config);

      // Attached before start: an `error` with no listener used to stop polling for good
      // (ssut/nestjs-sqs#89).
      consumer.on('error', (error, message) => {
        this.logger.error(`[${name}] SQS error${describe(message)}: ${error.message}`, error.stack);
      });
      consumer.on('processing_error', (error, message) => {
        this.logger.error(
          `[${name}] Handler failed${describe(message)}: ${error.message}`,
          error.stack,
        );
      });
      consumer.on('timeout_error', (error, message) => {
        this.logger.error(`[${name}] Handler timed out${describe(message)}: ${error.message}`);
      });
      for (const eventHandler of eventHandlers) {
        if (eventHandler.meta.name !== name) continue;
        // The event name is only known at runtime, so the typed overloads of `on` do not apply.
        (consumer as EventEmitter).on(eventHandler.meta.eventName, (...args: unknown[]) =>
          eventHandler.instance[eventHandler.methodName]!(...args),
        );
      }

      this.consumers.set(name, {
        instance: consumer,
        stopOptions: stopOptions ?? this.options.globalStopOptions ?? {},
      });
    }

    for (const { name, ...options } of this.options.producers ?? []) {
      if (this.producers.has(name)) throw new Error(`Producer already exists: ${name}`);
      this.producers.set(name, new SqsProducer(options));
    }
  }

  onApplicationBootstrap(): void {
    for (const [name, { instance }] of this.consumers) {
      instance.start();
      this.logger.log(`[${name}] Consumer started`);
    }
  }

  // onModuleDestroy is the first shutdown hook, before beforeApplicationShutdown and
  // onApplicationShutdown where database clients usually close.
  async onModuleDestroy(): Promise<void> {
    await this.stop();
  }

  /** Stops every consumer and waits for their in-flight handlers (up to `drainTimeoutMs`). */
  async stop(): Promise<void> {
    await Promise.all(
      [...this.consumers].map(async ([name, { instance, stopOptions }]) => {
        if (!instance.isRunning) return;
        await instance.stop(stopOptions);
        const left = instance.inFlightCount;
        if (left > 0) {
          this.logger.warn(
            `[${name}] Stopped with ${left} handler(s) still running; their messages will be redelivered`,
          );
        } else {
          this.logger.log(`[${name}] Consumer stopped`);
        }
      }),
    );
  }

  /** Sends messages. Bodies that are not strings are sent as JSON. */
  send<T = any>(
    name: QueueName,
    payload: Message<T> | Message<T>[],
  ): Promise<SendMessageBatchResultEntry[]> {
    const producer = this.producers.get(name);
    if (!producer) throw new Error(`Producer does not exist: ${name}`);
    const messages = (Array.isArray(payload) ? payload : [payload]).map((message) => ({
      ...message,
      body: typeof message.body === 'string' ? message.body : JSON.stringify(message.body),
    }));
    return producer.send(messages);
  }

  async purgeQueue(name: QueueName): Promise<PurgeQueueCommandOutput> {
    const { sqs, queueUrl } = this.getQueueInfo(name);
    return sqs.send(new PurgeQueueCommand({ QueueUrl: queueUrl }));
  }

  async getQueueAttributes(name: QueueName): Promise<{ [key in QueueAttributeName]: string }> {
    const { sqs, queueUrl } = this.getQueueInfo(name);
    const response = await sqs.send(
      new GetQueueAttributesCommand({ QueueUrl: queueUrl, AttributeNames: ['All'] }),
    );
    return response.Attributes as { [key in QueueAttributeName]: string };
  }

  /**
   * Deletes a message now, from inside its handler. The automatic delete after the handler then
   * skips it.
   */
  deleteMessage(name: QueueName, message: SqsMessage): Promise<void> {
    return this.getConsumer(name).deleteMessage(message);
  }

  /** Sets a received message's visibility timeout in seconds. */
  changeMessageVisibility(
    name: QueueName,
    message: SqsMessage,
    visibilityTimeout: number,
  ): Promise<void> {
    return this.getConsumer(name).changeMessageVisibility(message, visibilityTimeout);
  }

  getProducerQueueSize(name: QueueName): Promise<number> {
    const producer = this.producers.get(name);
    if (!producer) throw new Error(`Producer does not exist: ${name}`);
    return producer.queueSize();
  }

  private getConsumer(name: QueueName): SqsConsumer {
    const consumer = this.consumers.get(name)?.instance;
    if (!consumer) throw new Error(`Consumer does not exist: ${name}`);
    return consumer;
  }

  private getQueueInfo(name: QueueName): { sqs: SqsProducer['sqs']; queueUrl: string } {
    const target = this.consumers.get(name)?.instance ?? this.producers.get(name);
    if (!target) throw new Error(`Consumer/Producer does not exist: ${name}`);
    return { sqs: target.sqs, queueUrl: target.queueUrl };
  }

  private discover<T>(key: symbol): DiscoveredMethod<T>[] {
    const found: DiscoveredMethod<T>[] = [];
    for (const wrapper of this.discovery.getProviders()) {
      const { instance } = wrapper;
      // Request-scoped and transient providers have no single instance to call.
      if (!instance || typeof instance !== 'object' || !wrapper.isDependencyTreeStatic()) continue;
      const prototype = Object.getPrototypeOf(instance);
      for (const methodName of this.scanner.getAllMethodNames(prototype)) {
        const meta = this.reflector.get<T>(key, prototype[methodName]);
        if (meta === undefined) continue;
        found.push({ meta, instance, methodName, className: instance.constructor.name });
      }
    }
    return found;
  }
}

function describe(message: unknown): string {
  if (Array.isArray(message))
    return ` (messages ${message.map((item) => item.MessageId).join(', ')})`;
  const id = (message as { MessageId?: string } | undefined)?.MessageId;
  return id ? ` (message ${id})` : '';
}
