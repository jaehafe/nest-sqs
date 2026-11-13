import {
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  context,
  metrics,
  propagation,
  trace,
} from '@opentelemetry/api';
import type {
  Attributes,
  Link,
  Meter,
  TextMapGetter,
  TextMapSetter,
  Tracer,
} from '@opentelemetry/api';
import type { Message as SqsMessage, MessageAttributeValue } from '@aws-sdk/client-sqs';
import type { SqsMiddleware } from './consumer.ts';
import type { Message } from './types.ts';

export interface OpenTelemetryOptions {
  /** Defaults to the global tracer provider's `nest-sqs` tracer. */
  tracer?: Tracer;
  /** Defaults to the global meter provider's `nest-sqs` meter. */
  meter?: Meter;
}

// Names and values from the OpenTelemetry messaging semantic conventions
// (@opentelemetry/semantic-conventions/incubating). Written out so the package does not need it at
// runtime.
const ATTR_MESSAGING_SYSTEM = 'messaging.system';
const ATTR_MESSAGING_OPERATION_TYPE = 'messaging.operation.type';
const ATTR_MESSAGING_OPERATION_NAME = 'messaging.operation.name';
const ATTR_MESSAGING_DESTINATION_NAME = 'messaging.destination.name';
const ATTR_MESSAGING_MESSAGE_ID = 'messaging.message.id';
const ATTR_MESSAGING_BATCH_MESSAGE_COUNT = 'messaging.batch.message_count';
const ATTR_ERROR_TYPE = 'error.type';
const METRIC_MESSAGING_PROCESS_DURATION = 'messaging.process.duration';
const METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES = 'messaging.client.consumed.messages';

// SQS allows at most 10 message attributes per message.
const SQS_MAX_MESSAGE_ATTRIBUTES = 10;

const getter: TextMapGetter<SqsMessage> = {
  keys: (message) => Object.keys(message.MessageAttributes ?? {}),
  get: (message, key) => message.MessageAttributes?.[key]?.StringValue,
};

const setter: TextMapSetter<Record<string, MessageAttributeValue>> = {
  set: (carrier, key, value) => {
    carrier[key] = { DataType: 'String', StringValue: value };
  },
};

/**
 * Middleware that wraps each handler call in a `process` span and records
 * `messaging.process.duration` and `messaging.client.consumed.messages`.
 *
 * The trace context a producer put in the message attributes (`traceparent`, as
 * `injectTraceContext` or the AWS SDK instrumentation writes it) becomes the span's parent for a
 * single message and a link per message for a batch. The consumer only receives those attributes
 * when `messageAttributeNames` includes them, for example `['All']`.
 */
export function createOpenTelemetryMiddleware(options: OpenTelemetryOptions = {}): SqsMiddleware {
  const tracer = options.tracer ?? trace.getTracer('nest-sqs');
  const meter = options.meter ?? metrics.getMeter('nest-sqs');
  const duration = meter.createHistogram(METRIC_MESSAGING_PROCESS_DURATION, {
    unit: 's',
    description: 'Duration of processing operation.',
  });
  const consumed = meter.createCounter(METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES, {
    unit: '{message}',
    description: 'Number of messages that were delivered to the application.',
  });

  return async (handler, next) => {
    const { messages } = handler;
    const queueName = handler.queueUrl.slice(handler.queueUrl.lastIndexOf('/') + 1);
    const attributes: Attributes = {
      [ATTR_MESSAGING_SYSTEM]: 'aws_sqs',
      [ATTR_MESSAGING_OPERATION_TYPE]: 'process',
      [ATTR_MESSAGING_OPERATION_NAME]: 'process',
      [ATTR_MESSAGING_DESTINATION_NAME]: queueName,
    };
    const spanAttributes: Attributes = { ...attributes };
    let parent = context.active();
    const links: Link[] = [];
    if (messages.length === 1) {
      spanAttributes[ATTR_MESSAGING_MESSAGE_ID] = messages[0]!.MessageId;
      parent = propagation.extract(parent, messages[0]!, getter);
    } else {
      spanAttributes[ATTR_MESSAGING_BATCH_MESSAGE_COUNT] = messages.length;
      for (const message of messages) {
        const spanContext = trace.getSpanContext(
          propagation.extract(ROOT_CONTEXT, message, getter),
        );
        if (spanContext) links.push({ context: spanContext });
      }
    }

    const started = performance.now();
    return tracer.startActiveSpan(
      `process ${queueName}`,
      { kind: SpanKind.CONSUMER, attributes: spanAttributes, links },
      parent,
      async (span) => {
        try {
          return await next();
        } catch (error) {
          const errorType = error instanceof Error ? error.name : 'Error';
          attributes[ATTR_ERROR_TYPE] = errorType;
          span.setAttribute(ATTR_ERROR_TYPE, errorType);
          span.recordException(error instanceof Error ? error : String(error));
          span.setStatus({
            code: SpanStatusCode.ERROR,
            message: String((error as Error)?.message ?? error),
          });
          throw error;
        } finally {
          span.end();
          duration.record((performance.now() - started) / 1000, attributes);
          consumed.add(messages.length, attributes);
        }
      },
    );
  };
}

/**
 * Returns the message with the active trace context added to its `messageAttributes`, so the
 * consumer's span joins the producer's trace. Left unchanged when SQS's limit of 10 attributes
 * would be exceeded.
 */
export function injectTraceContext<T>(message: Message<T>): Message<T> {
  const added: Record<string, MessageAttributeValue> = {};
  propagation.inject(context.active(), added, setter);
  const existing = message.messageAttributes ?? {};
  if (Object.keys(added).length === 0) return message;
  if (Object.keys({ ...existing, ...added }).length > SQS_MAX_MESSAGE_ATTRIBUTES) return message;
  return { ...message, messageAttributes: { ...existing, ...added } };
}
