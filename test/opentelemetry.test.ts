import type { Message as SqsMessage } from '@aws-sdk/client-sqs';
import { SpanKind, SpanStatusCode, context, propagation, trace } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { MeterProvider, MetricReader } from '@opentelemetry/sdk-metrics';
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'vitest';
import type { SqsHandlerContext } from '../src/index.ts';
import { createOpenTelemetryMiddleware, injectTraceContext } from '../src/opentelemetry.ts';

class TestMetricReader extends MetricReader {
  protected async onForceFlush(): Promise<void> {}
  protected async onShutdown(): Promise<void> {}
}

const QUEUE = 'https://sqs.us-east-1.amazonaws.com/000000000000/orders';
const exporter = new InMemorySpanExporter();
const tracerProvider = new BasicTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(exporter)],
});
const tracer = tracerProvider.getTracer('test');
let reader: TestMetricReader;
let meterProvider: MeterProvider;
const contextManager = new AsyncLocalStorageContextManager();

beforeAll(() => {
  context.setGlobalContextManager(contextManager.enable());
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());
});

afterAll(() => {
  context.disable();
  propagation.disable();
});

beforeEach(() => {
  exporter.reset();
  reader = new TestMetricReader();
  meterProvider = new MeterProvider({ readers: [reader] });
});

function middleware() {
  return createOpenTelemetryMiddleware({ tracer, meter: meterProvider.getMeter('test') });
}

function handlerContext(messages: SqsMessage[]): SqsHandlerContext {
  return { name: 'orders', queueUrl: QUEUE, messages, signal: new AbortController().signal };
}

/** A message as a consumer receives it after a producer sent it inside `span`. */
function sentInside(span: ReturnType<typeof tracer.startSpan>, id: string): SqsMessage {
  const message = context.with(trace.setSpan(context.active(), span), () =>
    injectTraceContext({ id, body: 'x' }),
  );
  return { MessageId: id, Body: 'x', MessageAttributes: message.messageAttributes };
}

async function collectMetrics() {
  const { resourceMetrics } = await reader.collect();
  return Object.fromEntries(
    resourceMetrics.scopeMetrics.flatMap((scope) =>
      scope.metrics.map((metric) => [metric.descriptor.name, metric.dataPoints]),
    ),
  );
}

describe('createOpenTelemetryMiddleware', () => {
  test('continues the producer trace for a single message', async () => {
    const producer = tracer.startSpan('send orders');
    const message = sentInside(producer, 'm-1');
    producer.end();

    await middleware()(handlerContext([message]), async () => {});

    const span = exporter.getFinishedSpans().find((item) => item.name === 'process orders')!;
    expect(span.kind).toBe(SpanKind.CONSUMER);
    expect(span.spanContext().traceId).toBe(producer.spanContext().traceId);
    expect(span.parentSpanContext?.spanId).toBe(producer.spanContext().spanId);
    expect(span.attributes).toMatchObject({
      'messaging.system': 'aws_sqs',
      'messaging.operation.type': 'process',
      'messaging.operation.name': 'process',
      'messaging.destination.name': 'orders',
      'messaging.message.id': 'm-1',
    });
  });

  test('links every message of a batch', async () => {
    const first = tracer.startSpan('send a');
    const second = tracer.startSpan('send b');
    const messages = [sentInside(first, 'a'), sentInside(second, 'b')];

    await middleware()(handlerContext(messages), async () => {});

    const span = exporter.getFinishedSpans().find((item) => item.name === 'process orders')!;
    expect(span.attributes['messaging.batch.message_count']).toBe(2);
    expect(span.links.map((link) => link.context.spanId)).toEqual([
      first.spanContext().spanId,
      second.spanContext().spanId,
    ]);
  });

  test('makes the span active for the handler', async () => {
    let active: string | undefined;
    await middleware()(handlerContext([{ MessageId: 'm' }]), async () => {
      active = trace.getActiveSpan()?.spanContext().spanId;
    });

    expect(active).toBe(exporter.getFinishedSpans()[0]!.spanContext().spanId);
  });

  test('records a failure on the span and in the metrics, then rethrows', async () => {
    class PaymentDeclined extends Error {
      override name = 'PaymentDeclined';
    }

    await expect(
      middleware()(handlerContext([{ MessageId: 'm' }]), async () => {
        throw new PaymentDeclined('declined');
      }),
    ).rejects.toThrow('declined');

    const span = exporter.getFinishedSpans()[0]!;
    expect(span.status.code).toBe(SpanStatusCode.ERROR);
    expect(span.attributes['error.type']).toBe('PaymentDeclined');
    const recorded = await collectMetrics();
    expect(recorded['messaging.client.consumed.messages']![0]!.attributes['error.type']).toBe(
      'PaymentDeclined',
    );
  });

  test('records duration and consumed messages', async () => {
    await middleware()(handlerContext([{ MessageId: 'a' }, { MessageId: 'b' }]), async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
    });

    const recorded = await collectMetrics();
    const consumed = recorded['messaging.client.consumed.messages']![0]!;
    expect(consumed.value).toBe(2);
    expect(consumed.attributes).toMatchObject({
      'messaging.system': 'aws_sqs',
      'messaging.destination.name': 'orders',
    });
    const duration = recorded['messaging.process.duration']![0]!.value as {
      count: number;
      sum: number;
    };
    expect(duration.count).toBe(1);
    expect(duration.sum).toBeGreaterThanOrEqual(0.015);
  });
});

describe('injectTraceContext', () => {
  test('adds traceparent as a String message attribute', () => {
    const span = tracer.startSpan('send');
    const message = context.with(trace.setSpan(context.active(), span), () =>
      injectTraceContext({
        id: '1',
        body: 'x',
        messageAttributes: { kind: { DataType: 'String', StringValue: 'order' } },
      }),
    );

    expect(message.messageAttributes?.kind?.StringValue).toBe('order');
    expect(message.messageAttributes?.traceparent).toEqual({
      DataType: 'String',
      StringValue: expect.stringContaining(span.spanContext().traceId),
    });
  });

  test('leaves the message alone when it would exceed 10 attributes', () => {
    const span = tracer.startSpan('send');
    const full = Object.fromEntries(
      Array.from({ length: 10 }, (_, i) => [`a${i}`, { DataType: 'String', StringValue: 'v' }]),
    );
    const message = { id: '1', body: 'x', messageAttributes: full };

    const result = context.with(trace.setSpan(context.active(), span), () =>
      injectTraceContext(message),
    );
    expect(result).toBe(message);
  });

  test('leaves the message alone without an active trace', () => {
    const message = { id: '1', body: 'x' };
    expect(injectTraceContext(message)).toBe(message);
  });
});

describe('semantic convention names', () => {
  // The middleware writes the names out to avoid a runtime dependency; keep them in step with the
  // official package.
  test('match @opentelemetry/semantic-conventions', async () => {
    const semconv = await import('@opentelemetry/semantic-conventions/incubating');
    await middleware()(handlerContext([{ MessageId: 'm' }]), async () => {});
    const span = exporter.getFinishedSpans()[0]!;

    expect(Object.keys(span.attributes).toSorted()).toEqual(
      [
        semconv.ATTR_MESSAGING_SYSTEM,
        semconv.ATTR_MESSAGING_OPERATION_TYPE,
        semconv.ATTR_MESSAGING_OPERATION_NAME,
        semconv.ATTR_MESSAGING_DESTINATION_NAME,
        semconv.ATTR_MESSAGING_MESSAGE_ID,
      ].toSorted(),
    );
    expect(span.attributes[semconv.ATTR_MESSAGING_SYSTEM]).toBe(
      semconv.MESSAGING_SYSTEM_VALUE_AWS_SQS,
    );
    expect(span.attributes[semconv.ATTR_MESSAGING_OPERATION_TYPE]).toBe(
      semconv.MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
    );
    expect(Object.keys(await collectMetrics()).toSorted()).toEqual(
      [
        semconv.METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES,
        semconv.METRIC_MESSAGING_PROCESS_DURATION,
      ].toSorted(),
    );
  });
});
