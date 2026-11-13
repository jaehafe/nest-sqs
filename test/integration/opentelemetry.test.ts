import { context, propagation, trace } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import { Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { afterAll, beforeAll, expect, test } from 'vitest';
import { SqsMessageHandler, SqsModule, SqsService } from '../../src/index.ts';
import { createOpenTelemetryMiddleware, injectTraceContext } from '../../src/opentelemetry.ts';
import { waitFor } from '../fake-sqs.ts';
import { createClient, createQueue, deleteQueue } from './elasticmq.ts';

const exporter = new InMemorySpanExporter();
const tracer = new BasicTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(exporter)],
}).getTracer('test');

beforeAll(() => {
  context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());
});

afterAll(() => {
  context.disable();
  propagation.disable();
});

test('the consumer span continues the trace the producer sent through SQS', async () => {
  const sqs = createClient();
  const url = await createQueue(sqs);
  let handled = 0;

  @Injectable()
  class Handler {
    @SqsMessageHandler('orders')
    async handle() {
      handled++;
    }
  }

  const moduleRef = await Test.createTestingModule({
    imports: [
      SqsModule.register({
        middleware: [createOpenTelemetryMiddleware({ tracer })],
        consumers: [
          {
            name: 'orders',
            queueUrl: url,
            sqs,
            waitTimeSeconds: 1,
            messageAttributeNames: ['All'],
          },
        ],
        producers: [{ name: 'orders', queueUrl: url, sqs }],
      }),
    ],
    providers: [Handler],
  }).compile();
  const app = await moduleRef.init();

  try {
    const producer = tracer.startSpan('send orders');
    await context.with(trace.setSpan(context.active(), producer), () =>
      app.get(SqsService).send('orders', injectTraceContext({ id: '1', body: { orderId: 1 } })),
    );
    producer.end();

    await waitFor(() => handled === 1, 10_000);
    await waitFor(() =>
      exporter.getFinishedSpans().some((span) => span.name.startsWith('process ')),
    );
    const consumer = exporter.getFinishedSpans().find((span) => span.name.startsWith('process '))!;
    expect(consumer.spanContext().traceId).toBe(producer.spanContext().traceId);
    expect(consumer.parentSpanContext?.spanId).toBe(producer.spanContext().spanId);
  } finally {
    await app.close();
    await deleteQueue(sqs, url);
  }
});
