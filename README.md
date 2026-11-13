# nest-sqs

Amazon SQS consumers and producers for NestJS 11 and 12, as a drop-in replacement for
[`@ssut/nestjs-sqs`](https://github.com/ssut/nestjs-sqs).

> **Status: 0.x.** Tested against ElasticMQ, including the e2e suite of `@ssut/nestjs-sqs` on NestJS 11
> and 12, but not yet against real AWS SQS or in production. See [docs/design.md](docs/design.md).

## Why

`@ssut/nestjs-sqs` allows NestJS up to 11 and has had no release since February 2025. nest-sqs keeps
its API and fixes the failure modes that are easy to hit in production:

- **Installs on NestJS 12.** `@ssut/nestjs-sqs` declares `@nestjs/common` up to `^11`, so `npm install`
  fails with `ERESOLVE` on NestJS 12. nest-sqs uses Nest's built-in discovery instead of
  `@golevelup/nestjs-discovery`, whose range also stops at 11.
- **Deletes on success.** A message is deleted when the handler resolves, whatever it returns, so an
  upgrade of `sqs-consumer` cannot turn every message into an endless redelivery.
- **Concurrency independent of batch size**, so one slow message does not block the next receive.
  With 1 in 50 messages taking 1 s, 1,000 messages took 3.4 s instead of 21.2 s at the same
  concurrency ([benchmark](bench/README.md), on ElasticMQ).
- **No silent stalls.** Error listeners are attached before the first poll, and receive errors back
  off instead of retrying in a tight loop.
- **Graceful shutdown** that waits for in-flight handlers before the app closes its connections.
- **Ordered processing per FIFO message group** when concurrency is above 1.
- **Delete retries** for transient errors, so a DNS hiccup does not mean a duplicate run.
- **Middleware, a per-handler context and OpenTelemetry** for tracing and correlation ids.

## Install

```sh
pnpm add nest-sqs @aws-sdk/client-sqs
```

Requires Node `^22.12.0 || >=24`.

## Usage

```ts
import { Injectable, Module } from '@nestjs/common';
import type { Message } from '@aws-sdk/client-sqs';
import { SqsConsumerEventHandler, SqsMessageHandler, SqsModule } from 'nest-sqs';

@Injectable()
export class OrderConsumer {
  @SqsMessageHandler('orders')
  async handle(message: Message, signal: AbortSignal) {
    // Resolve to delete the message. Throw to have it redelivered.
    // `signal` aborts when handleMessageTimeout or the shutdown drain runs out.
  }

  @SqsConsumerEventHandler('orders', 'processing_error')
  onError(error: Error, message: Message) {}
}

@Module({
  imports: [
    SqsModule.register({
      consumers: [{ name: 'orders', queueUrl: process.env.ORDERS_QUEUE_URL!, concurrency: 8 }],
      producers: [{ name: 'orders', queueUrl: process.env.ORDERS_QUEUE_URL! }],
    }),
  ],
  providers: [OrderConsumer],
})
export class AppModule {}
```

Call `app.enableShutdownHooks()` so consumers drain on `SIGTERM`.

## Acknowledging early or retrying later

```ts
@SqsMessageHandler('orders')
async handle(message: Message) {
  await this.sqsService.deleteMessage('orders', message); // done; no redelivery even if this throws later
  // or: await this.sqsService.changeMessageVisibility('orders', message, 300); then throw to retry in 5 min
}
```

## Middleware and context

Middleware runs around every handler call; `getSqsContext()` returns the current call's consumer,
messages and signal anywhere below it, across awaits.

```ts
import { getSqsContext } from 'nest-sqs';

SqsModule.register({
  middleware: [
    async (context, next) => {
      const started = Date.now();
      try {
        return await next();
      } finally {
        console.log(`${context.name} took ${Date.now() - started} ms`);
      }
    },
  ],
  consumers: [/* ... */],
});

// In a logger, for example a pino mixin:
const mixin = () => ({ sqsMessageId: getSqsContext()?.messages[0]?.MessageId });
```

## OpenTelemetry

```ts
import { createOpenTelemetryMiddleware, injectTraceContext } from 'nest-sqs/opentelemetry';

SqsModule.register({
  middleware: [createOpenTelemetryMiddleware()],
  // `traceparent` arrives as a message attribute, so request it.
  consumers: [{ name: 'orders', queueUrl, messageAttributeNames: ['All'] }],
  producers: [{ name: 'orders', queueUrl }],
});

await sqsService.send('orders', injectTraceContext({ id: '1', body: order }));
```

Each handler call gets a CONSUMER span with the messaging semantic convention attributes, a child of
the producer's span, and the `messaging.process.duration` and `messaging.client.consumed.messages`
metrics. Needs `@opentelemetry/api` and an SDK set up by the app.

## Migrating from `@ssut/nestjs-sqs`

Change the import from `@ssut/nestjs-sqs` to `nest-sqs`. Module options, decorators and metadata keys
are the same. Differences:

- Handlers no longer need to return the message to have it deleted.
- `SqsService.consumers` and `producers` no longer hold `sqs-consumer` / `sqs-producer` instances.
- A failing batch handler emits `processing_error` with the messages, not `error`.
- On FIFO queues, messages of one group run in order; `sqs-consumer` ran a batch in parallel.
- Two `@SqsMessageHandler`s for one consumer fail the boot instead of one being picked.
- `handleMessageTimeout` aborts the handler's signal but keeps its slot until it settles, instead
  of treating the message as failed while the handler still runs.
- With `concurrency` and no `batchSize`, a receive asks for up to `concurrency` messages (at most 10) instead of 1.
- New options: `concurrency`, `pollingErrorBackoff`, `drainTimeoutMs`, `abortGraceMs`,
  `middleware`, and `extraProviders` on `registerAsync`.

## License

[MIT](LICENSE)
