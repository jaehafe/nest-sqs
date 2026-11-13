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
- **No silent stalls.** Error listeners are attached before the first poll, and receive errors back
  off instead of retrying in a tight loop.
- **Graceful shutdown** that waits for in-flight handlers before the app closes its connections.
- **Ordered processing per FIFO message group** when concurrency is above 1.

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

## Migrating from `@ssut/nestjs-sqs`

Change the import from `@ssut/nestjs-sqs` to `nest-sqs`. Module options, decorators and metadata keys
are the same. Differences:

- Handlers no longer need to return the message to have it deleted.
- `SqsService.consumers` and `producers` no longer hold `sqs-consumer` / `sqs-producer` instances.
- A failing batch handler emits `processing_error` with the messages, not `error`.
- On FIFO queues, messages of one group run in order; `sqs-consumer` ran a batch in parallel.
- Two `@SqsMessageHandler`s for one consumer fail the boot instead of one being picked.
- New options: `concurrency`, `pollingErrorBackoff`, `drainTimeoutMs`, and `extraProviders` on
  `registerAsync`.

## License

[MIT](LICENSE)
