# Design

nest-sqs is a NestJS 11/12 replacement for `@ssut/nestjs-sqs`. This document records what it keeps,
what it changes, and why. Everything here is implemented in 0.1 unless marked planned.

## Goals

- **Drop-in for existing users.** Same decorators, module options, metadata keys and `SqsService`
  methods. Moving over should be an import change.
- **Safe defaults.** Failure modes that are easy to miss with the old stack (silent stalls, tight
  retry loops, lost messages on shutdown) are handled without extra options.
- **Tested failure behavior.** Redelivery, shutdown and receive errors are tested against a real
  SQS-compatible server, not only mocks.

## Non-goals

- **Exactly-once processing.** SQS standard queues deliver at least once, and a delete can fail after
  the handler succeeds. Handlers still need idempotency keys or a database constraint.
- **Dead-letter queues in the library.** That is the queue's `RedrivePolicy`.
- **A native (Rust or Go) core.** Almost all time is spent waiting on SQS and in the user's handler,
  which runs in JavaScript either way. A native poller would add a call across the FFI boundary per
  message, platform-specific binaries, a second AWS SDK with its own credential chain, and a break in
  `AsyncLocalStorage` context, for gains limited to the polling loop.
- **Lambda event source mappings.** Lambda already polls for you; use Powertools batch processing.

## Compatibility contract

| Surface                                                                       | Status                                                                     |
| ----------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| `@SqsMessageHandler(name, batch?)`                                            | Same signature and metadata                                                |
| `@SqsConsumerEventHandler(name, eventName)`                                   | Same signature; `eventName` is a union type, so a typo fails to compile    |
| `SQS_OPTIONS`, `SQS_CONSUMER_METHOD`, `SQS_CONSUMER_EVENT_HANDLER`            | Same `Symbol.for` keys                                                     |
| `SqsModule.register` / `registerAsync`                                        | Same options, plus `extraProviders` on `registerAsync`                     |
| Consumer and producer options                                                 | Same field names as `sqs-consumer` / `sqs-producer`                        |
| `SqsService.send`, `purgeQueue`, `getQueueAttributes`, `getProducerQueueSize` | Same signatures                                                            |
| `SqsService.consumers` / `producers` maps                                     | Values change: the old ones held `sqs-consumer` / `sqs-producer` instances |
| Acknowledgement by return value                                               | **Changed**, see below                                                     |

## Behavior

### Own polling loop instead of `sqs-consumer`

`sqs-consumer` receives a batch, awaits every handler in it (`Promise.all` in
`handleSqsResponse`), and only then schedules the next poll. So:

- handlers in flight can never exceed `batchSize`, which SQS caps at 10, and
- one slow message holds back the next `ReceiveMessage` for the whole consumer.

nest-sqs runs its own loop with a semaphore of `concurrency` slots. It asks for
`min(batchSize, free slots)` messages and polls again as soon as a slot frees. That is also the
backpressure: the loop never holds more messages than it can run, so a fast poller cannot swamp the
event loop. Spring Cloud AWS does the same with `SemaphoreBackPressureHandler` and
`maxConcurrentMessages`.

`concurrency` defaults to `batchSize`, which matches the old throughput ceiling.

### Acknowledgement

A message is deleted when its handler resolves and kept when it throws, whatever the handler
returns. This matches Spring Cloud AWS (`ON_SUCCESS`) and Shoryuken (`auto_delete`).

`sqs-consumer` 11 deleted when a handler returned `undefined`; 14 and later do not, so a handler that
returns nothing is redelivered forever after an upgrade (ssut/nestjs-sqs#103, likely #99). Tying the
delete to success removes that trap.

Batch handlers: returning an array deletes only those messages (partial success); returning nothing
deletes all of them.

`terminateVisibilityTimeout` sets the visibility of a failed message so its retry comes before the
queue's timeout. `alwaysAcknowledge` deletes even on failure.

### FIFO queues

With `concurrency` above 1, messages of one `MessageGroupId` still run one at a time and in order;
different groups run in parallel. Spring Cloud AWS does this in `FifoSqsComponentFactory` with
`MessageGroupingSinkAdapter` and `OrderedMessageSink`.

### Handler lookup

Handlers are found with Nest's `DiscoveryService` and `MetadataScanner`. `@golevelup/nestjs-discovery`
is not used; its peer range also stops at NestJS 11.

Each call looks the method up on the instance at call time instead of binding it once at startup.
A method wrapped after boot (tracing, metrics, `@Transactional`-style decorators) is then the one that
runs (ssut/nestjs-sqs#108).

### Startup

Consumers start in `onApplicationBootstrap`, after every module's `onModuleInit`, so handlers do not
run before database connections are ready. Mozilla's `fxa-event-broker` starts its `sqs-consumer` the
same way.

Error listeners are attached before the first poll. A Node `EventEmitter` throws when `error` has no
listener, and inside `sqs-consumer`'s poll that skips scheduling the next poll: the process stays up
and passes health checks, but stops consuming. `error` and `processing_error` are logged by default
(ssut/nestjs-sqs#89).

### Receive errors

After a failed `ReceiveMessage`, the next poll waits 1 s, doubling up to 10 s, and resets on the next
success. These are Spring Cloud AWS's poll back-off defaults (`AbstractContainerOptions`). Without a
delay, a refused connection retries immediately, and once the AWS SDK's retry quota is spent it spins
at hundreds of requests per second.

### Long handlers

- `heartbeatInterval` extends visibility while a handler runs, so a slow message is not delivered to
  a second worker. Shoryuken's `auto_extend_visibility` does the same.
- `handleMessageTimeout` gives up on a handler that never settles and leaves its message for
  redelivery, so a lost promise cannot hold a slot forever (ssut/nestjs-sqs#42). The handler receives
  an `AbortSignal` so it can stop work it no longer owns.

### Shutdown

In `onModuleDestroy` (on `SIGTERM` this needs `app.enableShutdownHooks()`). It is the first shutdown
hook, so it runs before `beforeApplicationShutdown` and `onApplicationShutdown`, where database
clients usually close:

1. stop polling,
2. wait up to `drainTimeoutMs` (default 15 s) for in-flight handlers,
3. abort the outstanding long poll, so a late response is not processed while the app tears down
   its connections,
4. log any messages still in flight; they will be redelivered.

`sqs-consumer`'s `stop()` stops polling but does not wait for running handlers, so a plain
`app.close()` closes the database under them (ssut/nestjs-sqs#66).

### Producer

`SqsService.send` keeps its signature. It chunks into `SendMessageBatch` calls of `batchSize` and,
like `sqs-producer`'s `FailedMessagesError`, throws with the ids of entries SQS rejected.

## Package

- ESM and CommonJS builds. NestJS 11 apps are mostly CommonJS and their Jest setups usually cannot
  `require()` ESM.
- Node `^22.12.0 || >=24`. NestJS 12 is ESM only; Node 22.12 is the first 22.x that loads ESM from
  `require()` without a flag.
- Peer dependencies: `@nestjs/common` and `@nestjs/core` `^11 || ^12`, `@aws-sdk/client-sqs` `^3`.
  No dependency on `sqs-consumer` or `sqs-producer`.

## Old issues and where they land

| Issue     | Topic                                        | Here                                                                                       |
| --------- | -------------------------------------------- | ------------------------------------------------------------------------------------------ |
| #112, #98 | NestJS 12 / 11.1 support                     | Peer `^11 \|\| ^12`, built-in discovery                                                    |
| #108      | `.bind()` at startup bypasses later wrapping | Look up the method per call                                                                |
| #103, #99 | Successful messages redelivered              | Delete on success, not on return value                                                     |
| #89       | No catch-all for errors                      | Listeners before start, logged by default                                                  |
| #86       | Limit concurrent processing                  | `concurrency`                                                                              |
| #80       | Providers missing in `registerAsync`         | `extraProviders`                                                                           |
| #66       | Stopping consumers cleanly                   | Drain on shutdown                                                                          |
| #63, #45  | Delete a message manually                    | Planned: `SqsService.deleteMessage`                                                        |
| #42       | Handler never settles                        | `handleMessageTimeout` with `AbortSignal`                                                  |
| #75       | Correlation id in logs                       | Planned: run each handler in its own async context                                         |
| #97       | FIFO warning                                 | Ordered per message group                                                                  |
| #101      | AWS SDK version mismatch                     | `sqs-consumer` ships its own `@aws-sdk/client-sqs` as a dependency; here it is only a peer |

## Verification

| Layer         | What                                                                                                    | Where                                  |
| ------------- | ------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| Unit          | Polling loop, acknowledgement, concurrency, back-off, shutdown, FIFO, timeouts, producer, module wiring | `test/*.test.ts`, in-memory SQS client |
| Integration   | Redelivery, no loss when stopping under load, long poll abort, heartbeat, FIFO order                    | `test/integration`, ElasticMQ          |
| Compatibility | The e2e suite of `@ssut/nestjs-sqs`, unchanged except imports, on NestJS 11 and 12                      | `test/compat`, ElasticMQ               |

Each behavior test was checked by putting back the defect it guards against and seeing it fail.

Not yet verified: real AWS SQS (ElasticMQ can differ on error codes and limits), throughput against
`@ssut/nestjs-sqs`, and production use.

## Roadmap

| Version | Scope                                                                                                               |
| ------- | ------------------------------------------------------------------------------------------------------------------- |
| 0.1     | Module, discovery, polling loop, acknowledgement, FIFO groups, heartbeat, handler timeout, shutdown drain, producer |
| 0.2     | `SqsService.deleteMessage` (#63, #45), per-handler async context (#75), OpenTelemetry spans and metrics             |
| 0.3     | Smoke test on real AWS SQS, benchmark against `@ssut/nestjs-sqs`                                                    |
| 1.0     | Migration guide, production use                                                                                     |
