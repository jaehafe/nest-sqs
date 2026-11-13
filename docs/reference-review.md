# Reference review

nest-sqs runs its own polling loop instead of `sqs-consumer`. To find where that loop falls short,
it was read side by side with SQS consumers written by teams with long experience in message
processing. Each finding below cites code on both sides; nest-sqs line numbers refer to the code
before the fixes listed.

## References

| Reference                                                                                                                   | Who                        | Commit read          |
| --------------------------------------------------------------------------------------------------------------------------- | -------------------------- | -------------------- |
| [Broadway](https://github.com/elixir-broadway/broadway) and [broadway_sqs](https://github.com/elixir-broadway/broadway_sqs) | Dashbit (Elixir core team) | `0d09c6e`, `4780c85` |
| [Go CDK pubsub](https://github.com/google/go-cloud/tree/master/pubsub) (`awssnssqs`, `batcher`)                             | Google                     | `bbb3555`            |
| [AWS Message Processing Framework for .NET](https://github.com/aws/aws-dotnet-messaging)                                    | AWS                        | `5b4e9e7`            |
| [AWS SDK for Java v2 SQS batch manager](https://github.com/aws/aws-sdk-java-v2/tree/master/services/sqs)                    | AWS                        | `66022b5`            |
| [Spring Cloud AWS SQS](https://github.com/awspring/spring-cloud-aws)                                                        | awspring                   | `96912b6`            |
| [kombu SQS transport](https://github.com/celery/kombu/blob/main/kombu/transport/SQS/__init__.py)                            | Celery                     | `38fbf78`            |
| [sqs-consumer](https://github.com/bbc/sqs-consumer)                                                                         | BBC                        | `9f72384`            |

## Bugs in nest-sqs (fixed)

None of these depended on a reference to be wrong; the references made them visible.

| Bug                                                                                                                        | Effect                                                                                                                                                 | Fix                                                                                                                                                      |
| -------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A timed-out handler released its slot while still running (`consumer.ts:442`)                                              | More handlers than `concurrency` ran at once; with `terminateVisibilityTimeout` the message was redelivered and ran twice                              | Slot and heartbeat are held until the handler settles; a late success deletes                                                                            |
| The heartbeat started at processing, not receipt (`consumer.ts:358`, `:463`)                                               | A FIFO message waiting behind its group could time out, go to another consumer and run out of order; the 12-hour limit was counted from the wrong time | Heartbeats start at receipt and stop before the delete or hand-back. AWS .NET registers messages at receipt (`DefaultMessageManager.cs`)                 |
| `batchSize` defaulted to 1 even with `concurrency` set (`consumer.ts:144`, `:242`)                                         | `concurrency: 50` still received one message per call                                                                                                  | Defaults to `concurrency` capped at 10, as AWS .NET's `SQSMessagePoller` (min(free slots, 10))                                                           |
| Messages a batch handler did not return were dropped silently (`consumer.ts:399`)                                          | No event, no visibility handling; on FIFO a later message of a failed group could be deleted                                                           | Reported and handled as failures; FIFO hands back later messages of a failed group. Broadway routes them to `handle_failed` (`batch_processor_stage.ex`) |
| `stop()` returned right after aborting handlers (`consumer.ts:209-210`)                                                    | Handlers and their deletes ran after Nest closed the database                                                                                          | Waits `abortGraceMs` (default 5 s) after the abort. Go CDK `Subscription.Shutdown` waits within the caller's deadline (`pubsub.go`)                      |
| `ChangeMessageVisibilityBatch` `Failed` entries were not read (`consumer.ts:550`)                                          | A failed heartbeat or hand-back went unnoticed                                                                                                         | Reported as `error`; a message deleted meanwhile is skipped with the same check AWS .NET uses (`SQSMessagePoller.cs`)                                    |
| A handler that deleted its message and then threw leaked its receipt handle; batch handlers ignored `handleMessageTimeout` | Slow memory growth, a visibility call on a deleted message; no timeout for batches                                                                     | Cleared on every exit; batches use the same timeout path                                                                                                 |

## Gaps that remain

| Gap                                                                                                       | Who does it                                                                                                                              | Impact                                                                       |
| --------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Deletes are not batched across messages: one `DeleteMessageBatch` per message for single-message handlers | Spring (`BatchingAcknowledgementProcessor`, 10 or 1 s), Java SDK (`RequestBatchManager`, 200 ms), Go CDK (`batcher`), Broadway (batcher) | Up to 10x the delete requests, and each slot waits for its delete round trip |
| One heartbeat timer per message, extending on every tick                                                  | AWS .NET: one scanner over all in-flight messages, extending in batches of 10 only near expiry (`DefaultMessageManager.cs`)              | Extra API calls at high concurrency                                          |
| Visibility extension is off unless configured                                                             | AWS .NET turns it on by default and reads the queue's timeout                                                                            | Long handlers run twice by default                                           |
| No retry backoff by receive count                                                                         | Spring `ExponentialBackoffErrorHandler`, kombu                                                                                           | During an outage every failed message retries at the same interval           |
| Fatal errors (`AccessDenied`, missing queue) retry every 10 s forever                                     | AWS .NET stops the poller                                                                                                                | A misconfigured consumer never fails visibly                                 |
| One `ReceiveMessage` in flight at a time                                                                  | Spring, Go CDK (up to 100), Java SDK prefetch                                                                                            | At most 10 messages per round trip                                           |
| No jitter on poll back-off                                                                                | AWS .NET `CappedExponentialBackoffPolicy`                                                                                                | A fleet retries in step                                                      |
| Producer splits by count only, and stops at the first thrown batch                                        | Go CDK, Java SDK (byte limits, per-message results)                                                                                      | One oversized message fails a batch; the caller cannot tell what was sent    |
| No receive or delete spans, no ack failure metric                                                         | Spring observation covers the whole pipeline                                                                                             | Delete failures are invisible in traces                                      |
| No rate limiting                                                                                          | Broadway                                                                                                                                 | Nothing protects a downstream service when a backlog drains                  |

## Where nest-sqs goes further

- **Delete retries** with backoff and error classification. Spring, kombu, Broadway and AWS .NET log
  a failed delete and move on; the message runs again.
- **Recovery from receive errors.** Go CDK's SQS subscription fails permanently on the first one;
  Broadway retries at a fixed interval.
- **Handing messages back at once** (visibility 0) when they arrive during shutdown or are skipped in
  a FIFO group. Spring and AWS .NET leave them hidden until the timeout.
- **FIFO ordering within a group**, which Broadway and Go CDK leave to the user.
- Handler timeout with an `AbortSignal`, middleware with an async context.

## Testing gaps

- **Tests that count API calls.** Go CDK (`acks_test.go`) and broadway_sqs (receive sizes 10, then 5,
  5, 0) pin how many requests a workload makes. That is how the receive-size bug above went unseen.
- **Real SQS.** Go CDK replays recorded AWS traffic for every driver; AWS .NET runs parameterised
  FIFO tests (groups x messages x concurrency, with failures) against real SQS. nest-sqs has
  ElasticMQ and a smoke script that has not been run against AWS yet.
- **Option validation.** broadway_sqs has about 20 cases; nest-sqs has 5.
