# Benchmark

nest-sqs against `@ssut/nestjs-sqs` 3.0.1 in the same NestJS 11.2.9 app, on a local ElasticMQ.
Each library runs in its own process; every run gets a fresh, pre-filled queue.

```sh
docker run -d -p 9324:9324 softwaremill/elasticmq-native:1.7.1
cd bench
node setup.mjs   # builds and packs nest-sqs, installs it next to @ssut/nestjs-sqs
RUNS=5 node run.mjs  # runs per case (default 3); SCENARIO=fast or LIBRARY=nest-sqs narrows it
```

Scenarios (`scenarios.mjs`), all with `batchSize: 10`:

- **fast**: 3,000 messages, handler returns at once. Measures the library's own polling and
  deleting.
- **mixed**: 1,000 messages, 1 in 50 takes 1 s and the rest 5 ms. Measures whether a slow message
  holds back the others.

`@ssut/nestjs-sqs` runs one batch of 10 at a time, so concurrency 10 is the fair comparison; the
concurrency 50 rows show what the extra option buys.

## Results

Apple M5 Pro, Node 24.22.1, ElasticMQ 1.7.1 in Docker, median of 5 runs after one warm-up run.

| scenario | library          | concurrency | elapsed | msg/s | CPU    | peak RSS |
| -------- | ---------------- | ----------- | ------- | ----- | ------ | -------- |
| fast     | @ssut/nestjs-sqs | 10 (batch)  | 1.47 s  | 2,041 | 0.67 s | 176 MB   |
| fast     | nest-sqs         | 10          | 0.93 s  | 3,237 | 0.76 s | 195 MB   |
| fast     | nest-sqs         | 50          | 0.64 s  | 4,706 | 0.64 s | 187 MB   |
| mixed    | @ssut/nestjs-sqs | 10 (batch)  | 21.20 s | 47    | 0.59 s | 140 MB   |
| mixed    | nest-sqs         | 10          | 3.41 s  | 293   | 0.75 s | 165 MB   |
| mixed    | nest-sqs         | 50          | 1.31 s  | 766   | 0.42 s | 154 MB   |

- **mixed, concurrency 10: 6.2x faster.** Each of the 20 slow messages holds back its whole batch
  with `@ssut/nestjs-sqs` (about 20 x 1 s); nest-sqs keeps the other 9 slots working.
- **fast, concurrency 10: 1.6x faster**, because nest-sqs polls again as soon as a slot frees
  instead of after the whole batch.
- **Costs:** at concurrency 10 nest-sqs uses a little more CPU (0.76 s vs 0.67 s on fast) and
  about 20 MB more peak RSS. It makes more, smaller `ReceiveMessage` calls when slots free one at a
  time.

## What these numbers are not

- **Not AWS.** ElasticMQ answers in well under a millisecond. Against SQS each call takes several
  milliseconds, which makes the extra small receives cost more and the batch barrier cost less in
  the fast case. The mixed result comes from the handler times and should carry over.
- **Not real handlers.** Handlers wait on timers, so CPU is the library's overhead only.
- **Rejected tuning.** Waiting up to 20 ms, 100 ms or 1 s for a full batch of free slots before
  polling (Spring Cloud AWS's AUTO back-pressure) did not help the fast case and made mixed 1.6x
  to 6x slower, so nest-sqs polls for whatever slots are free.
