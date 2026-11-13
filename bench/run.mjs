// Runs every library x scenario RUNS times against ElasticMQ with a fresh queue each time and prints
// the median of each. `node run.mjs` after `node setup.mjs` and with ElasticMQ on :9324.
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  SQSClient,
  SendMessageBatchCommand,
} from '@aws-sdk/client-sqs';
import { SCENARIOS } from './scenarios.mjs';

const RUNS = Number(process.env.RUNS ?? 3);
const sqs = new SQSClient({
  endpoint: process.env.ELASTICMQ_URL ?? 'http://localhost:9324',
  region: 'us-east-1',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});

// Both libraries handle at most 10 messages at once here: @ssut/nestjs-sqs runs one batch of 10 at
// a time, and nest-sqs gets concurrency 10. The last row shows nest-sqs with more slots.
const CASES = [
  { library: '@ssut/nestjs-sqs', concurrency: 10 },
  { library: 'nest-sqs', concurrency: 10 },
  { library: 'nest-sqs', concurrency: 50 },
];

async function filledQueue(count) {
  const { QueueUrl } = await sqs.send(
    new CreateQueueCommand({ QueueName: `bench-${randomUUID()}` }),
  );
  for (let i = 0; i < count; i += 10) {
    const ids = Array.from({ length: Math.min(10, count - i) }, (_, j) => i + j);
    await sqs.send(
      new SendMessageBatchCommand({
        QueueUrl,
        Entries: ids.map((id) => ({ Id: String(id), MessageBody: String(id) })),
      }),
    );
  }
  return QueueUrl;
}

const median = (values) => values.toSorted((a, b) => a - b)[Math.floor(values.length / 2)];
const rows = [];
const only = process.env.SCENARIO;
const onlyLibrary = process.env.LIBRARY;
for (const [scenarioName, scenario] of Object.entries(SCENARIOS)) {
  if (only && only !== scenarioName) continue;
  // ElasticMQ runs on the JVM and is slow until warmed up; the first run of each scenario is
  // thrown away so it does not count against whichever library goes first.
  {
    const queueUrl = await filledQueue(scenario.messages);
    execFileSync('node', ['worker.mjs', 'nest-sqs', scenarioName, queueUrl, '10'], {
      cwd: import.meta.dirname,
    });
    await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
  }
  for (const { library, concurrency } of CASES) {
    if (onlyLibrary && onlyLibrary !== library) continue;
    const results = [];
    for (let run = 0; run < RUNS; run++) {
      const queueUrl = await filledQueue(scenario.messages);
      const output = execFileSync(
        'node',
        ['worker.mjs', library, scenarioName, queueUrl, String(concurrency)],
        { cwd: import.meta.dirname, encoding: 'utf8' },
      );
      results.push(JSON.parse(output.trim().split('\n').at(-1)));
      await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    }
    const elapsedMs = median(results.map((result) => result.elapsedMs));
    rows.push({
      scenario: scenarioName,
      library,
      concurrency: library === 'nest-sqs' ? concurrency : '10 (batch)',
      'elapsed s': (elapsedMs / 1000).toFixed(2),
      'msg/s': Math.round(scenario.messages / (elapsedMs / 1000)),
      'CPU s': (median(results.map((result) => result.cpuMs)) / 1000).toFixed(2),
      'peak RSS MB': Math.round(median(results.map((result) => result.peakRssMb))),
    });
    console.error(`done: ${scenarioName} ${library} ${concurrency}`);
  }
}
console.table(rows);
