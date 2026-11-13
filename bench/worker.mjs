// One benchmark run in its own process: `node worker.mjs <library> <scenario> <queueUrl> <concurrency>`.
// Prints one JSON line with elapsed time, CPU time and peak RSS.
// oxlint-disable-next-line import/no-unassigned-import -- Nest needs the Reflect polyfill loaded first.
import 'reflect-metadata';
import { SQSClient } from '@aws-sdk/client-sqs';
import { Injectable, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { SCENARIOS, handle } from './scenarios.mjs';

const [library, scenarioName, queueUrl, concurrencyArg] = process.argv.slice(2);
const scenario = SCENARIOS[scenarioName];
const lib = await import(library === 'nest-sqs' ? 'nest-sqs' : '@ssut/nestjs-sqs');
const sqs = new SQSClient({
  endpoint: process.env.ELASTICMQ_URL ?? 'http://localhost:9324',
  region: 'us-east-1',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});

let processed = 0;
let started = 0;
let done;
const finished = new Promise((resolve) => (done = resolve));

class Handler {
  async handle(message) {
    if (processed === 0 && started === 0) started = performance.now();
    await handle(scenario, message);
    if (++processed === scenario.messages) done(performance.now());
  }
}
// Plain JS has no decorator syntax, so apply them by hand.
Injectable()(Handler);
const descriptor = Object.getOwnPropertyDescriptor(Handler.prototype, 'handle');
lib.SqsMessageHandler('bench')(Handler.prototype, 'handle', descriptor);

const consumer = {
  name: 'bench',
  queueUrl,
  sqs,
  batchSize: 10,
  waitTimeSeconds: 1,
  ...(library === 'nest-sqs' ? { concurrency: Number(concurrencyArg) } : {}),
};

// oxlint-disable-next-line typescript/no-extraneous-class -- a Nest module class is empty by design.
class AppModule {}
Module({ imports: [lib.SqsModule.register({ consumers: [consumer] })], providers: [Handler] })(
  AppModule,
);

let peakRss = 0;
const sampler = setInterval(() => (peakRss = Math.max(peakRss, process.memoryUsage().rss)), 20);
const cpuBefore = process.cpuUsage();
const appStarted = performance.now();
const app = await NestFactory.createApplicationContext(AppModule, { logger: false });
const ended = await finished;
const cpu = process.cpuUsage(cpuBefore);
clearInterval(sampler);
await app.close();

console.log(
  JSON.stringify({
    elapsedMs: ended - (started || appStarted),
    cpuMs: (cpu.user + cpu.system) / 1000,
    peakRssMb: peakRss / 1024 / 1024,
  }),
);
process.exit(0);
