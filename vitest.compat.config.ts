import { defineConfig } from 'vitest/config';

// Runs the e2e suite of @ssut/nestjs-sqs (test/compat) against nest-sqs. Needs ElasticMQ started with
// test/compat/elasticmq.conf, which creates the FIFO queue and its dead-letter queue the suite expects.
export default defineConfig({
  // The root tsconfig skips test/compat (so the copied file is not type-checked against our stricter
  // settings), and Vite then compiles its decorators as standard ones, which Node cannot run.
  oxc: { decorator: { legacy: true, emitDecoratorMetadata: true } },
  test: {
    include: ['test/compat/**/*.e2e-spec.ts'],
    setupFiles: ['reflect-metadata'],
    testTimeout: 20_000,
    hookTimeout: 30_000,
  },
});
