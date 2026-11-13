import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/integration/**/*.test.ts'],
    setupFiles: ['reflect-metadata'],
    testTimeout: 30_000,
    hookTimeout: 30_000,
    // Queues are per test, but timing assertions are steadier one file at a time.
    fileParallelism: false,
  },
});
