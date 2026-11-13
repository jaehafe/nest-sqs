import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // Needs an SQS server; run with `pnpm test:integration`.
    exclude: ['test/integration/**'],
    // Nest decorators read and write metadata through the global Reflect polyfill.
    setupFiles: ['reflect-metadata'],
  },
});
