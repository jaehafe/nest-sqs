import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // Nest decorators read and write metadata through the global Reflect polyfill.
    setupFiles: ['reflect-metadata'],
  },
});
