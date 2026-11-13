import { defineConfig } from 'tsdown';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  platform: 'node',
  dts: true,
  exports: true,
  publint: { level: 'error' },
  attw: { level: 'error', profile: 'node16' },
});
