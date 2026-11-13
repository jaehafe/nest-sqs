import { describe, expect, test } from 'vitest';
import { SQS_CONSUMER_EVENT_HANDLER, SQS_CONSUMER_METHOD, SQS_OPTIONS } from '../src/index.ts';

// `@ssut/nestjs-sqs` used these exact registered symbols. Keeping them lets code that reads the
// metadata or injects `SQS_OPTIONS` directly keep working after the switch.
describe('metadata keys', () => {
  test.each([
    [SQS_OPTIONS, 'SQS_OPTIONS'],
    [SQS_CONSUMER_METHOD, 'SQS_CONSUMER_METHOD'],
    [SQS_CONSUMER_EVENT_HANDLER, 'SQS_CONSUMER_EVENT_HANDLER'],
  ])('%s is Symbol.for(%j)', (actual, key) => {
    expect(actual).toBe(Symbol.for(key));
  });
});
