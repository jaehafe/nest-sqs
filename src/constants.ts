// Keys are the same `Symbol.for` values as `@ssut/nestjs-sqs`, so code that reads the metadata
// directly keeps working after the switch.
export const SQS_OPTIONS: unique symbol = Symbol.for('SQS_OPTIONS');
export const SQS_CONSUMER_METHOD: unique symbol = Symbol.for('SQS_CONSUMER_METHOD');
export const SQS_CONSUMER_EVENT_HANDLER: unique symbol = Symbol.for('SQS_CONSUMER_EVENT_HANDLER');
