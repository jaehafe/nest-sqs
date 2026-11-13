import { SetMetadata } from '@nestjs/common';
import type { CustomDecorator } from '@nestjs/common';
import { SQS_CONSUMER_EVENT_HANDLER, SQS_CONSUMER_METHOD } from './constants.ts';
import type {
  QueueName,
  SqsConsumerEventHandlerMeta,
  SqsConsumerEventName,
  SqsMessageHandlerMeta,
} from './types.ts';

/**
 * Marks a provider method as the handler for the consumer named `name`.
 *
 * With `batch` the method receives every message of one `ReceiveMessage` call as an array.
 */
export function SqsMessageHandler(name: QueueName, batch?: boolean): CustomDecorator<symbol> {
  return SetMetadata<symbol, SqsMessageHandlerMeta>(SQS_CONSUMER_METHOD, { name, batch });
}

/** Marks a provider method as a listener for `eventName` on the consumer named `name`. */
export function SqsConsumerEventHandler(
  name: QueueName,
  eventName: SqsConsumerEventName,
): CustomDecorator<symbol> {
  return SetMetadata<symbol, SqsConsumerEventHandlerMeta>(SQS_CONSUMER_EVENT_HANDLER, {
    name,
    eventName,
  });
}
