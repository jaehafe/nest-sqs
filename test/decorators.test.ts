import { Injectable } from '@nestjs/common';
import { DiscoveryModule, DiscoveryService, MetadataScanner, Reflector } from '@nestjs/core';
import { Test } from '@nestjs/testing';
import { describe, expect, expectTypeOf, test } from 'vitest';
import {
  SQS_CONSUMER_EVENT_HANDLER,
  SQS_CONSUMER_METHOD,
  SqsConsumerEventHandler,
  SqsMessageHandler,
} from '../src/index.ts';
import type {
  SqsConsumerEventHandlerMeta,
  SqsConsumerEventName,
  SqsMessageHandlerMeta,
} from '../src/index.ts';

@Injectable()
class OrderHandler {
  @SqsMessageHandler('orders')
  async handle() {}

  @SqsMessageHandler('orders-batch', true)
  async handleBatch() {}

  @SqsConsumerEventHandler('orders', 'processing_error')
  onProcessingError() {}
}

describe('decorators', () => {
  const reflector = new Reflector();

  test('@SqsMessageHandler stores the consumer name and batch flag', () => {
    expect(reflector.get(SQS_CONSUMER_METHOD, OrderHandler.prototype.handle)).toEqual({
      name: 'orders',
      batch: undefined,
    });
    expect(reflector.get(SQS_CONSUMER_METHOD, OrderHandler.prototype.handleBatch)).toEqual({
      name: 'orders-batch',
      batch: true,
    });
  });

  test('@SqsConsumerEventHandler stores the consumer name and event', () => {
    expect(
      reflector.get(SQS_CONSUMER_EVENT_HANDLER, OrderHandler.prototype.onProcessingError),
    ).toEqual({ name: 'orders', eventName: 'processing_error' });
  });

  test('rejects an unknown event name at compile time', () => {
    expectTypeOf<'processing_failed'>().not.toExtend<SqsConsumerEventName>();
    expectTypeOf(SqsConsumerEventHandler).parameter(1).toEqualTypeOf<SqsConsumerEventName>();
  });

  // The module finds handlers with Nest's own DiscoveryService instead of
  // `@golevelup/nestjs-discovery`, whose peer range also stops at NestJS 11.
  test('Nest DiscoveryService finds decorated methods', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [DiscoveryModule],
      providers: [OrderHandler],
    }).compile();
    const discovery = moduleRef.get(DiscoveryService);
    const scanner = moduleRef.get(MetadataScanner);

    const found: SqsMessageHandlerMeta[] = [];
    const events: SqsConsumerEventHandlerMeta[] = [];
    for (const wrapper of discovery.getProviders()) {
      const { instance } = wrapper;
      if (!instance || typeof instance !== 'object') continue;
      const prototype = Object.getPrototypeOf(instance);
      for (const key of scanner.getAllMethodNames(prototype)) {
        const message = reflector.get<SqsMessageHandlerMeta>(SQS_CONSUMER_METHOD, prototype[key]);
        if (message) found.push(message);
        const event = reflector.get<SqsConsumerEventHandlerMeta>(
          SQS_CONSUMER_EVENT_HANDLER,
          prototype[key],
        );
        if (event) events.push(event);
      }
    }

    expect(found.map((meta) => meta.name).toSorted()).toEqual(['orders', 'orders-batch']);
    expect(events).toEqual([{ name: 'orders', eventName: 'processing_error' }]);
  });
});
