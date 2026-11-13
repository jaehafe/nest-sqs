import type { Message } from '@aws-sdk/client-sqs';
import { Inject, Injectable, Module } from '@nestjs/common';
import type { INestApplicationContext } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { afterEach, describe, expect, test } from 'vitest';
import {
  SqsConsumerEventHandler,
  SqsMessageHandler,
  SqsModule,
  SqsService,
  getSqsContext,
} from '../src/index.ts';
import type { SqsConsumerOptions, SqsModuleOptionsFactory, SqsOptions } from '../src/index.ts';
import { FakeSqs, deferred, waitFor } from './fake-sqs.ts';

const QUEUE = 'https://sqs.us-east-1.amazonaws.com/000000000000/orders';
const apps: INestApplicationContext[] = [];

async function boot(imports: any[], providers: any[]) {
  const moduleRef = await Test.createTestingModule({ imports, providers }).compile();
  const app = await moduleRef.init();
  apps.push(app);
  return app;
}

afterEach(async () => {
  await Promise.all(apps.splice(0).map((app) => app.close()));
});

function options(sqs: FakeSqs, overrides: Partial<SqsConsumerOptions> = {}): SqsOptions {
  return {
    consumers: [{ name: 'orders', queueUrl: QUEUE, sqs: sqs.asClient(), ...overrides }],
  };
}

describe('SqsModule', () => {
  test('runs the decorated handler with DI and deletes on success', async () => {
    @Injectable()
    class Repo {
      readonly saved: string[] = [];
    }

    @Injectable()
    class Handler {
      constructor(@Inject(Repo) private readonly repo: Repo) {}

      @SqsMessageHandler('orders')
      async handle(message: Message) {
        this.repo.saved.push(message.Body!);
      }
    }

    const sqs = new FakeSqs();
    sqs.add('order-1');
    const app = await boot([SqsModule.register(options(sqs))], [Repo, Handler]);

    await waitFor(() => sqs.deleted.length === 1);
    expect(app.get(Repo).saved).toEqual(['order-1']);
  });

  test('calls event handlers on their own instance', async () => {
    @Injectable()
    class Handler {
      @SqsMessageHandler('orders')
      async handle() {
        throw new Error('boom');
      }
    }

    @Injectable()
    class Monitor {
      readonly errors: string[] = [];

      @SqsConsumerEventHandler('orders', 'processing_error')
      onError(error: Error) {
        this.errors.push(error.message);
      }
    }

    const sqs = new FakeSqs();
    sqs.add('order-1');
    const app = await boot([SqsModule.register(options(sqs))], [Handler, Monitor]);

    await waitFor(() => app.get(Monitor).errors.length === 1);
    expect(app.get(Monitor).errors).toEqual(['boom']);
  });

  // ssut/nestjs-sqs#108: binding at boot ran the original method even after it was wrapped.
  test('calls a method wrapped after boot', async () => {
    @Injectable()
    class Handler {
      readonly calls: string[] = [];

      @SqsMessageHandler('orders')
      async handle() {
        this.calls.push('original');
      }
    }

    const sqs = new FakeSqs();
    sqs.receiveDelayMs = 30;
    const app = await boot([SqsModule.register(options(sqs))], [Handler]);
    const handler = app.get(Handler);
    const original = handler.handle.bind(handler);
    handler.handle = async () => {
      handler.calls.push('wrapped');
      await original();
    };
    sqs.add('order-1');

    await waitFor(() => sqs.deleted.length === 1);
    expect(handler.calls).toEqual(['wrapped', 'original']);
  });

  test('passes all messages of a receive to a batch handler', async () => {
    @Injectable()
    class Handler {
      readonly batches: string[][] = [];

      @SqsMessageHandler('orders', true)
      async handle(messages: Message[]) {
        this.batches.push(messages.map((message) => message.Body!));
      }
    }

    const sqs = new FakeSqs();
    sqs.add('a');
    sqs.add('b');
    const app = await boot([SqsModule.register(options(sqs, { batchSize: 2 }))], [Handler]);

    await waitFor(() => sqs.deleted.length === 2);
    expect(app.get(Handler).batches).toEqual([['a', 'b']]);
  });

  test('close waits for an in-flight handler', async () => {
    const handlerDone = deferred();

    @Injectable()
    class Handler {
      finished = false;

      @SqsMessageHandler('orders')
      async handle() {
        await handlerDone.promise;
        this.finished = true;
      }
    }

    const sqs = new FakeSqs();
    sqs.add('order-1');
    const app = await boot([SqsModule.register(options(sqs))], [Handler]);
    await waitFor(() => app.get(SqsService).consumers.get('orders')!.instance.inFlightCount === 1);

    const closing = app.close();
    apps.splice(apps.indexOf(app), 1);
    setTimeout(() => handlerDone.resolve(), 30);
    await closing;

    expect(app.get(Handler).finished).toBe(true);
    expect(sqs.deleted).toEqual(['order-1']);
  });

  test('registerAsync injects extraProviders into the options factory', async () => {
    const sqs = new FakeSqs();
    sqs.add('order-1');

    @Injectable()
    class Config {
      readonly client = sqs.asClient();
    }

    @Injectable()
    class OptionsFactory implements SqsModuleOptionsFactory {
      constructor(@Inject(Config) private readonly config: Config) {}

      createOptions(): SqsOptions {
        return { consumers: [{ name: 'orders', queueUrl: QUEUE, sqs: this.config.client }] };
      }
    }

    @Injectable()
    class Handler {
      @SqsMessageHandler('orders')
      async handle() {}
    }

    await boot(
      [SqsModule.registerAsync({ useClass: OptionsFactory, extraProviders: [Config] })],
      [Handler],
    );

    await waitFor(() => sqs.deleted.length === 1);
    expect(sqs.remaining).toEqual([]);
  });

  test('rejects two handlers for one consumer', async () => {
    @Injectable()
    class A {
      @SqsMessageHandler('orders')
      async handle() {}
    }

    @Injectable()
    class B {
      @SqsMessageHandler('orders')
      async handle() {}
    }

    @Module({ imports: [SqsModule.register(options(new FakeSqs()))], providers: [A, B] })
    class AppModule {}

    await expect(
      Test.createTestingModule({ imports: [AppModule] })
        .compile()
        .then((ref) => ref.init()),
    ).rejects.toThrow('More than one @SqsMessageHandler for orders: A.handle, B.handle');
  });

  test('send serializes non-string bodies as JSON', async () => {
    const sent: any[] = [];
    const client = {
      send: async (command: { input: any }) => {
        sent.push(...command.input.Entries);
        return { Successful: [], Failed: [] };
      },
    };
    const app = await boot(
      [
        SqsModule.register({
          producers: [{ name: 'orders', queueUrl: QUEUE, sqs: client as any }],
        }),
      ],
      [],
    );

    await app.get(SqsService).send('orders', { id: '1', body: { orderId: 7 } });
    expect(sent).toEqual([{ Id: '1', MessageBody: '{"orderId":7}' }]);
  });
});

describe('SqsModule middleware and manual acknowledgement', () => {
  test('runs module-wide middleware outside consumer middleware, with the consumer name', async () => {
    const order: string[] = [];

    @Injectable()
    class Handler {
      @SqsMessageHandler('orders')
      async handle() {
        order.push(`handler ${getSqsContext()?.name}`);
      }
    }

    const sqs = new FakeSqs();
    sqs.add('order-1');
    await boot(
      [
        SqsModule.register({
          middleware: [
            async (_context, next) => {
              order.push('module');
              await next();
            },
          ],
          consumers: [
            {
              name: 'orders',
              queueUrl: QUEUE,
              sqs: sqs.asClient(),
              middleware: [
                async (_context, next) => {
                  order.push('consumer');
                  await next();
                },
              ],
            },
          ],
        }),
      ],
      [Handler],
    );

    await waitFor(() => sqs.deleted.length === 1);
    expect(order).toEqual(['module', 'consumer', 'handler orders']);
  });

  test('SqsService.deleteMessage acknowledges from inside the handler', async () => {
    @Injectable()
    class Handler {
      constructor(@Inject(SqsService) private readonly sqsService: SqsService) {}

      @SqsMessageHandler('orders')
      async handle(message: Message) {
        await this.sqsService.deleteMessage('orders', message);
      }
    }

    const sqs = new FakeSqs();
    sqs.add('order-1');
    await boot([SqsModule.register(options(sqs))], [Handler]);

    await waitFor(() => sqs.deleted.length === 1);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(sqs.callsOf('DeleteMessageCommand')).toHaveLength(1);
    expect(sqs.callsOf('DeleteMessageBatchCommand')).toHaveLength(0);
  });
});
