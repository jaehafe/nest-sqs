import { Global, Module } from '@nestjs/common';
import type { DynamicModule, Provider } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { SQS_OPTIONS } from './constants.ts';
import { SqsService } from './sqs.service.ts';
import type { SqsModuleAsyncOptions, SqsModuleOptionsFactory, SqsOptions } from './types.ts';

@Global()
@Module({})
export class SqsModule {
  static register(options: SqsOptions): DynamicModule {
    return {
      global: true,
      module: SqsModule,
      imports: [DiscoveryModule],
      providers: [{ provide: SQS_OPTIONS, useValue: options }, SqsService],
      exports: [SqsService],
    };
  }

  static registerAsync(options: SqsModuleAsyncOptions): DynamicModule {
    return {
      global: true,
      module: SqsModule,
      imports: [DiscoveryModule, ...(options.imports ?? [])],
      providers: [...createAsyncProviders(options), ...(options.extraProviders ?? []), SqsService],
      exports: [SqsService],
    };
  }
}

function createAsyncProviders(options: SqsModuleAsyncOptions): Provider[] {
  if (options.useFactory) {
    return [{ provide: SQS_OPTIONS, useFactory: options.useFactory, inject: options.inject ?? [] }];
  }
  const factoryClass = options.useClass ?? options.useExisting;
  if (!factoryClass) {
    throw new Error('SqsModule.registerAsync needs one of useFactory, useClass or useExisting');
  }
  const optionsProvider: Provider = {
    provide: SQS_OPTIONS,
    useFactory: (factory: SqsModuleOptionsFactory) => factory.createOptions(),
    inject: [factoryClass],
  };
  return options.useClass
    ? [optionsProvider, { provide: factoryClass, useClass: factoryClass }]
    : [optionsProvider];
}
