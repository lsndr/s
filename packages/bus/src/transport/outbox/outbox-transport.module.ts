import { type DynamicModule, Module } from '@nestjs/common';
import { BusConfig } from '../../types.js';
import { type BusTransport, Transport, TransportConfig } from '../transport.js';
import { OutboxTransport } from './outbox.js';

@Module({})
export class OutboxTransportModule {
  public static register(
    inner: BusTransport<Transport>,
    busConfig: BusConfig,
  ): DynamicModule {
    return {
      module: OutboxTransportModule,
      providers: [
        { provide: TransportConfig, useValue: inner.config },
        { provide: BusConfig, useValue: busConfig },
        inner.provider,
        { provide: OutboxTransport, useExisting: inner.provider },
      ],
      exports: [OutboxTransport],
    };
  }
}
