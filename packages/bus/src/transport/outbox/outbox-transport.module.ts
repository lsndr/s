import { type DynamicModule, Module } from '@nestjs/common';
import { type BusTransport, Transport, TransportConfig } from '../transport.js';
import { OutboxTransport } from './outbox.js';

@Module({})
export class OutboxTransportModule {
  public static register(inner: BusTransport<Transport>): DynamicModule {
    return {
      module: OutboxTransportModule,
      providers: [
        { provide: TransportConfig, useValue: inner.config },
        inner.provider,
        { provide: OutboxTransport, useExisting: inner.provider },
      ],
      exports: [OutboxTransport],
    };
  }
}
