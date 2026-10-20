import {
  type ClassProvider,
  type ExistingProvider,
  type FactoryProvider,
  type InjectionToken,
  type Provider,
  type Type,
  type ValueProvider,
} from '@nestjs/common';
import { type Transport } from './transport.js';

interface WithProviders {
  /**
   * Additional providers the transport needs. They are registered in the bus
   * module, next to the transport.
   */
  providers?: Provider[];
}

export type TransportProvider<T extends Transport = Transport> =
  | Type<T>
  | (Omit<ClassProvider<T>, 'provide'> & WithProviders)
  | (Omit<ValueProvider<T>, 'provide'> & WithProviders)
  | (Omit<FactoryProvider<T>, 'provide'> & WithProviders)
  | (Omit<ExistingProvider<T>, 'provide'> & WithProviders);

/** The additional providers of a transport, followed by the transport. */
export function toProviders(
  token: InjectionToken,
  provider: TransportProvider,
): Provider[] {
  if (typeof provider === 'function') {
    return [{ provide: token, useClass: provider }];
  }

  const { providers = [], ...definition } = provider;

  return [...providers, { provide: token, ...definition } as Provider];
}
