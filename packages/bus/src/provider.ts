import {
  type ClassProvider,
  type ExistingProvider,
  type FactoryProvider,
  type InjectionToken,
  type Provider,
  type Type,
  type ValueProvider,
} from '@nestjs/common';

interface WithProviders {
  /**
   * Additional providers the provided value needs. They are registered in the
   * bus module, next to it.
   */
  providers?: Provider[];
}

export type BusProvider<T> =
  | Type<T>
  | (Omit<ClassProvider<T>, 'provide'> & WithProviders)
  | (Omit<ValueProvider<T>, 'provide'> & WithProviders)
  | (Omit<FactoryProvider<T>, 'provide'> & WithProviders)
  | (Omit<ExistingProvider<T>, 'provide'> & WithProviders);

export function toProviders(
  token: InjectionToken,
  provider: BusProvider<unknown>,
): Provider[] {
  if (typeof provider === 'function') {
    return [{ provide: token, useClass: provider }];
  }

  const { providers = [], ...definition } = provider;

  return [...providers, { provide: token, ...definition } as Provider];
}
