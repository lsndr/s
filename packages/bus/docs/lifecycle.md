# Lifecycle hooks

The bus calls three optional hooks on **every provider of the bus module**: the transport (whichever module provides it), the serializer, and the classes you list in the `providers` option. A provider implements the ones it needs:

```typescript
import {
  type Transport,
  type OnBusInit,
  type OnBusBootstrap,
  type OnBusShutdown,
} from '@lsndrs/bus';

class MyTransport
  implements Transport, OnBusInit, OnBusBootstrap, OnBusShutdown
{
  // called when the application bootstraps, before the handlers are subscribed
  async onBusInit() {}

  // called once all the handlers are subscribed
  async onBusBootstrap() {}

  // called when the bus shuts down
  async onBusShutdown() {}
}
```

## Order

- Hooks run one provider after another, in registration order (the transport and its own `providers` first, then yours). The bus waits for each.
- `onBusShutdown` runs in reverse. A provider that fails to shut down doesn't stop the others: their hooks still run, and the errors are thrown afterwards (as an `AggregateError` if there is more than one).
- Providers with a request or transient scope have no single instance, so they are skipped.
- Providers of other modules are skipped too, unless the bus module aliases them (like a transport given as `{ useExisting }`).
- An instance is called once, however many aliases point to it.
