# @lsndr/domain

Lightweight building blocks for DDD-style domain models: `DomainObject`, `AggregateRoot` with event collection, `ValueObject`, and `DomainError`.

## Installation

```bash
npm install @lsndr/domain
```

## Usage

### Aggregate roots with domain events

```typescript
import { AggregateRoot } from '@lsndr/domain';

class Order extends AggregateRoot<OrderState> {
  public place(): void {
    this.events.add(new OrderPlacedEvent(this.state.id));
  }
}

const order = new Order(state);
order.place();

order.events.all(); // => [OrderPlacedEvent]
```

### Value objects

```typescript
import { ValueObject } from '@lsndr/domain';

class Email extends ValueObject<string, 'Email'> {}
```

### Domain errors

```typescript
import { DomainError } from '@lsndr/domain';

throw new DomainError('User', 'invalid_email');
```

## Configuration

None — the package has no runtime dependencies or configuration.

## ESLint rule

`DomainObject`'s constructor is public (so hydration code outside the class hierarchy can call it) but marked with a custom `@stateRecovery` JSDoc tag to discourage direct use for anything other than recreating an instance from persisted state. The package ships an ESLint rule that flags `new` calls on any constructor carrying that tag, anywhere in the type hierarchy:

```javascript
import noStateRecoveryConstructorUsage from '@lsndr/domain/eslint/no-state-recovery-constructor-usage';

const domainPlugin = {
  rules: {
    'no-state-recovery-constructor-usage': noStateRecoveryConstructorUsage,
  },
};

export default defineConfig({
  files: ['**/*.ts'],
  plugins: { domain: domainPlugin },
  rules: { 'domain/no-state-recovery-constructor-usage': 'error' },
});
```

Requires type-aware linting (a `parserOptions.project` pointing at a `tsconfig.json`) and `typescript` as a peer dependency.
