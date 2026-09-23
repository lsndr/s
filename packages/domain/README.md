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
