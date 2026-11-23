# @lsndr/domain

Lightweight building blocks for DDD-style domain models: aggregate roots that collect domain events, value objects, domain errors, and an ESLint rule that keeps state-recovery constructors out of business code. It has no runtime dependencies.

## Requirements

- Node.js >= 24
- `typescript` (`^5.0.0 || ^6.0.0`), only for the [ESLint rule](#eslint-rule). It is an optional peer dependency.

## Installation

The package is published to GitHub Packages. Point the `@lsndr` scope at that registry in `.npmrc` (authenticating with a token that can read packages), then install:

```ini
@lsndr:registry=https://npm.pkg.github.com
```

```bash
npm install @lsndr/domain
```

## Aggregate root

An aggregate is a cluster of related objects that must stay consistent together, for example an order and its items. The aggregate root is the single entry point to that cluster: outside code never changes the inner objects directly, it calls behaviour on the root, and the root decides whether the change is allowed.

This solves two problems:

- **Scattered invariants.** Rules live next to the data they protect, in one class, instead of being repeated in every service that touches an order. An instance can't be created or changed into an invalid state.
- **Side effects as facts.** When something meaningful happens, the root records a domain event (`OrderPlaced`) instead of calling other parts of the system. The application layer then publishes the collected events, for example through an outbox, after the aggregate is saved.

New aggregates are created through static factories that validate their input and raise events. The public constructor does no validation and is only for restoring an aggregate from persisted state; see [ESLint rule](#eslint-rule).

```typescript
import { AggregateRoot } from '@lsndr/domain';

interface OrderState {
  id: string;
  items: string[];
}

class OrderPlaced {
  public constructor(public readonly orderId: string) {}
}

class Order extends AggregateRoot<OrderState> {
  public static place(id: string, items: string[]): Order {
    Order.assert(items.length > 0, 'no_items');

    const order = new Order({ id, items });
    order.events.add(new OrderPlaced(id));

    return order;
  }
}

const order = Order.place('order-1', ['book']);

order.events.all(); // => [OrderPlaced { orderId: 'order-1' }]
```

## Value object

A value object represents a concept that is defined by its value rather than by an identity, such as an email address or an amount of money. Two emails with the same address are the same email, and a value object is never changed after it is created.

This solves two problems:

- **Primitive obsession.** Passing raw strings and numbers around lets invalid values spread through the code and lets a username be passed where an email is expected. A value object validates once, when it is created, and has its own type, so code that receives an `Email` doesn't need to check it again.
- **Scattered domain logic.** Behaviour that belongs to the value (formatting, comparison, arithmetic) lives on the value object instead of in helpers around the code base.

```typescript
import { ValueObject } from '@lsndr/domain';

class Email extends ValueObject<string, 'Email'> {
  public static create(value: string): Email {
    Email.assert(value.includes('@'), 'invalid_email');

    return new Email(value);
  }

  public get value(): string {
    return this.state;
  }
}

class Username extends ValueObject<string, 'Username'> {}

const email: Email = Email.create('john@example.com');
// const username: Username = email; // type error
```

## Domain errors

`DomainError` is thrown by `assert`. It has two public fields: `owner` (the class that raised it) and `key` (a stable identifier of the broken rule), and its message is `Domain error "<key>" occured in "<owner>"`. You can also throw it yourself.

```typescript
import { DomainError } from '@lsndr/domain';

try {
  Email.create('not-an-email');
} catch (error) {
  if (error instanceof DomainError && error.key === 'invalid_email') {
    // error.owner === 'Email'
  }
}

throw new DomainError('User', 'not_found');
```

## Domain object

`DomainObject<S>` is the base class of `AggregateRoot` and `ValueObject`, and you can extend it directly for other domain concepts, such as entities that live inside an aggregate. It keeps the object's state in a `protected state` field and provides `assert(expression, key)`, available as an instance and as a static method, for enforcing invariants. A failed assertion throws a [`DomainError`](#domain-errors) whose `owner` is the class name, and the call narrows the type of `expression` like a TypeScript assertion function.

The public constructor takes the state as is, with no validation. It is meant only for recreating an instance from persisted state (a repository or mapper). Create new instances through a static factory that validates its input, as in the examples above.

## ESLint rule

A domain object's constructor does no validation and exists only to restore an instance from persisted state. TypeScript can't hide it from business code, so the rule makes using it explicit: outside the class hierarchy, `new` on such a constructor is a lint error that needs an `eslint-disable` comment. Whoever writes that comment has to think twice, and reviewers can see it.

The public constructor of `DomainObject` is tagged with a custom `@stateRecovery` JSDoc tag. The package ships an ESLint rule, `no-state-recovery-constructor-usage`, that reports `new` expressions for any class whose constructor (its own or an inherited one) carries that tag or is `protected`, so that business code can't bypass the factories.

The rule allows:

- `new this(...)`;
- `new` inside the class itself or one of its subclasses, which is where factories live;
- classes that declare their own constructor without the tag, since it overrides the inherited one.

Hydration code outside the class hierarchy should disable the rule where it is intentional, with `// eslint-disable-next-line domain/no-state-recovery-constructor-usage -- <reason>`.

```javascript
import { defineConfig } from 'eslint/config';
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

The rule needs type information, so configure typed linting (for example `languageOptions.parserOptions.projectService: true` with `typescript-eslint`).

## Changelog

Release notes are published as [GitHub releases](https://github.com/lsndr/s/releases?q=%40lsndr%2Fdomain%40&expanded=true).
