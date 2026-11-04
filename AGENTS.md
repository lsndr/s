# Agent guide

## Project

Nx monorepo (npm workspaces, Node >= 24, ESM with NodeNext) of published TypeScript libraries in `packages/*`:

- `bus`: NestJS message bus over Redis Streams and AMQP, with an outbox.
- `domain`: DDD building blocks (aggregate roots, value objects, domain events).
- `logger`: pino-based NestJS logger.

Packages are published to the GitHub registry and versioned independently by `nx release`.

## Making decisions

- Ask questions when requirements are unclear or ambiguous. Don't guess.
- Don't make important decisions on your own. Present the options with their trade-offs and a recommendation, then let the user choose.
- Important decisions include architecture and design choices, public API or data model changes, changing a package's public exports or peer dependencies, adding or removing dependencies, changing scope beyond what was asked, and anything hard to reverse.
- Small, local, easily reversible choices that follow existing conventions don't need a question.

## Writing code

### Prefer OOP

- Model behaviour as classes with clear responsibilities, not loose functions and module-level state.
- Inject dependencies through the constructor instead of importing singletons or reaching for globals.
- Keep state private and expose behaviour through methods.
- Plain functions are fine for small, stateless, pure helpers.

### Comments

- Comment the why, not the what. If a comment only repeats the code, delete it or make the code clearer with better names.
- Give abstract classes and user-facing classes (the ones a package exports from its `index.ts` for consumers to use) a short `/** ... */` doc comment that says what the class is for, in plain language. Don't add doc comments to other classes, types, functions or methods unless they hold a non-obvious decision or constraint, which is then explained as above.
- Write `/** ... */` blocks as full sentences: start with a capital letter and end with a period. Short `//` comments start with a lowercase letter and have no final period. Keep comments to one or two lines where possible.
- Explain non-obvious decisions, constraints and workarounds right where they happen, with a link to the issue or docs if there is one.
- Every `eslint-disable` or `@ts-expect-error` names the rule and gives a reason after `--`.
- Keep comments true: update or remove them in the same change as the code they describe.
- No commented-out code, no change logs or author notes; git keeps the history.
- `TODO` comments say what is missing and link an issue.

### Commits

Use conventional commits (enforced by commitlint). The commit type decides each package's version bump on release.

## Verification

Before considering a change done, run all of these and make sure they pass:

- `npm run build`
- `npx nx run-many -t test:build` (type-checks the spec files)
- `npm test`
- `npm run lint` (use `npx nx run-many -t lint:fix` to fix)
- `npm run format` (use `npm run format:fix` to fix)

## Writing tests

Applies to every Vitest spec (`*.spec.ts`, colocated with the source). New and changed tests follow it; existing ones are not retrofitted.

### Arrange, act, assert

- Every test has `// arrange`, `// act` and `// assert` comments, in lower case.
- One act per test. If a test needs several steps, split it and move the earlier steps into arrange. Don't chain act/assert/act/assert.
- Don't clean up inside a test. Reset stubs and mocks in `afterEach` so cleanup also runs when a test fails.
- Tests that need no setup may omit `// arrange`.

### Unit tests

- Exercise the class through its public API, not its private state.
- Only the edges are faked.
