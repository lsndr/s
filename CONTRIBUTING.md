# Contributing

Thanks for your interest in contributing. This repository is an [Nx](https://nx.dev) monorepo of TypeScript packages in [`packages`](packages). Bug reports and feature requests are welcome as [issues](https://github.com/lsndr/s/issues); for a larger change, please open an issue first so the approach can be agreed on before you spend time on it.

To report a security vulnerability, follow [SECURITY.md](SECURITY.md) instead of opening an issue.

## Setup

You need Node.js 24 or newer and npm. The `@lsndrs/bus` tests start Redis and RabbitMQ containers with [Testcontainers](https://testcontainers.com), so they also need a running Docker daemon.

```bash
npm ci
```

This installs the dependencies of every package and sets up the Git hooks.

## Checks

Run all of these before opening a pull request. CI runs the same checks (on Node 24 and 26) for the affected packages.

Code style, comments and the way tests are written are described in [AGENTS.md](AGENTS.md). It is written for coding agents, but it applies to everyone.

## Commits

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org), for example `fix(bus): stop reclaiming messages twice`. The scope is the package name without `@lsndrs/`. If your pull request is squash-merged, its title becomes the commit message, so it needs to follow the same format.

The commit type decides the version bump of the affected packages when they are released:

| Type                                                 | Bump  |
| ---------------------------------------------------- | ----- |
| `feat`                                               | minor |
| `fix`, `docs`, `perf`, `refactor`, `build`, `revert` | patch |
| any type with `!` or a `BREAKING CHANGE:` footer     | major |

While a package is on `0.x`, a major bump is released as a minor one and a minor bump as a patch (`adjustSemverBumpsForZeroMajorVersion` in [`nx.json`](nx.json)).

## Releases

Releases are automated; contributors don't bump versions or edit changelogs. Every push to `main` runs the [release workflow](.github/workflows/release.yml): [`nx release`](https://nx.dev/features/manage-releases) bumps the packages that changed, tags them as `<package>@<version>`, creates a GitHub release with generated notes and publishes them to npm. The releases page is the changelog.
