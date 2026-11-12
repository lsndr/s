import { beforeEach, describe, expect, it } from 'vitest';
import { ReadmeLinkPinner } from './readme-link-pinner.js';
import { ReleaseRef } from './release-ref.js';

const base = 'https://github.com/lsndr/s';
const tag = '@lsndr/bus@0.18.0';

describe(ReadmeLinkPinner, () => {
  let pinner: ReadmeLinkPinner;

  beforeEach(() => {
    pinner = new ReadmeLinkPinner(
      ReleaseRef.fromManifest({
        name: '@lsndr/bus',
        version: '0.18.0',
        repository: {
          url: 'git+https://github.com/lsndr/s.git',
          directory: 'packages/bus',
        },
      }),
    );
  });

  it('pins a relative link to the tag', () => {
    // arrange
    const markdown = 'See [outbox](docs/outbox.md).';

    // act
    const result = pinner.pin(markdown);

    // assert
    expect(result).toBe(
      `See [outbox](${base}/blob/${tag}/packages/bus/docs/outbox.md).`,
    );
  });

  it('resolves ./ and ../ segments against the package directory', () => {
    // arrange
    const markdown = '[a](./docs/a.md) [b](../domain/README.md)';

    // act
    const result = pinner.pin(markdown);

    // assert
    expect(result).toBe(
      `[a](${base}/blob/${tag}/packages/bus/docs/a.md) [b](${base}/blob/${tag}/packages/domain/README.md)`,
    );
  });

  it('resolves a root-relative link against the repository root', () => {
    // arrange
    const markdown = '[guide](/AGENTS.md)';

    // act
    const result = pinner.pin(markdown);

    // assert
    expect(result).toBe(`[guide](${base}/blob/${tag}/AGENTS.md)`);
  });

  it('keeps the fragment', () => {
    // arrange
    const markdown = '[redis](docs/transports.md#redis-streams)';

    // act
    const result = pinner.pin(markdown);

    // assert
    expect(result).toBe(
      `[redis](${base}/blob/${tag}/packages/bus/docs/transports.md#redis-streams)`,
    );
  });

  it('keeps the link title', () => {
    // arrange
    const markdown = '[outbox](docs/outbox.md "The outbox")';

    // act
    const result = pinner.pin(markdown);

    // assert
    expect(result).toBe(
      `[outbox](${base}/blob/${tag}/packages/bus/docs/outbox.md "The outbox")`,
    );
  });

  it('pins an image to the raw url', () => {
    // arrange
    const markdown = '![diagram](docs/diagram.png)';

    // act
    const result = pinner.pin(markdown);

    // assert
    expect(result).toBe(
      `![diagram](${base}/raw/${tag}/packages/bus/docs/diagram.png)`,
    );
  });

  it('pins a reference definition', () => {
    // arrange
    const markdown = '[outbox]: docs/outbox.md';

    // act
    const result = pinner.pin(markdown);

    // assert
    expect(result).toBe(
      `[outbox]: ${base}/blob/${tag}/packages/bus/docs/outbox.md`,
    );
  });

  it.each([
    ['an absolute url', '[x](https://example.com/a)'],
    ['a mailto link', '[x](mailto:a@example.com)'],
    ['an anchor', '[x](#installation)'],
  ])('leaves %s unchanged', (_name, markdown) => {
    // act
    const result = pinner.pin(markdown);

    // assert
    expect(result).toBe(markdown);
  });

  it('pins an image nested in a link', () => {
    // arrange
    const markdown = '[![logo](docs/logo.png)](https://example.com)';

    // act
    const result = pinner.pin(markdown);

    // assert
    expect(result).toBe(
      `[![logo](${base}/raw/${tag}/packages/bus/docs/logo.png)](https://example.com)`,
    );
  });

  it('throws for a link that leaves the repository', () => {
    // arrange
    const markdown = '[x](../../../outside.md)';

    // act
    const pin = () => pinner.pin(markdown);

    // assert
    expect(pin).toThrow('../../../outside.md');
  });

  it('leaves links in fenced code blocks unchanged', () => {
    // arrange
    const markdown = '```md\n[outbox](docs/outbox.md)\n```';

    // act
    const result = pinner.pin(markdown);

    // assert
    expect(result).toBe(markdown);
  });

  it('leaves links in inline code unchanged', () => {
    // arrange
    const markdown = 'Write `[outbox](docs/outbox.md)` like this.';

    // act
    const result = pinner.pin(markdown);

    // assert
    expect(result).toBe(markdown);
  });
});
