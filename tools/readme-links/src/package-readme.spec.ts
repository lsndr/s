import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PackageReadme } from './package-readme.js';

const manifest = {
  name: '@lsndr/bus',
  version: '0.18.0',
  repository: {
    url: 'git+https://github.com/lsndr/s.git',
    directory: 'packages/bus',
  },
};

describe(PackageReadme, () => {
  let dir: string;
  let readme: PackageReadme;

  const readReadme = () => readFile(join(dir, 'README.md'), 'utf8');

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'package-readme-'));
    readme = new PackageReadme(dir);
    await writeFile(join(dir, 'package.json'), JSON.stringify(manifest));
    await writeFile(join(dir, 'README.md'), '[outbox](docs/outbox.md)');
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it('pins the links to the tag of the version in package.json', async () => {
    // act
    await readme.pin();

    // assert
    expect(await readReadme()).toBe(
      '[outbox](https://github.com/lsndr/s/blob/@lsndr/bus@0.18.0/packages/bus/docs/outbox.md)',
    );
  });

  it('pins from the original when pinned twice', async () => {
    // arrange
    await readme.pin();

    // act
    await readme.pin();

    // assert
    expect(await readReadme()).toBe(
      '[outbox](https://github.com/lsndr/s/blob/@lsndr/bus@0.18.0/packages/bus/docs/outbox.md)',
    );
  });

  it('brings back the relative links on restore', async () => {
    // arrange
    await readme.pin();

    // act
    await readme.restore();

    // assert
    expect(await readReadme()).toBe('[outbox](docs/outbox.md)');
  });

  it('does nothing for a package without a readme', async () => {
    // arrange
    await rm(join(dir, 'README.md'));

    // act
    const pin = readme.pin();

    // assert
    await expect(pin).resolves.toBeUndefined();
  });
});
