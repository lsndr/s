import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ReadmeBackup } from './readme-backup.js';

describe(ReadmeBackup, () => {
  let dir: string;
  let backup: ReadmeBackup;

  const writeReadme = (content: string) =>
    writeFile(join(dir, 'README.md'), content);
  const readReadme = () => readFile(join(dir, 'README.md'), 'utf8');

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'readme-backup-'));
    backup = new ReadmeBackup(dir);
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it('returns the readme as it was when saved', async () => {
    // arrange
    await writeReadme('original');
    await backup.save();
    await writeReadme('pinned');

    // act
    const original = await backup.original();

    // assert
    expect(original).toBe('original');
  });

  it('keeps the first backup when saved again', async () => {
    // arrange
    await writeReadme('original');
    await backup.save();
    await writeReadme('pinned');
    await backup.save();

    // act
    const original = await backup.original();

    // assert
    expect(original).toBe('original');
  });

  it('puts the saved readme back on restore', async () => {
    // arrange
    await writeReadme('original');
    await backup.save();
    await writeReadme('pinned');

    // act
    await backup.restore();

    // assert
    expect(await readReadme()).toBe('original');
  });

  it('starts from the current readme after a restore', async () => {
    // arrange
    await writeReadme('first');
    await backup.save();
    await backup.restore();
    await writeReadme('second');
    await backup.save();

    // act
    const original = await backup.original();

    // assert
    expect(original).toBe('second');
  });

  it('leaves the readme alone on restore without a backup', async () => {
    // arrange
    await writeReadme('untouched');

    // act
    await backup.restore();

    // assert
    expect(await readReadme()).toBe('untouched');
  });

  it('returns nothing when there is no readme', async () => {
    // arrange
    await backup.save();

    // act
    const original = await backup.original();

    // assert
    expect(original).toBeUndefined();
  });
});
