import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { ReadmeBackup } from './readme-backup.js';
import { ReadmeLinkPinner } from './readme-link-pinner.js';
import { ReleaseRef, type PackageManifest } from './release-ref.js';

export class PackageReadme {
  private readonly backup: ReadmeBackup;

  public constructor(private readonly packageDir: string) {
    this.backup = new ReadmeBackup(packageDir);
  }

  public async pin(): Promise<void> {
    await this.backup.save();

    const original = await this.backup.original();

    if (original === undefined) {
      return;
    }

    const manifest = JSON.parse(
      await readFile(join(this.packageDir, 'package.json'), 'utf8'),
    ) as PackageManifest;
    const pinner = new ReadmeLinkPinner(ReleaseRef.fromManifest(manifest));

    await writeFile(join(this.packageDir, 'README.md'), pinner.pin(original));
  }

  public async restore(): Promise<void> {
    await this.backup.restore();
  }
}
