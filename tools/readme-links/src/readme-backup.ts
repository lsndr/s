import { copyFile, mkdir, readFile, rename } from 'node:fs/promises';
import { dirname, join } from 'node:path';

export class ReadmeBackup {
  private readonly readmePath: string;
  private readonly backupPath: string;

  public constructor(packageDir: string) {
    this.readmePath = join(packageDir, 'README.md');
    // node_modules is git-ignored and never packed
    this.backupPath = join(
      packageDir,
      'node_modules',
      '.cache',
      'readme-links',
      'README.md',
    );
  }

  /** Copies the readme aside, unless a previous run left a backup behind. */
  public async save(): Promise<void> {
    // a leftover backup means that postpack never ran (e.g. a failed publish),
    // so the readme in place may already be pinned
    if ((await this.read(this.backupPath)) !== undefined) {
      return;
    }

    await mkdir(dirname(this.backupPath), { recursive: true });

    try {
      await copyFile(this.readmePath, this.backupPath);
    } catch (error) {
      if (!this.isMissing(error)) {
        throw error;
      }
    }
  }

  public async original(): Promise<string | undefined> {
    return this.read(this.backupPath);
  }

  public async restore(): Promise<void> {
    try {
      await rename(this.backupPath, this.readmePath);
    } catch (error) {
      if (!this.isMissing(error)) {
        throw error;
      }
    }
  }

  private async read(path: string): Promise<string | undefined> {
    try {
      return await readFile(path, 'utf8');
    } catch (error) {
      if (this.isMissing(error)) {
        return undefined;
      }

      throw error;
    }
  }

  private isMissing(error: unknown): boolean {
    return (error as NodeJS.ErrnoException).code === 'ENOENT';
  }
}
