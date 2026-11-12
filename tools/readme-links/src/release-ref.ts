export interface PackageManifest {
  name?: string;
  version?: string;
  repository?: { url?: string; directory?: string } | string;
}

export class ReleaseRef {
  private constructor(
    private readonly repositoryUrl: string,
    public readonly tag: string,
    public readonly directory: string,
  ) {}

  public static fromManifest(manifest: PackageManifest): ReleaseRef {
    const { name, version, repository } = manifest;

    if (!name || !version) {
      throw new Error('package.json needs a name and a version');
    }

    if (!repository || typeof repository === 'string' || !repository.url) {
      throw new Error('package.json needs a repository with a url');
    }

    const match = /github\.com[/:]([^/]+\/[^/]+?)(?:\.git)?\/?$/.exec(
      repository.url,
    );

    if (!match) {
      throw new Error(
        `Only GitHub repositories are supported, got ${repository.url}`,
      );
    }

    return new ReleaseRef(
      `https://github.com/${match[1]}`,
      // same shape as releaseTag.pattern in nx.json
      `${name}@${version}`,
      repository.directory ?? '',
    );
  }

  public blobUrl(repoPath: string): string {
    return this.url('blob', repoPath);
  }

  public rawUrl(repoPath: string): string {
    return this.url('raw', repoPath);
  }

  private url(kind: 'blob' | 'raw', repoPath: string): string {
    // GitHub resolves the "/" and "@" of the tag unencoded
    return `${this.repositoryUrl}/${kind}/${this.tag}/${repoPath}`;
  }
}
