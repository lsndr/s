import { describe, expect, it } from 'vitest';
import { ReleaseRef } from './release-ref.js';

const manifest = {
  name: '@lsndr/bus',
  version: '0.18.0',
  repository: {
    type: 'git',
    url: 'git+https://github.com/lsndr/s.git',
    directory: 'packages/bus',
  },
};

describe(ReleaseRef, () => {
  it('names the tag like the nx release tag pattern', () => {
    // arrange
    const ref = ReleaseRef.fromManifest(manifest);

    // act
    const tag = ref.tag;

    // assert
    expect(tag).toBe('@lsndr/bus@0.18.0');
  });

  it('exposes the package directory', () => {
    // arrange
    const ref = ReleaseRef.fromManifest(manifest);

    // act
    const directory = ref.directory;

    // assert
    expect(directory).toBe('packages/bus');
  });

  it('builds a blob url pinned to the tag', () => {
    // arrange
    const ref = ReleaseRef.fromManifest(manifest);

    // act
    const url = ref.blobUrl('packages/bus/docs/outbox.md');

    // assert
    expect(url).toBe(
      'https://github.com/lsndr/s/blob/@lsndr/bus@0.18.0/packages/bus/docs/outbox.md',
    );
  });

  it('builds a raw url pinned to the tag', () => {
    // arrange
    const ref = ReleaseRef.fromManifest(manifest);

    // act
    const url = ref.rawUrl('packages/bus/docs/diagram.png');

    // assert
    expect(url).toBe(
      'https://github.com/lsndr/s/raw/@lsndr/bus@0.18.0/packages/bus/docs/diagram.png',
    );
  });

  it('accepts a plain https repository url', () => {
    // arrange
    const ref = ReleaseRef.fromManifest({
      ...manifest,
      repository: {
        ...manifest.repository,
        url: 'https://github.com/lsndr/s',
      },
    });

    // act
    const url = ref.blobUrl('a.md');

    // assert
    expect(url).toBe('https://github.com/lsndr/s/blob/@lsndr/bus@0.18.0/a.md');
  });

  it('throws when the repository is missing', () => {
    // arrange
    const withoutRepository = {
      name: manifest.name,
      version: manifest.version,
    };

    // act
    const create = () => ReleaseRef.fromManifest(withoutRepository);

    // assert
    expect(create).toThrow('repository');
  });

  it('throws when the repository is not on GitHub', () => {
    // arrange
    const gitlab = {
      ...manifest,
      repository: {
        ...manifest.repository,
        url: 'git+https://gitlab.com/lsndr/s.git',
      },
    };

    // act
    const create = () => ReleaseRef.fromManifest(gitlab);

    // assert
    expect(create).toThrow('GitHub');
  });
});
