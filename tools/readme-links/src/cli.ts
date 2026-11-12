#!/usr/bin/env node
import { PackageReadme } from './package-readme.js';

// Run from the `prepack` and `postpack` scripts of a package. nx release has
// already bumped the version and created the tag by the time it publishes, so
// the version in package.json is the one the links have to point to. The
// pinned readme only goes into the tarball: postpack puts the original back,
// so the repository keeps relative links.
const command = process.argv[2];
const readme = new PackageReadme(process.cwd());

if (command === 'pin') {
  await readme.pin();
} else if (command === 'restore') {
  await readme.restore();
} else {
  console.error('Usage: readme-links <pin|restore>');
  process.exitCode = 1;
}
