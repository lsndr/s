#!/usr/bin/env node
// npm links bins during install, before dist is built, so the bin has to be a committed file
await import('../dist/cli.js');
