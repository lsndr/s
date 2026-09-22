import { defineConfig } from 'vitest/config';

// eslint-disable-next-line import-x/no-default-export --- required by Vitest
export default defineConfig({
  test: {
    globals: true,
  },
});
