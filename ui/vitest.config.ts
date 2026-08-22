import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    setupFiles: ['./tests/setup.ts'],
    // findEnabledRerunButton in detail-page.test.tsx waits up to 10s for the
    // Rerun gate; the default 5s testTimeout was tight enough to flake on CI.
    testTimeout: 15000,
  },
});
