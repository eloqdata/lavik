// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './test/browser',
  workers: 1,
  timeout: 150000,
  expect: { timeout: 20000 },
  use: { baseURL: process.env.LAVIK_ADMIN_TEST_URL || 'http://127.0.0.1:4173',
    viewport: { width: 1440, height: 1000 }, screenshot: 'only-on-failure', trace: 'retain-on-failure' },
  reporter: 'list'
});
