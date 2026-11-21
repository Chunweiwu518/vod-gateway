import { defineConfig } from '@playwright/test'

// H.264/AAC 在 Playwright 內建的 Chromium 不能播（沒有專利編碼），所以用系統安裝的 Google Chrome。
export default defineConfig({
  testDir: './tests',
  workers: 1,
  failOnFlakyTests: true,
  forbidOnly: true,
  retries: 0,
  reporter: [['list'], ['json', { outputFile: 'test-results/results.json' }]],
  use: {
    baseURL: process.env.BASE_URL ?? 'http://localhost:8090',
    channel: 'chrome',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    launchOptions: { args: ['--autoplay-policy=no-user-gesture-required'] },
  },
})
