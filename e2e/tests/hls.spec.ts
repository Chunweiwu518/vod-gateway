import { test, expect } from '@playwright/test'
import { createHmac } from 'node:crypto'

const SECRET = process.env.HLS_SECRET ?? 'dev-secret-change-me'
const GOOD = { Referer: 'http://localhost:8090/' }
const EVIL = { Referer: 'http://evil.example.com/' }

function sign(path: string, exp: number): string {
  const sig = createHmac('sha256', SECRET).update(`${path}\n${exp}`).digest('base64url')
  return `${path}?exp=${exp}&sig=${sig}`
}
const future = () => Math.floor(Date.now() / 1000) + 300
const past = () => Math.floor(Date.now() / 1000) - 10

test.describe('HLS 播放（真的瀏覽器）', () => {
  test('H5 頁面用 hls.js 取簽章網址、取 AES 金鑰、解密並開始播放', async ({ page }) => {
    const seen: { url: string; status: number }[] = []
    page.on('response', (r) => seen.push({ url: r.url(), status: r.status() }))

    await page.goto('/?id=1')
    const h264 = await page.evaluate(() => MediaSource.isTypeSupported('video/mp4; codecs="avc1.4d401f,mp4a.40.2"'))
    expect(h264, '這個瀏覽器必須支援 H.264/AAC').toBe(true)

    await expect
      .poll(() => page.evaluate(() => (document.getElementById('v') as HTMLVideoElement).currentTime), { timeout: 20_000 })
      .toBeGreaterThan(1)

    const state = await page.evaluate(() => (window as any).playState)
    expect(state.error).toBeNull()
    expect(state.keyRequests).toBeGreaterThanOrEqual(1)
    expect(state.segments).toBeGreaterThanOrEqual(1)

    expect(seen.some((r) => r.url.includes('/key/1?') && r.status === 200)).toBe(true)
    expect(seen.some((r) => r.url.includes('/hls/1/seg_000.ts?') && r.status === 200)).toBe(true)
    expect(seen.filter((r) => /\/(hls|key)\//.test(r.url) && r.status >= 400)).toEqual([])
    await page.screenshot({ path: 'test-results/playing.png' })
  })
})

test('對照組：金鑰請求被擋時，播放器出錯且不會開始播放（證明上面真的有解密）', async ({ page }) => {
  await page.route('**/key/**', (r) => r.abort())
  await page.goto('/?id=1')
  await expect.poll(() => page.evaluate(() => (window as any).playState.error), { timeout: 20_000 }).not.toBeNull()
  expect(await page.evaluate(() => (document.getElementById('v') as HTMLVideoElement).currentTime)).toBe(0)
})

test.describe('HLS 鑑權與防盜鏈（直接打 API）', () => {
  test('發播放網址：沒有 Referer 或不在白名單 → 403；白名單內 → 200', async ({ request }) => {
    expect((await request.post('/api/videos/1/play')).status()).toBe(403)
    expect((await request.post('/api/videos/1/play', { headers: EVIL })).status()).toBe(403)
    const ok = await request.post('/api/videos/1/play', { headers: GOOD })
    expect(ok.status()).toBe(200)
    expect((await ok.json()).url).toMatch(/^\/hls\/1\/index\.m3u8\?exp=\d+&sig=/)
  })

  test('m3u8：沒簽章、竄改、過期、Referer 錯 → 403；有效 → 200 且內容已改寫成簽章網址', async ({ request }) => {
    const p = '/hls/1/index.m3u8'
    expect((await request.get(p, { headers: GOOD })).status()).toBe(403)
    expect((await request.get(sign(p, future()).replace(/sig=./, 'sig=A'), { headers: GOOD })).status()).toBe(403)
    expect((await request.get(sign(p, past()), { headers: GOOD })).status()).toBe(403)
    expect((await request.get(sign(p, future()), { headers: EVIL })).status()).toBe(403)
    // 簽章綁路徑：拿影片 1 的簽章去要影片 2 → 403
    expect((await request.get(sign(p, future()).replace('/hls/1/', '/hls/2/'), { headers: GOOD })).status()).toBe(403)

    const ok = await request.get(sign(p, future()), { headers: GOOD })
    expect(ok.status()).toBe(200)
    const body = await ok.text()
    expect(body).toContain('#EXT-X-KEY:METHOD=AES-128,URI="/key/1?exp=')
    expect(body).toMatch(/\/hls\/1\/seg_000\.ts\?exp=\d+&sig=/)
    expect(body).not.toContain('"enc.key"')
  })

  test('AES 金鑰：簽章加 Referer 都對才給 16 bytes；沒有 Referer 一律拒絕', async ({ request }) => {
    const p = '/key/1'
    expect((await request.get(p, { headers: GOOD })).status()).toBe(403)
    expect((await request.get(sign(p, future()))).status()).toBe(403)
    expect((await request.get(sign(p, future()), { headers: EVIL })).status()).toBe(403)
    expect((await request.get(sign(p, past()), { headers: GOOD })).status()).toBe(403)
    const ok = await request.get(sign(p, future()), { headers: GOOD })
    expect(ok.status()).toBe(200)
    expect((await ok.body()).length).toBe(16)
    expect(ok.headers()['cache-control']).toBe('no-store')
  })

  test('TS 分段：簽章驗證；內容確實是加密的（沒有 MPEG-TS 同步位元組 0x47）', async ({ request }) => {
    const p = '/hls/1/seg_000.ts'
    expect((await request.get(p, { headers: GOOD })).status()).toBe(403)
    expect((await request.get(sign(p, past()), { headers: GOOD })).status()).toBe(403)
    const ok = await request.get(sign(p, future()), { headers: GOOD })
    expect(ok.status()).toBe(200)
    expect(ok.headers()['content-type']).toBe('video/mp2t')
    const buf = await ok.body()
    expect(buf.length).toBeGreaterThan(10_000)
    expect(buf[0] === 0x47 && buf[188] === 0x47 && buf[376] === 0x47).toBe(false)
  })

  test('路徑檢查：目錄穿越與不允許的副檔名被拒絕', async ({ request }) => {
    for (const p of ['/hls/1/..%2Findex.m3u8', '/hls/1/enc.key', '/hls/x/index.m3u8', '/hls/1/seg_000.mp4']) {
      const r = await request.get(sign(p, future()), { headers: GOOD })
      expect([400, 403, 404], p).toContain(r.status())
    }
    // 金鑰檔放在公開目錄之外：用 HLS 路徑拿不到
    const k = await request.get(sign('/hls/1/1.key', future()), { headers: GOOD })
    expect(k.status()).not.toBe(200)
  })
})
