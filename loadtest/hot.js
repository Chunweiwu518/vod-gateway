// 熱門 API 壓測：模擬首頁／播放頁的流量組合。
//   65% 影片詳情（80% 集中在 1000 支熱門影片，20% 長尾）
//   15% 創作者影片列表   8% 熱門榜   5% 搜尋   5% 播放事件（寫入 Redis Stream）   2% 查不存在的影片（布隆擋掉）
// 執行：loadtest/run.sh <標籤>
import http from 'k6/http'
import { check } from 'k6'

const BASE = __ENV.BASE || 'http://api:8080'
const VUS = Number(__ENV.VUS || 128)
const DURATION = __ENV.DURATION || '30s'
const TERMS = ['旅行', '美食', '料理', '健身', '音樂', '電影', '遊戲', '攝影', '科技', '手作', '露營', '咖啡', 'vlog', 'tutorial', 'review']

export const options = {
  scenarios: { hot: { executor: 'constant-vus', vus: VUS, duration: DURATION } },
  thresholds: { http_req_failed: ['rate<0.01'] },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
}

http.setResponseCallback(http.expectedStatuses(200, 202))

const HOT_ONLY = __ENV.HOT_ONLY === '1'
function pickVideo() {
  return HOT_ONLY || Math.random() < 0.8 ? 1 + Math.floor(Math.random() * 1000) : 1 + Math.floor(Math.random() * 500000)
}

export default function () {
  const r = Math.random()
  let res
  if (r < 0.65) {
    res = http.get(`${BASE}/api/videos/${pickVideo()}`, { tags: { name: 'video' } })
  } else if (r < 0.8) {
    res = http.get(`${BASE}/api/creators/${1 + Math.floor(Math.random() * 200)}/videos?limit=20`, { tags: { name: 'creator_videos' } })
  } else if (r < 0.88) {
    res = http.get(`${BASE}/api/rank/hot?limit=20`, { tags: { name: 'rank' } })
  } else if (r < 0.93) {
    res = http.get(`${BASE}/api/search?q=${encodeURIComponent(TERMS[Math.floor(Math.random() * TERMS.length)])}`, { tags: { name: 'search' } })
  } else if (r < 0.98) {
    res = http.post(`${BASE}/api/play-events`, JSON.stringify({ video_id: pickVideo(), position_sec: Math.floor(Math.random() * 600) }),
      { headers: { 'X-User-Id': String(1 + Math.floor(Math.random() * 100000)), 'Content-Type': 'application/json' }, tags: { name: 'play_event' } })
  } else {
    res = HOT_ONLY ? http.get(`${BASE}/api/videos/${pickVideo()}`) : http.get(`${BASE}/api/videos/${9000000 + Math.floor(Math.random() * 1000000)}`, { tags: { name: 'missing' }, responseCallback: http.expectedStatuses(404) })
  }
  check(res, { 'status ok': (x) => x.status === 200 || x.status === 202 || x.status === 404 })
}

export function handleSummary(data) {
  const m = data.metrics
  const out = {
    qps: m.http_reqs.values.rate,
    requests: m.http_reqs.values.count,
    p50_ms: m.http_req_duration.values.med,
    p95_ms: m.http_req_duration.values['p(95)'],
    p99_ms: m.http_req_duration.values['p(99)'],
    avg_ms: m.http_req_duration.values.avg,
    error_rate: m.http_req_failed.values.rate,
  }
  return { '/lt/results/last.json': JSON.stringify(out, null, 2), stdout: JSON.stringify(out) + '\n' }
}
