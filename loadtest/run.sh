#!/usr/bin/env bash
# 用法：loadtest/run.sh <標籤> [k6 額外參數，例如 -e VUS=64]
# 先確認 api 容器已經用想測的設定起好。結果存到 loadtest/results/<標籤>.json，
# 並記下這一輪期間 api 的 GC 次數、GC 暫停總時間與每個請求配置的記憶體。
set -euo pipefail
cd "$(dirname "$0")/.."
label="${1:?label}"; shift || true
mkdir -p loadtest/results
scrape() {
  curl -s http://127.0.0.1:8090/metrics | awk '
    /^go_gc_duration_seconds_count /{c=$2}
    /^go_gc_duration_seconds_sum /{s=$2}
    /^go_memstats_alloc_bytes_total /{a=$2}
    /^go_memstats_mallocs_total /{m=$2}
    END{printf "%s %s %s %s\n", c, s, a, m}'
}
read -r c0 s0 a0 m0 < <(scrape)
docker run --rm -u "$(id -u):$(id -g)" --network vod-gateway_default -v "$PWD/loadtest:/lt" \
  grafana/k6:0.54.0 run --quiet "$@" /lt/hot.js > "loadtest/results/$label.k6.txt" 2>&1 || true
read -r c1 s1 a1 m1 < <(scrape)
python3 - "$label" "$c0 $s0 $a0 $m0" "$c1 $s1 $a1 $m1" <<'PY'
import json, sys
label = sys.argv[1]
b = [float(x) for x in sys.argv[2].split()]
e = [float(x) for x in sys.argv[3].split()]
r = json.load(open('loadtest/results/last.json'))
r['gc_cycles'] = int(e[0] - b[0])
r['gc_pause_total_ms'] = round((e[1] - b[1]) * 1000, 1)
r['alloc_bytes_per_req'] = round((e[2] - b[2]) / r['requests'])
r['mallocs_per_req'] = round((e[3] - b[3]) / r['requests'], 1)
json.dump(r, open(f'loadtest/results/{label}.json', 'w'), indent=2)
print(f"{label}: qps={r['qps']:.0f} p50={r['p50_ms']:.2f}ms p95={r['p95_ms']:.2f}ms p99={r['p99_ms']:.2f}ms "
      f"err={r['error_rate']*100:.3f}% gc={r['gc_cycles']} pause={r['gc_pause_total_ms']}ms alloc/req={r['alloc_bytes_per_req']}B mallocs/req={r['mallocs_per_req']}")
PY
