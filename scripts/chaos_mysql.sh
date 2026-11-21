#!/usr/bin/env bash
# 實機演練：流量進行中把 MySQL 停掉，觀察降級（回舊快取）與熔斷，再把 MySQL 開回來看恢復。
# 軟 TTL 設 4 秒，讓快取很快進入「過期但還留著」的狀態，才能看到 stale。
set -uo pipefail
cd "$(dirname "$0")/.."
export CACHE_TTL=4s L1_TTL=200ms BREAKER_MIN_REQUESTS=10 BREAKER_OPEN_FOR=5s \
  IP_RATE=1000000000 IP_BURST=1000000000 ACCESS_LOG_SAMPLE=100
docker compose up -d --force-recreate api >/dev/null 2>&1
until curl -sf localhost:8090/healthz >/dev/null; do sleep 0.5; done

probe() { # 對 50 支熱門影片各打一次，統計狀態碼與 X-Degraded
  local ok=0 stale=0 e503=0 other=0 src=""
  for id in $(seq 1 50); do
    out=$(curl -s -o /dev/null -D - "localhost:8090/api/videos/$id" | tr -d '\r')
    code=$(echo "$out" | head -1 | awk '{print $2}')
    src="$src $(echo "$out" | awk -F': ' 'tolower($1)=="x-cache"{print $2}')"
    if [ "$code" = 200 ]; then
      if echo "$out" | grep -qi '^x-degraded: stale'; then stale=$((stale+1)); else ok=$((ok+1)); fi
    elif [ "$code" = 503 ]; then e503=$((e503+1)); else other=$((other+1)); fi
  done
  state=$(curl -s localhost:8090/metrics | grep '^vod_breaker_state' | awk '{print $2}')
  echo "$(date +%T) 200新鮮=$ok 200舊值=$stale 503=$e503 其他=$other breaker_state=$state (0=closed 1=half-open 2=open) 來源:$(echo $src | tr ' ' '
' | sort | uniq -c | tr '
' ' ')"
}

echo "== 1. 暖快取"; probe
echo "== 2. 停掉 MySQL（等 6 秒讓軟 TTL 過期）"; docker compose stop mysql >/dev/null 2>&1; sleep 6
for i in 1 2 3; do probe; sleep 1; done
echo "== 3. 沒快取過的影片（長尾 id）在 MySQL 停機時"
for id in 400001 400002 400003; do echo "video $id -> $(curl -s -o /dev/null -w '%{http_code}' localhost:8090/api/videos/$id)"; done
echo "== 4. 開回 MySQL，等熔斷進半開並恢復"
docker compose start mysql >/dev/null 2>&1
until docker compose exec -T mysql mysqladmin ping -h 127.0.0.1 -uroot -proot --silent >/dev/null 2>&1; do sleep 1; done
sleep 6
for i in 1 2 3; do probe; sleep 2; done
echo "video 400001 -> $(curl -s -o /dev/null -w '%{http_code}' localhost:8090/api/videos/400001)"
