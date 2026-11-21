#!/usr/bin/env bash
# 對本機 compose 裡的 MySQL 跑 explain_demo.sql（只動 vod 資料庫的索引，結束時索引會加回去）。
set -euo pipefail
cd "$(dirname "$0")/.."
for i in 1 2 3; do
  echo "##### 第 $i 次 #####"
  docker compose exec -T mysql mysql -uroot -proot --default-character-set=utf8mb4 < scripts/explain_demo.sql 2>/dev/null
done
