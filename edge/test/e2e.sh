#!/usr/bin/env bash
# 跨語言端到端（Go 簽章 -> wrangler dev 本機 Worker -> 假源站）。需要 docker（scripts/gorun）與 node。
cd "$(dirname "$0")/.." || exit 1
if command -v nvm-exec >/dev/null 2>&1; then exec nvm-exec node test/e2e.mjs; else exec node test/e2e.mjs; fi
