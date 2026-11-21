#!/usr/bin/env bash
# 用法：scripts/pprof_top.sh <profile.pb.gz> [pprof 參數...]   在 golang 容器裡跑 go tool pprof -top
f="$1"; shift
"$(dirname "$0")/gorun" go tool pprof -top -nodecount=25 "$@" "$f"
