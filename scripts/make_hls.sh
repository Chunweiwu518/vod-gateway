#!/usr/bin/env bash
# 用 ffmpeg 的 lavfi 合成來源（testsrc／sine，沒有任何版權素材）切成 AES-128 加密的 HLS。
# WSL 沒有原生 ffmpeg，所以用 alpine 的 ffmpeg 套件包成一個本機映像。
# 用法：scripts/make_hls.sh [id ...]   預設 1 2 3
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IDS=("${@:-1 2 3}")
[ $# -eq 0 ] && IDS=(1 2 3)
if ! docker image inspect vod-ffmpeg >/dev/null 2>&1; then
  printf 'FROM alpine:3.20\nRUN apk add --no-cache ffmpeg\nENTRYPOINT ["ffmpeg"]\n' | docker build -q -t vod-ffmpeg -
fi
SRC=(testsrc testsrc2 smptebars)
for id in "${IDS[@]}"; do
  out="$ROOT/data/hls/$id"; keys="$ROOT/data/keys"
  rm -rf "$out"; mkdir -p "$out" "$keys"
  openssl rand 16 > "$keys/$id.key"
  # keyinfo 三行：m3u8 裡的 key URI（之後由 Go 改寫成簽章網址）、金鑰檔路徑、IV
  printf 'enc.key\n/data/keys/%s.key\n%s\n' "$id" "$(openssl rand -hex 16)" > "$keys/$id.keyinfo"
  docker run --rm -u "$(id -u):$(id -g)" -v "$ROOT/data:/data" vod-ffmpeg -loglevel error -y \
    -f lavfi -i "${SRC[$(( (id-1) % 3 ))]}=duration=30:size=640x360:rate=25" \
    -f lavfi -i "sine=frequency=$((330 + id * 110)):duration=30" \
    -c:v libx264 -preset veryfast -pix_fmt yuv420p -profile:v main -g 50 -c:a aac -b:a 64k \
    -f hls -hls_time 6 -hls_playlist_type vod \
    -hls_key_info_file "/data/keys/$id.keyinfo" \
    -hls_segment_filename "/data/hls/$id/seg_%03d.ts" "/data/hls/$id/index.m3u8"
  echo "video $id: $(ls "$out" | wc -l) files"
done
