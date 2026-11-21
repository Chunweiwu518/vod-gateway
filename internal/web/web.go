// Package web 內嵌 H5 播放頁與 hls.js。
package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var content embed.FS

func FS() http.FileSystem {
	sub, _ := fs.Sub(content, "static")
	return http.FS(sub)
}
