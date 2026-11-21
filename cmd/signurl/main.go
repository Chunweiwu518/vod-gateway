// signurl 簽出帶 exp／sig 的網址，用於手動測試與跨語言驗證（Go 簽、Worker 驗）。
//
//	signurl -secret s3cret -path /hls/1/seg_000.ts -ttl 60
//	signurl -secret s3cret -path /hls/1/seg_000.ts -exp 1700000000   # 指定絕對秒，做過期測試
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Chunweiwu518/vod-gateway/internal/hls"
)

func main() {
	secret := flag.String("secret", "", "HMAC 金鑰（必填）")
	path := flag.String("path", "", "要簽的 URL path，例如 /hls/1/seg_000.ts（必填）")
	ttl := flag.Int64("ttl", 60, "有效秒數（從現在起算）")
	exp := flag.Int64("exp", 0, "絕對到期時間（Unix 秒）；指定時忽略 -ttl")
	flag.Parse()

	if *secret == "" || *path == "" {
		fmt.Fprintln(os.Stderr, "usage: signurl -secret S -path /hls/1/seg_000.ts [-ttl 60 | -exp UNIX_SECONDS]")
		os.Exit(2)
	}
	expAt := time.Now().Add(time.Duration(*ttl) * time.Second)
	if *exp != 0 {
		expAt = time.Unix(*exp, 0)
	}
	fmt.Println(hls.NewSigner(*secret).SignedURL(*path, expAt))
}
