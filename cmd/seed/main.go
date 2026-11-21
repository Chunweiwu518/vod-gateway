// seed 產生測試資料（全部是合成的，不含任何真實影片或個資）。
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/Chunweiwu518/vod-gateway/internal/config"
	"github.com/Chunweiwu518/vod-gateway/internal/store"
)

var zh = []string{"旅行", "美食", "料理", "健身", "音樂", "電影", "遊戲", "攝影", "科技", "手作", "露營", "咖啡", "寵物", "運動", "開箱", "教學", "日常", "挑戰", "劇場", "紀錄片"}
var en = []string{"vlog", "tutorial", "review", "guide", "tips", "highlights", "behind the scenes", "challenge", "live", "unboxing", "workout", "recipe", "travel", "gaming", "coding"}
var tail = []string{"完整版", "第一集", "精華", "懶人包", "新手入門", "進階技巧", "一次看完", "深度解析", "實測", "分享"}

func pick(r *rand.Rand, s []string) string { return s[r.IntN(len(s))] }

func main() {
	nCreators := flag.Int("creators", 2000, "創作者數")
	nVideos := flag.Int("videos", 500000, "影片數")
	flag.Parse()
	ctx := context.Background()
	st, err := store.Open(config.Str("MYSQL_DSN", "app:app@tcp(127.0.0.1:13306)/vod?parseTime=true&charset=utf8mb4"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	if err := st.Migrate(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	var have int
	_ = st.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM videos`).Scan(&have)
	if have >= *nVideos {
		fmt.Println("videos already seeded:", have)
		return
	}
	r := rand.New(rand.NewPCG(42, 7))
	now := time.Now()

	var cc int
	_ = st.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM creators`).Scan(&cc)
	if cc == 0 {
		insertBatch(ctx, st, "creators (name, followers, created_at)", *nCreators, 1000, func(i int) []any {
			return []any{fmt.Sprintf("%s頻道%d", pick(r, zh), i+1), r.IntN(1_000_000), now.Add(-time.Duration(r.IntN(900*24)) * time.Hour)}
		})
	}
	// 前 3 支是 HLS 示範用的影片（對應 data/hls/1..3）。
	insertBatch(ctx, st, "videos (creator_id, title, description, duration_sec, play_count, fav_count, created_at)", *nVideos, 2000, func(i int) []any {
		title := fmt.Sprintf("%s %s %s %d", pick(r, zh), pick(r, en), pick(r, tail), r.IntN(1000))
		if i < 3 {
			title = fmt.Sprintf("HLS 測試影片 %d testsrc 加密串流", i+1)
		}
		desc := strings.Join([]string{pick(r, zh), pick(r, zh), pick(r, en), pick(r, tail)}, " ")
		return []any{1 + r.IntN(*nCreators), title, desc, 30 + r.IntN(3600), r.IntN(100000), r.IntN(500), now.Add(-time.Duration(r.IntN(365*24*60)) * time.Minute)}
	})
	fmt.Println("done")
}

func insertBatch(ctx context.Context, st *store.Store, target string, total, batch int, row func(i int) []any) {
	start := time.Now()
	for done := 0; done < total; done += batch {
		n := min(batch, total-done)
		var sb strings.Builder
		var args []any
		first := row(done)
		ph := "(" + strings.TrimSuffix(strings.Repeat("?,", len(first)), ",") + ")"
		sb.WriteString("INSERT INTO " + target + " VALUES ")
		for i := 0; i < n; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(ph)
			if i == 0 {
				args = append(args, first...)
			} else {
				args = append(args, row(done+i)...)
			}
		}
		if _, err := st.DB.ExecContext(ctx, sb.String(), args...); err != nil {
			fmt.Fprintln(os.Stderr, "insert:", err)
			os.Exit(1)
		}
		if (done/batch)%20 == 0 {
			fmt.Printf("%s: %d/%d (%.0fs)\n", strings.Fields(target)[0], done+n, total, time.Since(start).Seconds())
		}
	}
}
