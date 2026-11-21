//go:build integration

// 整合測試：連真的 MySQL 與 Redis（docker compose up -d mysql redis 之後執行）。
// 執行：scripts/gorun go test -tags integration -count=1 -v ./internal/integration
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chunweiwu518/vod-gateway/internal/api"
	"github.com/Chunweiwu518/vod-gateway/internal/cache"
	"github.com/Chunweiwu518/vod-gateway/internal/events"
	"github.com/Chunweiwu518/vod-gateway/internal/rank"
	"github.com/Chunweiwu518/vod-gateway/internal/resilience"
	"github.com/Chunweiwu518/vod-gateway/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const (
	rootDSN = "root:root@tcp(127.0.0.1:13306)/?parseTime=true"
	itDSN   = "root:root@tcp(127.0.0.1:13306)/vod_it?parseTime=true&charset=utf8mb4"
)

func redisAddr() string { return "127.0.0.1:16379" }

// freshStore 建一個乾淨的 vod_it 資料庫（每個測試各自重建）。
func freshStore(t *testing.T) *store.Store {
	t.Helper()
	root, err := sql.Open("mysql", rootDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	for _, q := range []string{"DROP DATABASE IF EXISTS vod_it", "CREATE DATABASE vod_it"} {
		if _, err := root.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	st, err := store.Open(itDSN)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.DB.Close() })
	return st
}

func freshRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr(), DB: 5})
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rdb.FlushDB(context.Background()); rdb.Close() })
	return rdb
}

func seedSmall(t *testing.T, st *store.Store, videos int) {
	t.Helper()
	ctx := context.Background()
	if _, err := st.DB.ExecContext(ctx, `INSERT INTO creators (name, followers) VALUES ('旅行頻道1', 10), ('旅行頻道2', 20), ('美食頻道', 30)`); err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	sb.WriteString(`INSERT INTO videos (creator_id, title, description, duration_sec) VALUES `)
	for i := 0; i < videos; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `(%d, '旅行 vlog 第%d集', '露營 咖啡 教學', %d)`, 1+i%3, i, 60+i)
	}
	if _, err := st.DB.ExecContext(ctx, sb.String()); err != nil {
		t.Fatal(err)
	}
}

func newAPI(t *testing.T, st *store.Store, rdb *redis.Client) http.Handler {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	ids, err := st.AllVideoIDs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	_, h := api.New(api.Config{IPRate: 1e9, IPBurst: 1 << 30, UserRate: 1000, UserBurst: 1000}, api.Deps{
		Store: st, Cache: cache.New(cache.RedisRemote{C: rdb}, cache.Options{Logger: quiet, Jitter: 0.2}),
		Breaker: resilience.NewBreaker(resilience.BreakerConfig{Name: "it", Logger: quiet}),
		Bloom:   api.NewBloom(ids, 0.01), Board: rank.New(rdb, "rank:it"), Publisher: events.NewProducer(rdb),
		UserLim: resilience.NewUserLimiter(rdb, 1000, 1000), Registry: reg,
	})
	return h
}

func comSelect(t *testing.T, st *store.Store) int64 {
	t.Helper()
	var name string
	var v int64
	if err := st.DB.QueryRow(`SHOW GLOBAL STATUS LIKE 'Com_select'`).Scan(&name, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// 擊穿，量真的 MySQL：1000 個併發請求打同一支冷門影片，MySQL 的 Com_select 計數只增加 1。
func TestColdKey1000Concurrent_RealMySQLComSelectDelta(t *testing.T) {
	st, rdb := freshStore(t), freshRedis(t)
	seedSmall(t, st, 30)
	h := newAPI(t, st, rdb)

	before := comSelect(t, st)
	gate := make(chan struct{})
	var wg sync.WaitGroup
	var bad atomic.Int64
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/api/videos/7", nil))
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"id":7`) {
				bad.Add(1)
			}
		}()
	}
	close(gate)
	wg.Wait()
	delta := comSelect(t, st) - before
	t.Logf("1000 個併發請求 → MySQL Com_select 增加 %d", delta)
	if bad.Load() != 0 {
		t.Fatalf("%d bad responses", bad.Load())
	}
	if delta != 1 {
		t.Fatalf("Com_select delta=%d, want 1", delta)
	}
}

// 穿透，量真的 MySQL：1000 次打一個已刪除（布隆誤判）的 id，MySQL 只被查 1 次。
func TestDeletedVideo1000Requests_RealMySQLComSelectDelta(t *testing.T) {
	st, rdb := freshStore(t), freshRedis(t)
	seedSmall(t, st, 10)
	h := newAPI(t, st, rdb) // 布隆含 1..10
	if _, err := st.DB.Exec(`DELETE FROM videos WHERE id = 5`); err != nil {
		t.Fatal(err)
	}
	before := comSelect(t, st)
	for i := 0; i < 1000; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/api/videos/5", nil))
		if w.Code != 404 {
			t.Fatalf("got %d", w.Code)
		}
	}
	if d := comSelect(t, st) - before; d != 1 {
		t.Fatalf("Com_select delta=%d, want 1", d)
	}
}

func TestSchema_IndexesUsedAndFulltextSearch(t *testing.T) {
	st := freshStore(t)
	seedSmall(t, st, 300)
	ctx := context.Background()

	explainKey := func(q string, args ...any) string {
		rows, err := st.DB.QueryContext(ctx, "EXPLAIN "+q, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]sql.RawBytes, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			rows.Scan(ptrs...)
			for i, c := range cols {
				if c == "key" {
					return string(vals[i])
				}
			}
		}
		return ""
	}
	if k := explainKey(`SELECT id FROM videos WHERE creator_id = ? ORDER BY created_at DESC, id DESC LIMIT 20`, 1); k != "idx_creator_created" {
		t.Fatalf("list-by-creator uses key %q, want idx_creator_created", k)
	}
	if k := explainKey(`SELECT id FROM creators WHERE name LIKE ? LIMIT 5`, "旅行%"); k != "idx_name" {
		t.Fatalf("creator prefix search uses key %q, want idx_name", k)
	}
	if k := explainKey(`SELECT user_id FROM play_history WHERE user_id = ? ORDER BY updated_at DESC LIMIT 20`, 1); k != "idx_user_updated" {
		t.Fatalf("history uses key %q, want idx_user_updated", k)
	}

	vs, err := st.SearchVideos(ctx, "旅行", 10)
	if err != nil || len(vs) == 0 {
		t.Fatalf("fulltext search: %v %v", vs, err)
	}
	cs, err := st.SearchCreators(ctx, "旅行", 10)
	if err != nil || len(cs) != 2 {
		t.Fatalf("creator search: %v %v", cs, err)
	}
	if cs, _ := st.SearchCreators(ctx, "%", 10); len(cs) != 0 {
		t.Fatalf("wildcard must be escaped, got %d rows", len(cs))
	}
}

func TestFavorites_IdempotentAndCounted(t *testing.T) {
	st := freshStore(t)
	seedSmall(t, st, 5)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := st.AddFavorite(ctx, 1, 2); err != nil {
			t.Fatal(err)
		}
	}
	st.AddFavorite(ctx, 2, 2)
	v, _ := st.GetVideo(ctx, 2)
	if v.FavCount != 2 {
		t.Fatalf("fav_count=%d want 2 (duplicates must not count)", v.FavCount)
	}
	st.RemoveFavorite(ctx, 1, 2)
	st.RemoveFavorite(ctx, 1, 2)
	if v, _ = st.GetVideo(ctx, 2); v.FavCount != 1 {
		t.Fatalf("fav_count=%d want 1", v.FavCount)
	}
}

// 非同步管線，真的 Redis Stream + MySQL：API 收 600 筆播放事件回 202，consumer 聚合後 play_count 與排行榜正確。
func TestPlayEventsEndToEnd_StreamToMySQLAndRank(t *testing.T) {
	st, rdb := freshStore(t), freshRedis(t)
	seedSmall(t, st, 10)
	h := newAPI(t, st, rdb)
	board := rank.New(rdb, "rank:it")
	c := events.NewConsumer(rdb, st, board, events.ConsumerConfig{Name: "it", Block: 100 * time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err := c.EnsureGroup(context.Background()); err != nil {
		t.Fatal(err)
	}

	want := map[int64]int{1: 300, 2: 200, 3: 100}
	n := 0
	for vid, cnt := range want {
		for i := 0; i < cnt; i++ {
			body := fmt.Sprintf(`{"video_id":%d,"position_sec":%d}`, vid, i)
			r := httptest.NewRequest("POST", "/api/play-events", strings.NewReader(body))
			r.Header.Set("X-User-Id", strconv.Itoa(1+n%5))
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 202 {
				t.Fatalf("publish %d: %d %s", n, w.Code, w.Body.String())
			}
			n++
		}
	}
	for read := 0; read < n; {
		got, err := c.ReadOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got == 0 {
			break
		}
		read += got
	}
	for vid, cnt := range want {
		v, err := st.GetVideo(context.Background(), vid)
		if err != nil || v.PlayCount != int64(cnt) {
			t.Fatalf("video %d play_count=%d err=%v want %d", vid, v.PlayCount, err, cnt)
		}
	}
	top, _ := board.Top(context.Background(), 3)
	if len(top) != 3 || top[0].VideoID != 1 || top[0].Score != 300 || top[2].VideoID != 3 {
		t.Fatalf("rank: %+v", top)
	}
	hist, _ := st.ListHistory(context.Background(), 1, 10)
	if len(hist) == 0 {
		t.Fatal("play_history not written")
	}
	// 榜單 API
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/rank/hot?limit=3", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"plays":300`) {
		t.Fatalf("rank api: %d %s", w.Code, w.Body.String())
	}
}

// 在真的 50 萬支影片的資料庫上量布隆過濾器的實際誤判率（需要先跑過 cmd/seed）。
func TestBloomFalsePositiveRateAtScale(t *testing.T) {
	st, err := store.Open("app:app@tcp(127.0.0.1:13306)/vod?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	defer st.DB.Close()
	ids, err := st.AllVideoIDs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) < 100_000 {
		t.Fatalf("seed database has only %d videos; run cmd/seed first", len(ids))
	}
	b := api.NewBloom(ids, 0.01)
	for _, id := range ids[:1000] { // 沒有假陰性
		if !b.Test(id) {
			t.Fatalf("false negative for %d", id)
		}
	}
	max := ids[0]
	for _, id := range ids {
		if id > max {
			max = id
		}
	}
	fp, probes := 0, 200_000
	for i := 1; i <= probes; i++ {
		if b.Test(max + int64(i)) {
			fp++
		}
	}
	rate := float64(fp) / float64(probes)
	t.Logf("布隆：%d 筆已存在 id、%d 個不存在 id 探測 → 誤判 %d 個（%.4f%%）", len(ids), probes, fp, rate*100)
	if rate > 0.01 {
		t.Fatalf("false positive rate %.4f above 1%%", rate)
	}
}
