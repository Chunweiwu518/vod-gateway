package api

import (
	"context"
	"errors"
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

	"github.com/Chunweiwu518/vod-gateway/internal/cache"
	"github.com/Chunweiwu518/vod-gateway/internal/events"
	"github.com/Chunweiwu518/vod-gateway/internal/rank"
	"github.com/Chunweiwu518/vod-gateway/internal/resilience"
	"github.com/Chunweiwu518/vod-gateway/internal/store"
	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// fakeStore 取代 MySQL，精確計算每種查詢被呼叫幾次。
type fakeStore struct {
	videos    map[int64]store.Video
	delay     time.Duration
	fail      atomic.Bool
	getVideo  atomic.Int64
	searchV   atomic.Int64
	searchC   atomic.Int64
	published atomic.Int64
	mu        sync.Mutex
	favorites map[[2]int64]bool
}

func (f *fakeStore) GetVideo(ctx context.Context, id int64) (store.Video, error) {
	f.getVideo.Add(1)
	time.Sleep(f.delay)
	if f.fail.Load() {
		return store.Video{}, errors.New("mysql down")
	}
	v, ok := f.videos[id]
	if !ok {
		return v, cache.ErrNotFound
	}
	return v, nil
}
func (f *fakeStore) GetCreator(context.Context, int64) (store.Creator, error) {
	return store.Creator{}, cache.ErrNotFound
}
func (f *fakeStore) ListByCreator(context.Context, int64, int) ([]store.Video, error) {
	return nil, nil
}
func (f *fakeStore) SearchVideos(_ context.Context, q string, _ int) ([]store.Video, error) {
	f.searchV.Add(1)
	return []store.Video{{ID: 1, Title: q + " video"}}, nil
}
func (f *fakeStore) SearchCreators(_ context.Context, q string, _ int) ([]store.Creator, error) {
	f.searchC.Add(1)
	return []store.Creator{{ID: 9, Name: q + " creator"}}, nil
}
func (f *fakeStore) ListHistory(context.Context, int64, int) ([]store.HistoryItem, error) {
	return []store.HistoryItem{}, nil
}
func (f *fakeStore) AddFavorite(_ context.Context, u, v int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.favorites[[2]int64{u, v}] {
		return false, nil
	}
	f.favorites[[2]int64{u, v}] = true
	return true, nil
}
func (f *fakeStore) RemoveFavorite(context.Context, int64, int64) (bool, error) { return true, nil }
func (f *fakeStore) ListFavorites(context.Context, int64, int) ([]store.Video, error) {
	return []store.Video{}, nil
}
func (f *fakeStore) CreateVideo(context.Context, store.Video) (int64, error) { return 0, nil }

type fakePub struct{ n atomic.Int64 }

func (p *fakePub) Publish(context.Context, events.Event) error { p.n.Add(1); return nil }

type harness struct {
	h     http.Handler
	st    *fakeStore
	pub   *fakePub
	br    *resilience.Breaker
	bloom *Bloom
	rdb   *redis.Client
}

type opts struct {
	soft      time.Duration
	ipRate    float64
	ipBurst   int
	userBurst int
	bloomIDs  []int64
}

func newHarness(t *testing.T, o opts) *harness {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := &fakeStore{videos: map[int64]store.Video{}, favorites: map[[2]int64]bool{}}
	var ids []int64
	for i := int64(1); i <= 1000; i++ {
		st.videos[i] = store.Video{ID: i, Title: "t"}
		ids = append(ids, i)
	}
	if o.bloomIDs != nil {
		ids = o.bloomIDs
	}
	if o.ipRate == 0 {
		o.ipRate, o.ipBurst = 1e9, 1_000_000
	}
	if o.userBurst == 0 {
		o.userBurst = 1000
	}
	reg := prometheus.NewRegistry()
	c := cache.New(cache.RedisRemote{C: rdb}, cache.Options{Logger: quiet, SoftTTL: o.soft, L1TTL: 10 * time.Millisecond, HardTTLFactor: 100})
	br := resilience.NewBreaker(resilience.BreakerConfig{Name: "t", MinRequests: 5, OpenFor: time.Minute, Logger: quiet})
	bl := NewBloom(ids, 0.01)
	pub := &fakePub{}
	_, h := New(Config{IPRate: o.ipRate, IPBurst: o.ipBurst}, Deps{
		Store: st, Cache: c, Breaker: br, Bloom: bl, Board: rank.New(rdb, "rank:t"), Publisher: pub,
		UserLim: resilience.NewUserLimiter(rdb, 1, o.userBurst), Registry: reg,
	})
	return &harness{h: h, st: st, pub: pub, br: br, bloom: bl, rdb: rdb}
}

func (x *harness) do(method, path string, hdr map[string]string, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.RemoteAddr = "10.0.0.1:1234"
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	x.h.ServeHTTP(w, r)
	return w
}

// 擊穿：1000 個併發請求打同一個冷 key，後端只被查一次，而且每個人都拿到 200。
func TestAPI_ColdKeyWith1000ConcurrentRequests_OneStoreQuery(t *testing.T) {
	x := newHarness(t, opts{})
	x.st.delay = 100 * time.Millisecond
	gate := make(chan struct{})
	var wg sync.WaitGroup
	var non200 atomic.Int64
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			if w := x.do("GET", "/api/videos/1", nil, ""); w.Code != 200 {
				non200.Add(1)
			}
		}()
	}
	close(gate)
	wg.Wait()
	if got := x.st.getVideo.Load(); got != 1 {
		t.Fatalf("store queried %d times, want 1", got)
	}
	if non200.Load() != 0 {
		t.Fatalf("%d requests were not 200", non200.Load())
	}
}

// 穿透（布隆）：1000 個不存在的 id，幾乎都被布隆過濾器擋掉，store 只會被誤判的少數幾個打到。
func TestAPI_BloomFilterBlocksNonexistentIDs(t *testing.T) {
	x := newHarness(t, opts{})
	for i := int64(0); i < 1000; i++ {
		w := x.do("GET", "/api/videos/"+itoa(5_000_000+i), nil, "")
		if w.Code != 404 {
			t.Fatalf("id %d: got %d want 404", 5_000_000+i, w.Code)
		}
	}
	got := x.st.getVideo.Load()
	t.Logf("1000 個不存在的 id → store 被查 %d 次（布隆誤判）", got)
	if got > 30 { // 設計誤判率 1%，期望約 10；30 已是 3 倍
		t.Fatalf("bloom filter let %d of 1000 through", got)
	}
}

// 穿透（空值快取）：布隆說有、但資料庫沒有（例如被刪除），1000 次請求只回源 1 次。
func TestAPI_NullCacheForBloomFalsePositive(t *testing.T) {
	x := newHarness(t, opts{bloomIDs: []int64{1, 2, 3, 7777}}) // 7777 在布隆裡但不在 store
	for i := 0; i < 1000; i++ {
		if w := x.do("GET", "/api/videos/7777", nil, ""); w.Code != 404 {
			t.Fatalf("got %d want 404", w.Code)
		}
	}
	if got := x.st.getVideo.Load(); got != 1 {
		t.Fatalf("store queried %d times for a deleted video, want 1", got)
	}
}

// 降級：資料庫壞掉，已快取過的 key 回舊值（X-Degraded: stale），熔斷開啟後完全不再打 DB；沒快取過的回 503。
func TestAPI_StoreDown_ServesStaleAndBreakerOpens(t *testing.T) {
	x := newHarness(t, opts{soft: 80 * time.Millisecond})
	if w := x.do("GET", "/api/videos/1", nil, ""); w.Code != 200 {
		t.Fatalf("warmup: %d", w.Code)
	}
	time.Sleep(200 * time.Millisecond) // 軟 TTL 過期
	x.st.fail.Store(true)

	for i := 0; i < 20; i++ {
		w := x.do("GET", "/api/videos/1", nil, "")
		if w.Code != 200 || w.Header().Get("X-Degraded") != "stale" {
			t.Fatalf("req %d: code=%d degraded=%q, want 200 stale", i, w.Code, w.Header().Get("X-Degraded"))
		}
		time.Sleep(15 * time.Millisecond) // 讓 L1 失效，每次都走到回源判斷
	}
	if x.br.State() != "open" {
		t.Fatalf("breaker state=%s, want open", x.br.State())
	}
	before := x.st.getVideo.Load()
	for i := 0; i < 10; i++ {
		x.do("GET", "/api/videos/1", nil, "")
		time.Sleep(15 * time.Millisecond)
	}
	if x.st.getVideo.Load() != before {
		t.Fatal("breaker is open but the store was still queried")
	}
	if w := x.do("GET", "/api/videos/2", nil, ""); w.Code != 503 {
		t.Fatalf("uncached key during outage: got %d want 503", w.Code)
	}
}

func TestAPI_IPRateLimit429(t *testing.T) {
	x := newHarness(t, opts{ipRate: 1, ipBurst: 5})
	ok, limited := 0, 0
	for i := 0; i < 20; i++ {
		switch w := x.do("GET", "/api/videos/1", nil, ""); w.Code {
		case 200:
			ok++
		case 429:
			limited++
			if w.Header().Get("Retry-After") == "" {
				t.Fatal("429 without Retry-After")
			}
		}
	}
	if ok != 5 || limited != 15 {
		t.Fatalf("ok=%d limited=%d, want 5/15", ok, limited)
	}
	if w := x.do("GET", "/healthz", nil, ""); w.Code != 200 {
		t.Fatal("healthz must not be rate limited")
	}
	r := httptest.NewRequest("GET", "/api/videos/1", nil)
	r.RemoteAddr = "10.9.9.9:1"
	w := httptest.NewRecorder()
	x.h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal("a different IP must not be limited")
	}
}

func TestAPI_PlayEvent_UserRateLimitValidationAndBloom(t *testing.T) {
	x := newHarness(t, opts{userBurst: 3})
	body := `{"video_id":1,"position_sec":12}`
	if w := x.do("POST", "/api/play-events", nil, body); w.Code != 401 {
		t.Fatalf("no user header: %d", w.Code)
	}
	h := map[string]string{"X-User-Id": "7"}
	codes := map[int]int{}
	for i := 0; i < 8; i++ {
		codes[x.do("POST", "/api/play-events", h, body).Code]++
	}
	if codes[202] != 3 || codes[429] != 5 {
		t.Fatalf("codes=%v, want 3x202 and 5x429", codes)
	}
	if x.pub.n.Load() != 3 {
		t.Fatalf("published %d events, want 3 (rate-limited ones must not enqueue)", x.pub.n.Load())
	}
	// 別的使用者不受影響；不存在的影片被布隆擋掉
	if w := x.do("POST", "/api/play-events", map[string]string{"X-User-Id": "8"}, `{"video_id":99999999,"position_sec":1}`); w.Code != 404 {
		t.Fatalf("unknown video: %d", w.Code)
	}
	if w := x.do("POST", "/api/play-events", map[string]string{"X-User-Id": "8"}, `{"video_id":0}`); w.Code != 400 {
		t.Fatalf("bad body: %d", w.Code)
	}
}

func TestAPI_SearchAggregatesTwoSourcesAndCaches(t *testing.T) {
	x := newHarness(t, opts{})
	if w := x.do("GET", "/api/search?q=a", nil, ""); w.Code != 400 {
		t.Fatalf("short query: %d", w.Code)
	}
	for i := 0; i < 5; i++ {
		w := x.do("GET", "/api/search?q=travel", nil, "")
		if w.Code != 200 || !strings.Contains(w.Body.String(), "travel video") || !strings.Contains(w.Body.String(), "travel creator") {
			t.Fatalf("body=%s", w.Body.String())
		}
	}
	if x.st.searchV.Load() != 1 || x.st.searchC.Load() != 1 {
		t.Fatalf("each source should be queried once, got videos=%d creators=%d", x.st.searchV.Load(), x.st.searchC.Load())
	}
}

func TestAPI_FavoriteIsIdempotent(t *testing.T) {
	x := newHarness(t, opts{})
	h := map[string]string{"X-User-Id": "5"}
	if w := x.do("PUT", "/api/me/favorites/1", h, ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"changed":true`) {
		t.Fatalf("first add: %d %s", w.Code, w.Body.String())
	}
	if w := x.do("PUT", "/api/me/favorites/1", h, ""); !strings.Contains(w.Body.String(), `"changed":false`) {
		t.Fatalf("second add must be a no-op: %s", w.Body.String())
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
