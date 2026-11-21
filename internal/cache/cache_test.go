package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memRemote 是 Remote 的假實作：記錄呼叫次數與寫入時的 TTL。
type memRemote struct {
	mu     sync.Mutex
	m      map[string][]byte
	ttls   []time.Duration
	gets   atomic.Int64
	sets   atomic.Int64
	getErr error
}

func newMemRemote() *memRemote { return &memRemote{m: map[string][]byte{}} }

func (r *memRemote) Get(_ context.Context, k string) ([]byte, bool, error) {
	r.gets.Add(1)
	if r.getErr != nil {
		return nil, false, r.getErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.m[k]
	return b, ok, nil
}

func (r *memRemote) Set(_ context.Context, k string, v []byte, ttl time.Duration) error {
	r.sets.Add(1)
	r.mu.Lock()
	r.m[k] = v
	r.ttls = append(r.ttls, ttl)
	r.mu.Unlock()
	return nil
}

func (r *memRemote) Del(_ context.Context, ks ...string) error {
	r.mu.Lock()
	for _, k := range ks {
		delete(r.m, k)
	}
	r.mu.Unlock()
	return nil
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// 擊穿：1000 個 goroutine 同時打一個冷 key，loader 只能被呼叫 1 次。
func TestBreakdown_SingleflightCollapsesConcurrentColdKey(t *testing.T) {
	c := New(newMemRemote(), Options{Logger: quiet()})
	var loads atomic.Int64
	gate := make(chan struct{})
	load := func(ctx context.Context) ([]byte, error) {
		loads.Add(1)
		time.Sleep(100 * time.Millisecond) // 模擬 DB 查詢時間，讓其他 goroutine 都在等
		return []byte("v"), nil
	}
	const n = 1000
	var wg sync.WaitGroup
	var bad atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			res, err := c.Get(context.Background(), "video:1", 0, load)
			if err != nil || string(res.Val) != "v" {
				bad.Add(1)
			}
		}()
	}
	close(gate)
	wg.Wait()
	if got := loads.Load(); got != 1 {
		t.Fatalf("loader called %d times, want 1", got)
	}
	if bad.Load() != 0 {
		t.Fatalf("%d callers got a wrong result", bad.Load())
	}
}

// 穿透：不存在的資料只回源 1 次，之後由空值快取擋掉。
func TestPenetration_NullCacheStopsRepeatedMisses(t *testing.T) {
	c := New(newMemRemote(), Options{Logger: quiet()})
	var loads atomic.Int64
	load := func(ctx context.Context) ([]byte, error) { loads.Add(1); return nil, ErrNotFound }
	for i := 0; i < 1000; i++ {
		if _, err := c.Get(context.Background(), "video:999", 0, load); !errors.Is(err, ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("loader called %d times for a missing key, want 1", got)
	}
}

// 空值快取過期後，資料新增了要能被看見。
func TestPenetration_NullEntryExpires(t *testing.T) {
	c := New(newMemRemote(), Options{Logger: quiet(), NullTTL: 50 * time.Millisecond, L1TTL: 20 * time.Millisecond})
	exists := atomic.Bool{}
	load := func(ctx context.Context) ([]byte, error) {
		if exists.Load() {
			return []byte("now"), nil
		}
		return nil, ErrNotFound
	}
	if _, err := c.Get(context.Background(), "k", 0, load); !errors.Is(err, ErrNotFound) {
		t.Fatal("want not found")
	}
	exists.Store(true)
	time.Sleep(120 * time.Millisecond)
	res, err := c.Get(context.Background(), "k", 0, load)
	if err != nil || string(res.Val) != "now" {
		t.Fatalf("got %v %v", res, err)
	}
}

// 雪崩：1 萬個 key 寫入 Redis 的 TTL 必須落在 ±20% 內，且不是全部相同。
func TestAvalanche_TTLJitterSpreadsExpiry(t *testing.T) {
	r := newMemRemote()
	base := time.Minute
	c := New(r, Options{Logger: quiet(), SoftTTL: base, HardTTLFactor: 10, Jitter: 0.2})
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("video:%d", i)
		if _, err := c.Get(context.Background(), k, 0, func(context.Context) ([]byte, error) { return []byte("x"), nil }); err != nil {
			t.Fatal(err)
		}
	}
	hard := base * 10
	lo, hi := time.Duration(float64(hard)*0.8), time.Duration(float64(hard)*1.2)
	distinct := map[time.Duration]struct{}{}
	min, max := r.ttls[0], r.ttls[0]
	for _, d := range r.ttls {
		if d < lo || d > hi {
			t.Fatalf("ttl %v outside [%v,%v]", d, lo, hi)
		}
		distinct[d] = struct{}{}
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
	}
	if len(distinct) < 9000 {
		t.Fatalf("only %d distinct TTLs out of %d; jitter too weak", len(distinct), len(r.ttls))
	}
	t.Logf("TTL 範圍 %v ~ %v，%d 個 key 有 %d 種不同 TTL", min, max, len(r.ttls), len(distinct))
}

// 熱 key：L1 條目短時間內被命中超過門檻後，L1 存活變長；之後不再打 Redis。冷 key 則照常回 Redis。
func TestHotKey_PromotionKeepsHotKeyInL1(t *testing.T) {
	r := newMemRemote()
	c := New(r, Options{Logger: quiet(), L1TTL: 60 * time.Millisecond, HotL1TTL: 5 * time.Second, HotThreshold: 50})
	load := func(context.Context) ([]byte, error) { return []byte("x"), nil }
	ctx := context.Background()
	for i := 0; i < 200; i++ { // 熱 key
		c.Get(ctx, "hot", 0, load)
	}
	for i := 0; i < 10; i++ { // 冷 key
		c.Get(ctx, "cold", 0, load)
	}
	time.Sleep(120 * time.Millisecond) // 超過一般 L1 TTL
	before := r.gets.Load()
	for i := 0; i < 100; i++ {
		c.Get(ctx, "hot", 0, load)
	}
	if d := r.gets.Load() - before; d != 0 {
		t.Fatalf("hot key hit Redis %d times after L1TTL; want 0", d)
	}
	before = r.gets.Load()
	c.Get(ctx, "cold", 0, load)
	if d := r.gets.Load() - before; d != 1 {
		t.Fatalf("cold key should go back to Redis once, got %d", d)
	}
}

// 降級：軟 TTL 過期後 loader 壞掉，回舊值而不是錯誤；沒有舊值才回錯誤。
func TestDegrade_ServeStaleWhenLoaderFails(t *testing.T) {
	r := newMemRemote()
	c := New(r, Options{Logger: quiet(), SoftTTL: 50 * time.Millisecond, L1TTL: 10 * time.Millisecond, HardTTLFactor: 100})
	ctx := context.Background()
	if _, err := c.Get(ctx, "k", 0, func(context.Context) ([]byte, error) { return []byte("old"), nil }); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond) // 軟 TTL（含抖動前的 50ms）過期
	boom := errors.New("db down")
	res, err := c.Get(ctx, "k", 0, func(context.Context) ([]byte, error) { return nil, boom })
	if err != nil || string(res.Val) != "old" || res.Source != SourceStale {
		t.Fatalf("want stale old, got %+v err=%v", res, err)
	}
	if _, err := c.Get(ctx, "never-cached", 0, func(context.Context) ([]byte, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("want loader error, got %v", err)
	}
}

// Redis 掛掉時 L1 照常服務，loader 仍可回源，請求不會失敗。
func TestRemoteDown_FallsBackToLoader(t *testing.T) {
	r := newMemRemote()
	r.getErr = errors.New("redis down")
	c := New(r, Options{Logger: quiet()})
	res, err := c.Get(context.Background(), "k", 0, func(context.Context) ([]byte, error) { return []byte("v"), nil })
	if err != nil || string(res.Val) != "v" {
		t.Fatalf("got %+v %v", res, err)
	}
}

func TestInvalidate(t *testing.T) {
	r := newMemRemote()
	c := New(r, Options{Logger: quiet()})
	var n atomic.Int64
	load := func(context.Context) ([]byte, error) { return []byte(fmt.Sprint(n.Add(1))), nil }
	c.Get(context.Background(), "k", 0, load)
	c.Invalidate(context.Background(), "k")
	res, _ := c.Get(context.Background(), "k", 0, load)
	if string(res.Val) != "2" {
		t.Fatalf("want reload after invalidate, got %s", res.Val)
	}
}
