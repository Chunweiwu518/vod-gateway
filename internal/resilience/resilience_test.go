package resilience

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Chunweiwu518/vod-gateway/internal/cache"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestBreaker_OpensAfterFailuresAndRecovers(t *testing.T) {
	b := NewBreaker(BreakerConfig{Name: "t", MinRequests: 5, FailRatio: 0.5, OpenFor: 100 * time.Millisecond, Logger: quiet()})
	boom := errors.New("db down")
	calls := 0
	fail := func() ([]byte, error) { calls++; return nil, boom }
	for i := 0; i < 5; i++ {
		b.Do(fail)
	}
	if b.State() != "open" {
		t.Fatalf("state=%s want open", b.State())
	}
	before := calls
	if _, err := b.Do(fail); !errors.Is(err, ErrOpen) {
		t.Fatalf("want ErrOpen, got %v", err)
	}
	if calls != before {
		t.Fatal("open breaker must not call downstream")
	}
	time.Sleep(150 * time.Millisecond) // 進半開，成功的探測請求讓它關回去
	for i := 0; i < 3; i++ {
		if _, err := b.Do(func() ([]byte, error) { return []byte("ok"), nil }); err != nil {
			t.Fatalf("half-open probe %d failed: %v", i, err)
		}
	}
	if b.State() != "closed" {
		t.Fatalf("state=%s want closed", b.State())
	}
}

func TestBreaker_NotFoundIsNotAFailure(t *testing.T) {
	b := NewBreaker(BreakerConfig{Name: "t2", MinRequests: 5, Logger: quiet()})
	for i := 0; i < 50; i++ {
		b.Do(func() ([]byte, error) { return nil, cache.ErrNotFound })
	}
	if b.State() != "closed" {
		t.Fatalf("404s must not trip the breaker, state=%s", b.State())
	}
}

func TestIPLimiter_BurstThenReject_PerIPIsolation(t *testing.T) {
	l := NewIPLimiter(1, 5)
	ok := 0
	for i := 0; i < 20; i++ {
		if l.Allow("1.1.1.1") {
			ok++
		}
	}
	if ok != 5 {
		t.Fatalf("allowed %d of 20 in a burst, want exactly burst=5", ok)
	}
	if !l.Allow("2.2.2.2") {
		t.Fatal("another IP must have its own bucket")
	}
}

func TestUserLimiter_RedisBucket(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	l := NewUserLimiter(rdb, 1, 3)
	ctx := context.Background()
	ok := 0
	for i := 0; i < 10; i++ {
		if l.Allow(ctx, "42") {
			ok++
		}
	}
	if ok != 3 {
		t.Fatalf("allowed %d, want burst 3", ok)
	}
	if !l.Allow(ctx, "43") {
		t.Fatal("other user must not be affected")
	}
}

func TestUserLimiter_RedisDownFallsBackToLocalBucket(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond})
	l := NewUserLimiter(rdb, 1, 2)
	ok := 0
	for i := 0; i < 6; i++ {
		if l.Allow(context.Background(), "u") {
			ok++
		}
	}
	if ok != 2 {
		t.Fatalf("fallback allowed %d, want 2 (local bucket, not fail-open-all)", ok)
	}
}
