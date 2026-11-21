package resilience

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

// IPLimiter 是每個 IP 一個本機 token bucket。熱路徑上不打 Redis，
// 代價是多實例時每個實例各自計算（限額是「每實例每 IP」）。
type IPLimiter struct {
	rate   rate.Limit
	burst  int
	shards [64]ipShard
}

type ipShard struct {
	mu sync.Mutex
	m  map[string]*ipEntry
}

type ipEntry struct {
	lim  *rate.Limiter
	last time.Time
}

func NewIPLimiter(perSec float64, burst int) *IPLimiter {
	l := &IPLimiter{rate: rate.Limit(perSec), burst: burst}
	for i := range l.shards {
		l.shards[i].m = map[string]*ipEntry{}
	}
	return l
}

func fnv(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h = (h ^ uint32(s[i])) * 16777619
	}
	return h
}

func (l *IPLimiter) Allow(ip string) bool {
	sh := &l.shards[fnv(ip)%uint32(len(l.shards))]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.m[ip]
	if !ok {
		e = &ipEntry{lim: rate.NewLimiter(l.rate, l.burst)}
		sh.m[ip] = e
	}
	e.last = now
	sh.mu.Unlock()
	return e.lim.AllowN(now, 1)
}

// Sweep 清掉閒置超過 idle 的 IP，避免 map 無限長大。
func (l *IPLimiter) Sweep(idle time.Duration) {
	cut := time.Now().Add(-idle)
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for k, e := range sh.m {
			if e.last.Before(cut) {
				delete(sh.m, k)
			}
		}
		sh.mu.Unlock()
	}
}

func (l *IPLimiter) Run(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.Sweep(5 * time.Minute)
		}
	}
}

// UserLimiter 是跨實例共用的每使用者 token bucket，狀態放 Redis，用 Lua 保證原子。
// Redis 出錯時退回本機 bucket（fail-open 到本機，不是全放行）。
type UserLimiter struct {
	rdb      redis.UniversalClient
	perSec   float64
	burst    int
	fallback *IPLimiter
}

var userScript = redis.NewScript(`
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local rate, burst, now = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local tokens = tonumber(d[1]) or burst
local ts = tonumber(d[2]) or now
tokens = math.min(burst, tokens + math.max(0, now - ts) / 1000 * rate)
local ok = 0
if tokens >= 1 then tokens = tokens - 1; ok = 1 end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) * 2 + 1000)
return ok
`)

func NewUserLimiter(rdb redis.UniversalClient, perSec float64, burst int) *UserLimiter {
	return &UserLimiter{rdb: rdb, perSec: perSec, burst: burst, fallback: NewIPLimiter(perSec, burst)}
}

func (u *UserLimiter) Allow(ctx context.Context, userID string) bool {
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	n, err := userScript.Run(ctx, u.rdb, []string{"rl:user:" + userID}, u.perSec, u.burst, time.Now().UnixMilli()).Int()
	if err != nil {
		return u.fallback.Allow(userID)
	}
	return n == 1
}
