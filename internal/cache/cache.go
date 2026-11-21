// Package cache 實作兩層快取（本機記憶體 L1 + Redis L2），並處理經典問題：
//   - 擊穿：singleflight，同一個 key 同時只有一個 loader 在跑
//   - 穿透：loader 回 ErrNotFound 時寫入空值快取（短 TTL）
//   - 雪崩：所有 TTL 加隨機抖動
//   - 熱 key：L1 條目在短 TTL 內被命中超過門檻，就延長它的 L1 存活時間
//   - 降級：軟 TTL 過期但硬 TTL 未過的舊值，在 loader 失敗（含熔斷）時當作 stale 回傳
package cache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// ErrNotFound 由 loader 回傳，代表來源確定沒有這筆資料（會被空值快取）。
var ErrNotFound = errors.New("not found")

// Remote 是 L2（Redis）的最小介面，測試時可以換成計數用的假實作。
type Remote interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	Del(ctx context.Context, keys ...string) error
}

// Loader 從來源（MySQL）載入資料，回傳已序列化的位元組。
type Loader func(ctx context.Context) ([]byte, error)

type Source string

const (
	SourceL1    Source = "l1"
	SourceL2    Source = "l2"
	SourceLoad  Source = "load"
	SourceStale Source = "stale"
	SourceNull  Source = "null"
)

type Options struct {
	L1Size        int           // L1 最多條目數
	L1TTL         time.Duration // 一般 key 在 L1 的存活時間（短，換取跨實例一致性）
	HotL1TTL      time.Duration // 熱 key 在 L1 的存活時間
	HotThreshold  int64         // 一個 L1 條目存活期間被命中幾次算熱
	SoftTTL       time.Duration // 預設新鮮時間
	HardTTLFactor int           // 舊值保留時間 = 軟 TTL × 這個倍數
	NullTTL       time.Duration // 空值快取新鮮時間
	Jitter        float64       // TTL 抖動比例，0.2 = ±20%
	LoadTimeout   time.Duration
	Metrics       *Metrics
	Logger        *slog.Logger
}

func (o *Options) defaults() {
	if o.L1Size == 0 {
		o.L1Size = 100_000
	}
	if o.L1TTL == 0 {
		o.L1TTL = time.Second
	}
	if o.HotL1TTL == 0 {
		o.HotL1TTL = 30 * time.Second
	}
	if o.HotThreshold == 0 {
		o.HotThreshold = 200
	}
	if o.SoftTTL == 0 {
		o.SoftTTL = time.Minute
	}
	if o.HardTTLFactor == 0 {
		o.HardTTLFactor = 10
	}
	if o.NullTTL == 0 {
		o.NullTTL = 30 * time.Second
	}
	if o.LoadTimeout == 0 {
		o.LoadTimeout = 3 * time.Second
	}
	if o.Metrics == nil {
		o.Metrics = NewMetrics(nil)
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// entry 是 L1 條目；hits 與 hot 用 atomic，避免讀取路徑拿寫鎖。
type entry struct {
	val     []byte
	null    bool
	softExp time.Time // 過了就不新鮮（stale）
	l1Exp   atomic.Int64
	hits    atomic.Int64
	hot     atomic.Bool
}

type Cache struct {
	opt    Options
	remote Remote
	l1     *l1
	sf     singleflight.Group
}

func New(remote Remote, opt Options) *Cache {
	opt.defaults()
	return &Cache{opt: opt, remote: remote, l1: newL1(opt.L1Size)}
}

type Result struct {
	Val    []byte
	Source Source
}

// Get 取資料；soft 為 0 時用預設軟 TTL。
func (c *Cache) Get(ctx context.Context, key string, soft time.Duration, load Loader) (Result, error) {
	if soft == 0 {
		soft = c.opt.SoftTTL
	}
	now := time.Now()

	// L1
	var stale *entry
	if e, ok := c.l1.get(key, now); ok {
		if now.Before(e.softExp) {
			c.touch(e, now)
			if e.null {
				c.opt.Metrics.Request(SourceNull)
				return Result{Source: SourceNull}, ErrNotFound
			}
			c.opt.Metrics.Request(SourceL1)
			return Result{Val: e.val, Source: SourceL1}, nil
		}
		stale = e
	}

	// L2
	if raw, ok, err := c.remote.Get(ctx, key); err != nil {
		c.opt.Metrics.RemoteError()
		c.opt.Logger.Warn("cache l2 get failed", "key", key, "err", err)
	} else if ok {
		if e, derr := decode(raw); derr == nil {
			if now.Before(e.softExp) {
				c.putL1(key, e, now, false)
				if e.null {
					c.opt.Metrics.Request(SourceNull)
					return Result{Source: SourceNull}, ErrNotFound
				}
				c.opt.Metrics.Request(SourceL2)
				return Result{Val: e.val, Source: SourceL2}, nil
			}
			stale = e
		}
	}

	// 擊穿保護：同一個 key 只有一個 goroutine 去 loader，其餘等結果。
	ch := c.sf.DoChan(key, func() (any, error) {
		lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.opt.LoadTimeout)
		defer cancel()
		return c.loadAndStore(lctx, key, soft, load, stale)
	})
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return Result{}, r.Err
		}
		res := r.Val.(Result)
		if res.Source == SourceNull {
			return Result{Source: SourceNull}, ErrNotFound
		}
		return res, nil
	}
}

func (c *Cache) loadAndStore(ctx context.Context, key string, soft time.Duration, load Loader, stale *entry) (Result, error) {
	val, err := load(ctx)
	now := time.Now()
	switch {
	case err == nil:
		e := &entry{val: val, softExp: now.Add(c.jitter(soft))}
		c.store(ctx, key, e, soft, now)
		c.opt.Metrics.Request(SourceLoad)
		return Result{Val: val, Source: SourceLoad}, nil
	case errors.Is(err, ErrNotFound):
		e := &entry{null: true, softExp: now.Add(c.jitter(c.opt.NullTTL))}
		c.store(ctx, key, e, c.opt.NullTTL, now)
		c.opt.Metrics.Request(SourceLoad)
		return Result{Source: SourceNull}, nil
	default:
		if stale != nil && !stale.null {
			c.opt.Metrics.Request(SourceStale)
			c.opt.Logger.Warn("serving stale cache", "key", key, "err", err)
			return Result{Val: stale.val, Source: SourceStale}, nil
		}
		return Result{}, err
	}
}

func (c *Cache) store(ctx context.Context, key string, e *entry, soft time.Duration, now time.Time) {
	hard := c.jitter(soft * time.Duration(c.opt.HardTTLFactor))
	c.putL1(key, e, now, false)
	if err := c.remote.Set(ctx, key, encode(e), hard); err != nil {
		c.opt.Metrics.RemoteError()
		c.opt.Logger.Warn("cache l2 set failed", "key", key, "err", err)
	}
}

// putL1 放進 L1：一般 key 只留 L1TTL（但不超過軟 TTL），熱 key 留 HotL1TTL。
func (c *Cache) putL1(key string, e *entry, now time.Time, hot bool) {
	ttl := c.opt.L1TTL
	if hot {
		ttl = c.opt.HotL1TTL
	}
	if d := e.softExp.Sub(now); d < ttl && d > 0 {
		ttl = d
	}
	ne := &entry{val: e.val, null: e.null, softExp: e.softExp}
	ne.l1Exp.Store(now.Add(ttl).UnixNano())
	if hot {
		ne.hot.Store(true)
	}
	c.l1.put(key, ne)
}

// touch 計數；超過門檻就把這個條目升級為熱 key，延長 L1 存活。
func (c *Cache) touch(e *entry, now time.Time) {
	n := e.hits.Add(1)
	if n >= c.opt.HotThreshold && e.hot.CompareAndSwap(false, true) {
		exp := now.Add(c.opt.HotL1TTL)
		if exp.After(e.softExp) {
			exp = e.softExp
		}
		e.l1Exp.Store(exp.UnixNano())
		c.opt.Metrics.HotPromoted()
	}
}

// Invalidate 刪除 L1 與 L2（寫入資料後呼叫）。
func (c *Cache) Invalidate(ctx context.Context, keys ...string) error {
	for _, k := range keys {
		c.l1.del(k)
	}
	return c.remote.Del(ctx, keys...)
}

// jitter 把 TTL 乘上 [1-j, 1+j] 的隨機倍率，避免大量 key 同時過期。
func (c *Cache) jitter(d time.Duration) time.Duration {
	j := c.opt.Jitter
	if j <= 0 {
		return d
	}
	return time.Duration(float64(d) * (1 - j + rand.Float64()*2*j))
}

// 編碼：8 bytes softExp(UnixNano) + 1 byte flags(1=null) + payload
func encode(e *entry) []byte {
	b := make([]byte, 9+len(e.val))
	binary.BigEndian.PutUint64(b, uint64(e.softExp.UnixNano()))
	if e.null {
		b[8] = 1
	}
	copy(b[9:], e.val)
	return b
}

func decode(b []byte) (*entry, error) {
	if len(b) < 9 {
		return nil, fmt.Errorf("cache entry too short: %d", len(b))
	}
	e := &entry{softExp: time.Unix(0, int64(binary.BigEndian.Uint64(b))), null: b[8] == 1}
	if !e.null {
		e.val = b[9:]
	}
	return e, nil
}

// ---- L1 ----

type l1 struct {
	mu  sync.RWMutex
	m   map[string]*entry
	cap int
}

func newL1(capacity int) *l1 { return &l1{m: make(map[string]*entry, 1024), cap: capacity} }

func (l *l1) get(key string, now time.Time) (*entry, bool) {
	l.mu.RLock()
	e, ok := l.m[key]
	l.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if now.UnixNano() >= e.l1Exp.Load() {
		return nil, false
	}
	return e, true
}

func (l *l1) put(key string, e *entry) {
	l.mu.Lock()
	if len(l.m) >= l.cap {
		// 滿了就隨機丟掉一個（Go map 的遍歷順序是隨機的），夠簡單，不用維護 LRU 串列。
		for k := range l.m {
			delete(l.m, k)
			break
		}
	}
	l.m[key] = e
	l.mu.Unlock()
}

func (l *l1) del(key string) {
	l.mu.Lock()
	delete(l.m, key)
	l.mu.Unlock()
}
