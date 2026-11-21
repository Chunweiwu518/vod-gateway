// Package events 是播放事件的非同步管線：API 只 XADD 到 Redis Stream 就回 202，
// 背景 consumer 用 consumer group 批次讀取、去重、聚合，再更新排行榜與 MySQL。
//
// 投遞語意是 at-least-once。處理順序是「寫 MySQL → 寫排行榜 → 設 dedup key → XACK」，
// 任何一步失敗整批不 ACK，之後由 XAUTOCLAIM 重送；dedup key 擋掉已完成的重送。
// 窗口：寫完 MySQL／排行榜、但還沒設 dedup key 前當機，那一批會被重複計數一次。
// MySQL 與 Redis 之間沒有分散式交易，這個窗口只能縮小，沒辦法消除。
package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/Chunweiwu518/vod-gateway/internal/rank"
	"github.com/Chunweiwu518/vod-gateway/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const (
	Stream = "play_events"
	Group  = "agg"
)

type Event struct {
	ID          string
	UserID      int64
	VideoID     int64
	PositionSec int
}

type Producer struct {
	rdb    redis.UniversalClient
	maxLen int64
}

func NewProducer(rdb redis.UniversalClient) *Producer { return &Producer{rdb: rdb, maxLen: 1_000_000} }

func (p *Producer) Publish(ctx context.Context, e Event) error {
	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: Stream, MaxLen: p.maxLen, Approx: true,
		Values: map[string]any{"id": e.ID, "uid": e.UserID, "vid": e.VideoID, "pos": e.PositionSec},
	}).Err()
}

// Sink 是 consumer 的寫入端；store.Store 實作它，測試用假的。
type Sink interface {
	ApplyPlays(ctx context.Context, counts map[int64]int64, history []store.PlayEvent) error
}

type Scorer interface {
	Incr(ctx context.Context, deltas map[int64]float64) error
}

type Consumer struct {
	rdb      redis.UniversalClient
	name     string
	sink     Sink
	board    Scorer
	log      *slog.Logger
	batch    int64
	block    time.Duration
	claimAge time.Duration

	processed, duplicates, failures prometheus.Counter
	pending                         prometheus.Gauge
}

type ConsumerConfig struct {
	Name     string
	Batch    int64
	Block    time.Duration
	ClaimAge time.Duration // 超過這個時間沒 ACK 的訊息會被重新認領
	Reg      prometheus.Registerer
	Logger   *slog.Logger
}

func NewConsumer(rdb redis.UniversalClient, sink Sink, board Scorer, c ConsumerConfig) *Consumer {
	if c.Batch == 0 {
		c.Batch = 500
	}
	if c.Block == 0 {
		c.Block = time.Second
	}
	if c.ClaimAge == 0 {
		c.ClaimAge = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	cn := &Consumer{rdb: rdb, name: c.Name, sink: sink, board: board, log: c.Logger, batch: c.Batch, block: c.Block, claimAge: c.ClaimAge,
		processed:  prometheus.NewCounter(prometheus.CounterOpts{Name: "vod_events_processed_total", Help: "已聚合的播放事件數"}),
		duplicates: prometheus.NewCounter(prometheus.CounterOpts{Name: "vod_events_duplicates_total", Help: "被去重丟掉的重複事件數"}),
		failures:   prometheus.NewCounter(prometheus.CounterOpts{Name: "vod_events_batch_failures_total", Help: "批次處理失敗次數（會重試）"}),
		pending:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "vod_events_pending", Help: "Stream 中已讀取但尚未 ACK 的訊息數"}),
	}
	if c.Reg != nil {
		c.Reg.MustRegister(cn.processed, cn.duplicates, cn.failures, cn.pending)
	}
	return cn
}

// EnsureGroup 建立 consumer group（已存在就略過）。
func (c *Consumer) EnsureGroup(ctx context.Context) error {
	err := c.rdb.XGroupCreateMkStream(ctx, Stream, Group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// Run：定期認領逾時未 ACK 的訊息（前一個 consumer 掛掉留下的），再讀新訊息。
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.EnsureGroup(ctx); err != nil {
		return fmt.Errorf("create group: %w", err)
	}
	var lastClaim time.Time
	for ctx.Err() == nil {
		if time.Since(lastClaim) > c.claimAge {
			lastClaim = time.Now()
			if _, err := c.ClaimStale(ctx); err != nil && ctx.Err() == nil {
				c.failures.Inc()
				c.log.Warn("xautoclaim failed", "err", err)
			}
		}
		if _, err := c.ReadOnce(ctx); err != nil && ctx.Err() == nil {
			c.failures.Inc()
			c.log.Warn("consume batch failed; will retry", "err", err)
			time.Sleep(500 * time.Millisecond)
		}
	}
	return ctx.Err()
}

// ReadOnce 讀一批新訊息並處理，回傳處理的訊息數。
func (c *Consumer) ReadOnce(ctx context.Context) (int, error) {
	res, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: Group, Consumer: c.name, Streams: []string{Stream, ">"}, Count: c.batch, Block: c.block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n := 0
	for _, s := range res {
		if err := c.process(ctx, s.Messages); err != nil {
			return n, err
		}
		n += len(s.Messages)
	}
	return n, nil
}

// ClaimStale 認領並處理閒置超過 claimAge 的未 ACK 訊息。
func (c *Consumer) ClaimStale(ctx context.Context) (int, error) {
	total := 0
	start := "0-0"
	for {
		msgs, next, err := c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: Stream, Group: Group, Consumer: c.name, MinIdle: c.claimAge, Start: start, Count: c.batch,
		}).Result()
		if err != nil {
			return total, err
		}
		if len(msgs) > 0 {
			if err := c.process(ctx, msgs); err != nil {
				return total, err
			}
			total += len(msgs)
		}
		if next == "0-0" || len(msgs) == 0 {
			return total, nil
		}
		start = next
	}
}

func (c *Consumer) process(ctx context.Context, msgs []redis.XMessage) error {
	ids := make([]string, 0, len(msgs))
	events := make([]Event, 0, len(msgs))
	for _, m := range msgs {
		ids = append(ids, m.ID)
		e, ok := parse(m)
		if !ok {
			c.log.Warn("drop malformed event", "msg_id", m.ID)
			continue
		}
		events = append(events, e)
	}
	// 去重：同一個事件 id 只算一次。
	p := c.rdb.Pipeline()
	seen := make([]*redis.IntCmd, len(events))
	for i, e := range events {
		seen[i] = p.Exists(ctx, "dedup:"+e.ID)
	}
	if _, err := p.Exec(ctx); err != nil {
		return err
	}
	counts := map[int64]int64{}
	deltas := map[int64]float64{}
	var history []store.PlayEvent
	var fresh []Event
	inBatch := map[string]bool{}
	for i, e := range events {
		if seen[i].Val() > 0 || inBatch[e.ID] {
			c.duplicates.Inc()
			continue
		}
		inBatch[e.ID] = true
		fresh = append(fresh, e)
		counts[e.VideoID]++
		deltas[e.VideoID]++
		history = append(history, store.PlayEvent{UserID: e.UserID, VideoID: e.VideoID, PositionSec: e.PositionSec})
	}
	if len(fresh) > 0 {
		if err := c.sink.ApplyPlays(ctx, counts, lastPerUserVideo(history)); err != nil {
			return fmt.Errorf("apply plays: %w", err)
		}
		if err := c.board.Incr(ctx, deltas); err != nil {
			return fmt.Errorf("rank incr: %w", err)
		}
		dp := c.rdb.Pipeline()
		for _, e := range fresh {
			dp.Set(ctx, "dedup:"+e.ID, 1, time.Hour)
		}
		if _, err := dp.Exec(ctx); err != nil {
			return err
		}
		c.processed.Add(float64(len(fresh)))
	}
	if len(ids) > 0 {
		if err := c.rdb.XAck(ctx, Stream, Group, ids...).Err(); err != nil {
			return err
		}
	}
	if pend, err := c.rdb.XPending(ctx, Stream, Group).Result(); err == nil {
		c.pending.Set(float64(pend.Count))
	}
	return nil
}

// lastPerUserVideo：同一批裡同一個 (user, video) 只留最後一筆，
// 避免單一 INSERT 對同一個主鍵重複 upsert。
func lastPerUserVideo(in []store.PlayEvent) []store.PlayEvent {
	type k struct{ u, v int64 }
	idx := map[k]int{}
	out := make([]store.PlayEvent, 0, len(in))
	for _, e := range in {
		key := k{e.UserID, e.VideoID}
		if i, ok := idx[key]; ok {
			out[i] = e
			continue
		}
		idx[key] = len(out)
		out = append(out, e)
	}
	return out
}

func parse(m redis.XMessage) (Event, bool) {
	get := func(k string) string { s, _ := m.Values[k].(string); return s }
	uid, e1 := strconv.ParseInt(get("uid"), 10, 64)
	vid, e2 := strconv.ParseInt(get("vid"), 10, 64)
	pos, _ := strconv.Atoi(get("pos"))
	id := get("id")
	if e1 != nil || e2 != nil || id == "" {
		return Event{}, false
	}
	return Event{ID: id, UserID: uid, VideoID: vid, PositionSec: pos}, true
}

var _ Scorer = (*rank.Board)(nil)
var _ Sink = (*store.Store)(nil)
