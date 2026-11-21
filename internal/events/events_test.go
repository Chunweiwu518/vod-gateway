package events

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Chunweiwu518/vod-gateway/internal/rank"
	"github.com/Chunweiwu518/vod-gateway/internal/store"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeSink struct {
	mu      sync.Mutex
	counts  map[int64]int64
	history map[[2]int64]int
	failN   int // 前 N 次呼叫回錯誤
}

func (f *fakeSink) ApplyPlays(_ context.Context, c map[int64]int64, h []store.PlayEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failN > 0 {
		f.failN--
		return errors.New("mysql down")
	}
	for id, n := range c {
		f.counts[id] += n
	}
	for _, e := range h {
		f.history[[2]int64{e.UserID, e.VideoID}] = e.PositionSec
	}
	return nil
}

func setup(t *testing.T) (*redis.Client, *fakeSink, *rank.Board, *Consumer) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sink := &fakeSink{counts: map[int64]int64{}, history: map[[2]int64]int{}}
	board := rank.New(rdb, "rank:t")
	c := NewConsumer(rdb, sink, board, ConsumerConfig{Name: "c1", Batch: 100, Block: 50 * time.Millisecond, ClaimAge: 20 * time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err := c.EnsureGroup(context.Background()); err != nil {
		t.Fatal(err)
	}
	return rdb, sink, board, c
}

func drain(t *testing.T, c *Consumer) {
	t.Helper()
	for i := 0; i < 50; i++ {
		n, err := c.ReadOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
	}
}

func TestConsumer_AggregatesIntoRankAndSink(t *testing.T) {
	rdb, sink, board, c := setup(t)
	p := NewProducer(rdb)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		vid := int64(1 + i%10) // 10 支影片各 100 次
		if err := p.Publish(ctx, Event{ID: fmt.Sprintf("e%d", i), UserID: int64(i%7 + 1), VideoID: vid, PositionSec: i}); err != nil {
			t.Fatal(err)
		}
	}
	drain(t, c)
	for vid := int64(1); vid <= 10; vid++ {
		if sink.counts[vid] != 100 {
			t.Fatalf("video %d play_count=%d want 100", vid, sink.counts[vid])
		}
	}
	top, _ := board.Top(ctx, 10)
	if len(top) != 10 || top[0].Score != 100 {
		t.Fatalf("rank: %+v", top)
	}
	if pend, _ := rdb.XPending(ctx, Stream, Group).Result(); pend.Count != 0 {
		t.Fatalf("%d messages left un-ACKed", pend.Count)
	}
}

func TestConsumer_DuplicateEventIDsCountedOnce(t *testing.T) {
	rdb, sink, _, c := setup(t)
	p := NewProducer(rdb)
	ctx := context.Background()
	for i := 0; i < 5; i++ { // 同一個 event id 投遞 5 次（同批內重複）
		p.Publish(ctx, Event{ID: "same", UserID: 1, VideoID: 9, PositionSec: 10})
	}
	drain(t, c)
	p.Publish(ctx, Event{ID: "same", UserID: 1, VideoID: 9, PositionSec: 10}) // 跨批重送
	drain(t, c)
	if sink.counts[9] != 1 {
		t.Fatalf("counted %d times, want 1", sink.counts[9])
	}
}

func TestConsumer_FailedBatchIsRetriedViaClaim_NoLoss(t *testing.T) {
	rdb, sink, board, c := setup(t)
	sink.failN = 1
	p := NewProducer(rdb)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		p.Publish(ctx, Event{ID: fmt.Sprintf("x%d", i), UserID: 1, VideoID: 3, PositionSec: i})
	}
	if _, err := c.ReadOnce(ctx); err == nil {
		t.Fatal("first batch should fail (sink down)")
	}
	if sink.counts[3] != 0 {
		t.Fatal("nothing should be applied yet")
	}
	time.Sleep(40 * time.Millisecond) // 超過 ClaimAge，訊息變成可認領
	n, err := c.ClaimStale(ctx)
	if err != nil || n != 50 {
		t.Fatalf("claimed %d err=%v, want 50", n, err)
	}
	if sink.counts[3] != 50 {
		t.Fatalf("count=%d want 50 after retry", sink.counts[3])
	}
	if top, _ := board.Top(ctx, 1); top[0].Score != 50 {
		t.Fatalf("rank score %v want 50", top[0].Score)
	}
}

func TestConsumer_MalformedEventDroppedAndAcked(t *testing.T) {
	rdb, sink, _, c := setup(t)
	ctx := context.Background()
	rdb.XAdd(ctx, &redis.XAddArgs{Stream: Stream, Values: map[string]any{"id": "bad", "uid": "x", "vid": "1"}})
	drain(t, c)
	if len(sink.counts) != 0 {
		t.Fatal("malformed event must not be counted")
	}
	if pend, _ := rdb.XPending(ctx, Stream, Group).Result(); pend.Count != 0 {
		t.Fatal("malformed event must be ACKed so it does not loop forever")
	}
}
