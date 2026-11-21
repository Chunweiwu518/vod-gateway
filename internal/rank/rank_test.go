package rank

import (
	"context"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestBoard_ShardedTopKEqualsGlobalBruteForce(t *testing.T) {
	mr := miniredis.RunT(t)
	b := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "rank:t")
	ctx := context.Background()
	r := rand.New(rand.NewPCG(1, 2))
	const n = 10000
	scores := map[int64]float64{}
	for i := int64(1); i <= n; i++ {
		scores[i] = float64(r.IntN(1_000_000))
	}
	if err := b.Incr(ctx, scores); err != nil {
		t.Fatal(err)
	}

	got, err := b.Top(ctx, 20)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]Item, 0, n)
	for id, s := range scores {
		want = append(want, Item{id, s})
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].Score != want[j].Score {
			return want[i].Score > want[j].Score
		}
		return want[i].VideoID < want[j].VideoID
	})
	for i := 0; i < 20; i++ {
		if got[i] != want[i] {
			t.Fatalf("rank %d: got %+v want %+v", i, got[i], want[i])
		}
	}

	// 大 key 檢查：10000 個成員要平均分在 16 段，最大的一段不能超過平均的 1.2 倍。
	sizes, _ := b.ShardSizes(ctx)
	var max, sum int64
	for _, s := range sizes {
		sum += s
		if s > max {
			max = s
		}
	}
	if sum != n || float64(max) > float64(n)/Shards*1.2 {
		t.Fatalf("shards uneven: sum=%d max=%d sizes=%v", sum, max, sizes)
	}
	t.Logf("%d 個成員分成 %d 段，最大一段 %d（平均 %d）", n, Shards, max, n/Shards)
}

func TestBoard_IncrAccumulates(t *testing.T) {
	mr := miniredis.RunT(t)
	b := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "rank:t")
	ctx := context.Background()
	b.Incr(ctx, map[int64]float64{5: 3, 6: 1})
	b.Incr(ctx, map[int64]float64{5: 2, 6: 10})
	top, _ := b.Top(ctx, 2)
	if len(top) != 2 || top[0] != (Item{6, 11}) || top[1] != (Item{5, 5}) {
		t.Fatalf("got %+v", top)
	}
}
