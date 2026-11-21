// Package rank 是 Redis ZSET 排行榜。
// 為了避免一個超大的 key（單一 ZSET 幾百萬成員會讓 ZADD、刪除、遷移都卡住事件迴圈），
// 依 video_id 取模切成 Shards 個 ZSET；查前 K 名時每段各取前 K 再合併，結果與單一 ZSET 相同。
package rank

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const Shards = 16

type Item struct {
	VideoID int64   `json:"video_id"`
	Score   float64 `json:"score"`
}

type Board struct {
	rdb    redis.UniversalClient
	prefix string
}

func New(rdb redis.UniversalClient, prefix string) *Board { return &Board{rdb: rdb, prefix: prefix} }

func (b *Board) key(videoID int64) string {
	return fmt.Sprintf("%s:%d", b.prefix, videoID%Shards)
}

func (b *Board) Keys() []string {
	ks := make([]string, Shards)
	for i := range ks {
		ks[i] = fmt.Sprintf("%s:%d", b.prefix, i)
	}
	return ks
}

// Incr 批次累加分數，一次 pipeline。
func (b *Board) Incr(ctx context.Context, deltas map[int64]float64) error {
	if len(deltas) == 0 {
		return nil
	}
	p := b.rdb.Pipeline()
	for id, d := range deltas {
		p.ZIncrBy(ctx, b.key(id), d, strconv.FormatInt(id, 10))
	}
	_, err := p.Exec(ctx)
	return err
}

// Top 回傳全站前 k 名。
func (b *Board) Top(ctx context.Context, k int) ([]Item, error) {
	p := b.rdb.Pipeline()
	cmds := make([]*redis.ZSliceCmd, Shards)
	for i, key := range b.Keys() {
		cmds[i] = p.ZRevRangeWithScores(ctx, key, 0, int64(k-1))
	}
	if _, err := p.Exec(ctx); err != nil {
		return nil, err
	}
	var all []Item
	for _, c := range cmds {
		for _, z := range c.Val() {
			id, err := strconv.ParseInt(z.Member.(string), 10, 64)
			if err != nil {
				continue
			}
			all = append(all, Item{VideoID: id, Score: z.Score})
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Score != all[j].Score {
			return all[i].Score > all[j].Score
		}
		return all[i].VideoID < all[j].VideoID
	})
	if len(all) > k {
		all = all[:k]
	}
	return all, nil
}

// ShardSizes 回傳每段的成員數，測試與監控用。
func (b *Board) ShardSizes(ctx context.Context) ([]int64, error) {
	p := b.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, Shards)
	for i, key := range b.Keys() {
		cmds[i] = p.ZCard(ctx, key)
	}
	if _, err := p.Exec(ctx); err != nil {
		return nil, err
	}
	out := make([]int64, Shards)
	for i, c := range cmds {
		out[i] = c.Val()
	}
	return out, nil
}
