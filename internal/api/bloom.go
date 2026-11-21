package api

import (
	"encoding/binary"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

// Bloom 包一層讀寫鎖（底層的 BloomFilter 不是並行安全的）。
// 用途是擋穿透：id 不在過濾器裡就一定不存在，請求不必碰快取與 DB。
// 誤判（說存在但其實沒有）由空值快取兜底。
type Bloom struct {
	mu sync.RWMutex
	f  *bloom.BloomFilter
}

func NewBloom(ids []int64, fpRate float64) *Bloom {
	n := uint(len(ids)) * 2
	if n < 100_000 {
		n = 100_000
	}
	b := &Bloom{f: bloom.NewWithEstimates(n, fpRate)}
	for _, id := range ids {
		b.f.Add(key(id))
	}
	return b
}

func key(id int64) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], uint64(id))
	return k[:]
}

func (b *Bloom) Test(id int64) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.f.Test(key(id))
}

func (b *Bloom) Add(id int64) {
	b.mu.Lock()
	b.f.Add(key(id))
	b.mu.Unlock()
}
