package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisRemote 把 go-redis 包成 Remote。
type RedisRemote struct{ C redis.UniversalClient }

func (r RedisRemote) Get(ctx context.Context, key string) ([]byte, bool, error) {
	b, err := r.C.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func (r RedisRemote) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return r.C.Set(ctx, key, val, ttl).Err()
}

func (r RedisRemote) Del(ctx context.Context, keys ...string) error {
	return r.C.Del(ctx, keys...).Err()
}
