package dedup

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Deduper struct {
	client *redis.Client
	ttl    time.Duration
}

func New(addr string, ttl time.Duration) *Deduper {
	return &Deduper{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		ttl:    ttl,
	}
}

func (d *Deduper) Seen(ctx context.Context, key string) (bool, error) {
	wasSet, err := d.client.SetNX(ctx, key, "1", d.ttl).Result()
	if err != nil {
		return false, err
	}
	return !wasSet, nil
}

func (d *Deduper) SeenBatch(ctx context.Context, keys []string) ([]bool, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	pipe := d.client.Pipeline()
	cmds := make([]*redis.BoolCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.SetNX(ctx, k, "1", d.ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	seen := make([]bool, len(keys))
	for i, cmd := range cmds {
		wasSet, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		seen[i] = !wasSet
	}
	return seen, nil
}

func (d *Deduper) Close() error {
	return d.client.Close()
}
