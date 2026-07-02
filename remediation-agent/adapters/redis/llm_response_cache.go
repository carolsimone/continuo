package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// LLMResponseCache is a Redis-backed store of LLM propose results, keyed by the
// caller's content-addressed idempotency key. It stores only the small
// ProposeResult (never the prompt) as JSON with a TTL, and relies on Redis
// eviction (allkeys-lru); it never scans keys.
type LLMResponseCache struct {
	client *goredis.Client
	ttl    time.Duration
}

var _ ports.LLMResponseCache = (*LLMResponseCache)(nil)

// NewLLMResponseCache builds a cache backed by an existing Redis client, writing
// entries with the given TTL.
func NewLLMResponseCache(client *goredis.Client, ttl time.Duration) *LLMResponseCache {
	return &LLMResponseCache{client: client, ttl: ttl}
}

// Get returns the cached result for key. A missing key is a miss (found=false,
// no error); a decode or transport error is returned for the caller to treat as
// best-effort.
func (c *LLMResponseCache) Get(ctx context.Context, key string) (ports.ProposeResult, bool, error) {
	data, err := c.client.Get(ctx, key).Bytes()
	if errors.Is(err, goredis.Nil) {
		return ports.ProposeResult{}, false, nil
	}
	if err != nil {
		return ports.ProposeResult{}, false, fmt.Errorf("redis get: %w", err)
	}
	var res ports.ProposeResult
	if err := json.Unmarshal(data, &res); err != nil {
		return ports.ProposeResult{}, false, fmt.Errorf("decode cached result: %w", err)
	}
	return res, true, nil
}

// Put stores result under key with the configured TTL.
func (c *LLMResponseCache) Put(ctx context.Context, key string, result ports.ProposeResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	if err := c.client.Set(ctx, key, data, c.ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}
