package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// LLMResponseCache stores propose results in Redis as JSON, keyed by the
// caller's idempotency key, with a fixed TTL. It relies on Redis eviction
// (allkeys-lru) for capacity management and never scans keys. It is
// best-effort: it returns errors so the caller can log-and-fall-through, but a
// miss (missing key) is not an error.
type LLMResponseCache struct {
	client *goredis.Client
	ttl    time.Duration
}

var _ ports.LLMResponseCache = (*LLMResponseCache)(nil)

// NewLLMResponseCache builds a Redis-backed cache with the given entry TTL.
func NewLLMResponseCache(client *goredis.Client, ttl time.Duration) *LLMResponseCache {
	return &LLMResponseCache{client: client, ttl: ttl}
}

// Get returns the cached result for key. ok is false when the key is absent;
// that is not an error. A backend or decode failure returns an error.
func (c *LLMResponseCache) Get(ctx context.Context, key string) (ports.ProposeResult, bool, error) {
	raw, err := c.client.Get(ctx, key).Bytes()
	if err == goredis.Nil {
		return ports.ProposeResult{}, false, nil
	}
	if err != nil {
		return ports.ProposeResult{}, false, fmt.Errorf("llm cache get: %w", err)
	}
	var res ports.ProposeResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return ports.ProposeResult{}, false, fmt.Errorf("llm cache decode: %w", err)
	}
	return res, true, nil
}

// Put stores result under key as JSON with the configured TTL.
func (c *LLMResponseCache) Put(ctx context.Context, key string, result ports.ProposeResult) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("llm cache encode: %w", err)
	}
	if err := c.client.Set(ctx, key, raw, c.ttl).Err(); err != nil {
		return fmt.Errorf("llm cache put: %w", err)
	}
	return nil
}
