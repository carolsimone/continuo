package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	goredis "github.com/redis/go-redis/v9"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

func newTestRedis(t *testing.T) (*goredis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	return goredis.NewClient(&goredis.Options{Addr: mr.Addr()}), mr
}

func TestLLMResponseCache_PutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestRedis(t)
	cache := NewLLMResponseCache(client, time.Hour)

	want := ports.ProposeResult{
		ProposedSQL:            "SELECT 1",
		Rationale:              "cast the join key",
		Confidence:             "high",
		SuspectedRootCauseNode: "model.svc.table_a",
		Model:                  "claude-x",
	}

	require.NoError(t, cache.Put(ctx, "llmcache:abc", want))

	got, ok, err := cache.Get(ctx, "llmcache:abc")
	require.NoError(t, err)
	assert.True(t, ok, "the stored key is a hit")
	assert.Equal(t, want, got, "round-trip preserves every field")
}

func TestLLMResponseCache_MissingKey(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestRedis(t)
	cache := NewLLMResponseCache(client, time.Hour)

	_, ok, err := cache.Get(ctx, "llmcache:nope")
	require.NoError(t, err, "a missing key is not an error")
	assert.False(t, ok)
}

func TestLLMResponseCache_PutSetsTTL(t *testing.T) {
	ctx := context.Background()
	client, mr := newTestRedis(t)
	cache := NewLLMResponseCache(client, time.Hour)

	require.NoError(t, cache.Put(ctx, "llmcache:ttl", ports.ProposeResult{ProposedSQL: "x"}))

	// The key exists now; after fast-forwarding past the TTL it is gone.
	_, ok, err := cache.Get(ctx, "llmcache:ttl")
	require.NoError(t, err)
	require.True(t, ok)

	mr.FastForward(2 * time.Hour)
	_, ok, err = cache.Get(ctx, "llmcache:ttl")
	require.NoError(t, err)
	assert.False(t, ok, "the entry expired after its TTL")
}
