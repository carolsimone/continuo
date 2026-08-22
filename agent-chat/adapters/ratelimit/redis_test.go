package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	goredis "github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) *goredis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	return goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
}

func TestRedis_PerUserPerMinute(t *testing.T) {
	ctx := context.Background()
	client := newTestRedis(t)
	now := time.Now()

	rl := NewRedis(client, 2)
	rl.now = func() time.Time { return now }

	allow := func(user string) bool {
		ok, err := rl.Allow(ctx, user)
		require.NoError(t, err)
		return ok
	}

	assert.True(t, allow("alice"))
	assert.True(t, allow("alice"))
	assert.False(t, allow("alice"), "third message in the window is rejected")

	// Independent per user.
	assert.True(t, allow("bob"))

	// Window slides: alice's earlier hits age out and she is admitted again.
	now = now.Add(61 * time.Second)
	assert.True(t, allow("alice"))
}

func TestRedis_SharedAcrossClients(t *testing.T) {
	ctx := context.Background()
	client := newTestRedis(t)
	now := time.Now()

	// Two limiter instances against the same Redis model two replicas; the
	// per-user limit must be enforced globally, not per instance.
	mk := func() *Redis {
		r := NewRedis(client, 2)
		r.now = func() time.Time { return now }
		return r
	}
	a, b := mk(), mk()

	ok, err := a.Allow(ctx, "alice")
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = b.Allow(ctx, "alice")
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = a.Allow(ctx, "alice")
	require.NoError(t, err)
	assert.False(t, ok, "the global limit is reached even across replicas")
}

func TestRedis_AllowReturnsErrorWhenBackendDown(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	mr.Close() // simulate Redis being unreachable

	rl := NewRedis(client, 2)
	_, err := rl.Allow(ctx, "alice")
	assert.Error(t, err, "a dead backend surfaces an error so the caller can fail open")
}
