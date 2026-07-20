package delayqueue

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestRedis(t *testing.T) *goredis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	return goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
}

// TestSchedule_WritesHashAndZSetKeyedByJobName proves a scheduled check lands as
// one HASH field + one ZSET member, both keyed by JobName.
func TestSchedule_WritesHashAndZSetKeyedByJobName(t *testing.T) {
	r := newTestRedis(t)
	ctx := context.Background()

	require.NoError(t, Schedule(ctx, r, "job-1", `{"job_name":"job-1"}`, 1700000000))

	payload, err := r.HGet(ctx, TicketsKey, "job-1").Result()
	require.NoError(t, err)
	assert.Equal(t, `{"job_name":"job-1"}`, payload)

	score, err := r.ZScore(ctx, PendingKey, "job-1").Result()
	require.NoError(t, err)
	assert.Equal(t, float64(1700000000), score)
}

// TestSchedule_RescheduleIsInPlace proves scheduling the same JobName twice
// yields exactly one HASH field and one ZSET member (in-place update) — the
// property that makes unbounded accumulation impossible.
func TestSchedule_RescheduleIsInPlace(t *testing.T) {
	r := newTestRedis(t)
	ctx := context.Background()

	require.NoError(t, Schedule(ctx, r, "job-1", `{"v":1}`, 1000))
	require.NoError(t, Schedule(ctx, r, "job-1", `{"v":2}`, 2000))

	hlen, err := r.HLen(ctx, TicketsKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), hlen, "HASH must hold one entry per job")

	zcard, err := r.ZCard(ctx, PendingKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), zcard, "ZSET must hold one entry per job")

	payload, err := r.HGet(ctx, TicketsKey, "job-1").Result()
	require.NoError(t, err)
	assert.Equal(t, `{"v":2}`, payload, "latest payload wins")

	score, err := r.ZScore(ctx, PendingKey, "job-1").Result()
	require.NoError(t, err)
	assert.Equal(t, float64(2000), score, "latest score wins")
}
