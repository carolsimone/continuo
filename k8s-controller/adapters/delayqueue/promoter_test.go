package delayqueue

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestPromoteDue_MovesDueToStreamAndRemoves proves a due ticket is XADD'd to the
// stream as a `payload` field and removed from BOTH the ZSET and the HASH.
func TestPromoteDue_MovesDueToStreamAndRemoves(t *testing.T) {
	r := newTestRedis(t)
	ctx := context.Background()
	require.NoError(t, Schedule(ctx, r, "job-due", `{"job_name":"job-due"}`, 1000))

	p := NewPromoter(r, testLogger())
	n, err := p.PromoteDue(ctx, 1500) // now=1500 > 1000 → due
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	msgs, err := r.XRange(ctx, p.stream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, `{"job_name":"job-due"}`, msgs[0].Values["payload"])

	zcard, err := r.ZCard(ctx, PendingKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), zcard, "promoted member removed from ZSET")
	hlen, err := r.HLen(ctx, TicketsKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), hlen, "promoted payload removed from HASH")
}

// TestPromoteDue_LeavesNotYetDue proves a future ticket is not promoted.
func TestPromoteDue_LeavesNotYetDue(t *testing.T) {
	r := newTestRedis(t)
	ctx := context.Background()
	require.NoError(t, Schedule(ctx, r, "job-future", `{"x":1}`, 5000))

	p := NewPromoter(r, testLogger())
	n, err := p.PromoteDue(ctx, 1500) // now=1500 < 5000 → not due
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	xlen, err := r.XLen(ctx, p.stream).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), xlen)
	zcard, err := r.ZCard(ctx, PendingKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), zcard, "not-yet-due member stays")
}

// TestPromoteDue_ExactlyOnce proves a due ticket promotes once: a second
// PromoteDue over the same now finds nothing (the atomic ZREM/HDEL inside the
// script means a concurrent/repeat run sees an empty due set). This is the
// observable proxy for the multi-replica exactly-once guarantee.
func TestPromoteDue_ExactlyOnce(t *testing.T) {
	r := newTestRedis(t)
	ctx := context.Background()
	require.NoError(t, Schedule(ctx, r, "job-1", `{"v":1}`, 1000))

	p := NewPromoter(r, testLogger())
	n1, err := p.PromoteDue(ctx, 2000)
	require.NoError(t, err)
	assert.Equal(t, 1, n1)

	n2, err := p.PromoteDue(ctx, 2000)
	require.NoError(t, err)
	assert.Equal(t, 0, n2, "already-promoted ticket must not promote again")

	xlen, err := r.XLen(ctx, p.stream).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), xlen, "exactly one stream message")
}

// TestPromoteDue_ReschedulePromotesOnce proves a job scheduled twice before its
// due time promotes exactly one stream message (in-place ZSET/HASH update).
func TestPromoteDue_ReschedulePromotesOnce(t *testing.T) {
	r := newTestRedis(t)
	ctx := context.Background()
	require.NoError(t, Schedule(ctx, r, "job-1", `{"v":1}`, 1000))
	require.NoError(t, Schedule(ctx, r, "job-1", `{"v":2}`, 1200))

	p := NewPromoter(r, testLogger())
	n, err := p.PromoteDue(ctx, 2000)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	msgs, err := r.XRange(ctx, p.stream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, `{"v":2}`, msgs[0].Values["payload"], "latest payload promoted")
}
