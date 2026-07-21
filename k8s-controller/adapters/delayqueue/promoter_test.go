package delayqueue

import (
	"context"
	"log/slog"
	"os"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestPromoteDue_MovesDueToStreamAndRemoves proves a due ticket is XADD'd to the
// stream carrying both the `payload` and a flat `outbox_entry_id` field, and is
// removed from BOTH the ZSET and the HASH.
func TestPromoteDue_MovesDueToStreamAndRemoves(t *testing.T) {
	r := newTestRedis(t)
	ctx := context.Background()
	require.NoError(t, Schedule(ctx, r, "job-due", "entry-due", `{"job_name":"job-due"}`, 1000))

	p := NewPromoter(r, testLogger())
	n, err := p.PromoteDue(ctx, 1500) // now=1500 > 1000 → due
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	msgs, err := r.XRange(ctx, p.stream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, `{"job_name":"job-due"}`, msgs[0].Values["payload"])
	assert.Equal(t, "entry-due", msgs[0].Values["outbox_entry_id"])

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
	require.NoError(t, Schedule(ctx, r, "job-future", "entry-future", `{"x":1}`, 5000))

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
	require.NoError(t, Schedule(ctx, r, "job-1", "entry-1", `{"v":1}`, 1000))

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
	require.NoError(t, Schedule(ctx, r, "job-1", "entry-1", `{"v":1}`, 1000))
	require.NoError(t, Schedule(ctx, r, "job-1", "entry-2", `{"v":2}`, 1200))

	p := NewPromoter(r, testLogger())
	n, err := p.PromoteDue(ctx, 2000)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	msgs, err := r.XRange(ctx, p.stream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, `{"v":2}`, msgs[0].Values["payload"], "latest payload promoted")
}

// TestPromoteDue_DropsMalformedTicketWithoutWedging proves a HASH value that is
// not a valid {entry_id,payload} ticket is dropped rather than aborting the whole
// script. Because ZREM runs after XADD, a bad member that raised an error would
// never be removed and would re-select every tick, wedging the queue for every
// job behind it. The well-formed ticket scheduled alongside it still promotes.
func TestPromoteDue_DropsMalformedTicketWithoutWedging(t *testing.T) {
	r := newTestRedis(t)
	ctx := context.Background()

	// A well-formed ticket via Schedule, plus a malformed one injected directly
	// into both structures (raw JSON that is not a {entry_id,payload} envelope).
	require.NoError(t, Schedule(ctx, r, "job-good", "entry-good", `{"job_name":"job-good"}`, 1000))
	require.NoError(t, r.HSet(ctx, TicketsKey, "job-bad", `{"task_id":"legacy"}`).Err())
	require.NoError(t, r.ZAdd(ctx, PendingKey, goredis.Z{Score: 1000, Member: "job-bad"}).Err())

	p := NewPromoter(r, testLogger())
	n, err := p.PromoteDue(ctx, 2000)
	require.NoError(t, err, "a malformed ticket must not fail the whole promotion")
	assert.Equal(t, 2, n, "both due members are processed (one promoted, one dropped)")

	msgs, err := r.XRange(ctx, p.stream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1, "only the well-formed ticket is promoted")
	assert.Equal(t, `{"job_name":"job-good"}`, msgs[0].Values["payload"])
	assert.Equal(t, "entry-good", msgs[0].Values["outbox_entry_id"])

	// Both members are cleared from the queue — the bad one cannot wedge it.
	zcard, err := r.ZCard(ctx, PendingKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), zcard)
	hlen, err := r.HLen(ctx, TicketsKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), hlen)
}

// TestPromoteDue_ReplayCarriesSameOutboxEntryID proves that when a promoted
// ticket is re-scheduled (an outbox row whose Postgres tx rolled back after its
// Redis write, retried after the ticket was already promoted and deleted) and
// promoted again, both stream messages carry the SAME outbox_entry_id. That flat
// field is what lets the consumer's secondary dedup key suppress the replay even
// though each promotion assigned a distinct Redis msg_id.
func TestPromoteDue_ReplayCarriesSameOutboxEntryID(t *testing.T) {
	r := newTestRedis(t)
	ctx := context.Background()
	p := NewPromoter(r, testLogger())

	// First schedule + promote: the ticket is XADD'd and deleted from the HASH.
	require.NoError(t, Schedule(ctx, r, "job-1", "entry-1", `{"v":1}`, 1000))
	n1, err := p.PromoteDue(ctx, 2000)
	require.NoError(t, err)
	assert.Equal(t, 1, n1)

	// Outbox retry re-schedules the same check under the same entry ID, and it
	// promotes again into a second stream message.
	require.NoError(t, Schedule(ctx, r, "job-1", "entry-1", `{"v":1}`, 1000))
	n2, err := p.PromoteDue(ctx, 2000)
	require.NoError(t, err)
	assert.Equal(t, 1, n2)

	msgs, err := r.XRange(ctx, p.stream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 2, "two promotions produce two stream messages")
	assert.NotEqual(t, msgs[0].ID, msgs[1].ID, "distinct Redis msg_ids")
	assert.Equal(t, "entry-1", msgs[0].Values["outbox_entry_id"])
	assert.Equal(t, "entry-1", msgs[1].Values["outbox_entry_id"],
		"replay carries the same outbox_entry_id so the consumer can dedup it")
}
