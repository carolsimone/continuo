// pkg/outbox/processor_test.go
package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakePublisher struct {
	failTimes int // first N calls error; subsequent succeed
	calls     int
	lastEntry *outbox.Entry
}

func (f *fakePublisher) Publish(_ context.Context, e *outbox.Entry) error {
	f.calls++
	f.lastEntry = e
	if f.calls <= f.failTimes {
		return errors.New("synthetic publisher error")
	}
	return nil
}

// batchFakePublisher implements both Publish and PublishBatch. failIDs names
// entries (by ID) that should fail; everything else succeeds. It records how
// many times PublishBatch was invoked so a test can assert the batch path was
// taken.
type batchFakePublisher struct {
	failIDs    map[uuid.UUID]error
	batchCalls int
}

func (b *batchFakePublisher) Publish(_ context.Context, e *outbox.Entry) error {
	if err, ok := b.failIDs[e.ID]; ok {
		return err
	}
	return nil
}

func (b *batchFakePublisher) PublishBatch(_ context.Context, entries []*outbox.Entry) []error {
	b.batchCalls++
	errs := make([]error, len(entries))
	for i, e := range entries {
		if err, ok := b.failIDs[e.ID]; ok {
			errs[i] = err
		}
	}
	return errs
}

func seedRow(t *testing.T, db *sqlx.DB, maxRetries int) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(
		`INSERT INTO orchestrator_outbox (id, aggregate_type, aggregate_id, event_type, payload, stream_name, max_retries)
		 VALUES ($1, 'task', $2, 'x', '{}'::jsonb, 'x:v1', $3)`,
		id, uuid.New(), maxRetries,
	)
	require.NoError(t, err)
	return id
}

func TestProcessor_SuccessMarksProcessed(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, 3)

	pub := &fakePublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM orchestrator_outbox WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "processed", status)
	assert.Equal(t, 1, pub.calls)
}

func TestProcessor_TransientErrorIncrementsRetry(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, 3)

	pub := &fakePublisher{failTimes: 1}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	var rc int
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM orchestrator_outbox WHERE id=$1`, id).Scan(&status, &rc))
	assert.Equal(t, "pending", status)
	assert.Equal(t, 1, rc)
}

func TestProcessor_RetryBudgetExhaustedMarksFailedAndCallsHook(t *testing.T) {
	db := dbForTest(t)
	// MaxRetries = 1 so even attempt 1 exhausts immediately on failure.
	id := seedRow(t, db, 1)

	hookCalled := 0
	hook := outbox.TerminalFailureHook(func(_ context.Context, e *outbox.Entry, cause error) error {
		hookCalled++
		assert.Equal(t, id, e.ID)
		assert.Error(t, cause)
		return nil
	})
	pub := &fakePublisher{failTimes: 10}
	p := outbox.NewProcessor(db, testOutboxTable, pub, hook, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	var errMsg string
	require.NoError(t, db.QueryRow(`SELECT status, error_message FROM orchestrator_outbox WHERE id=$1`, id).Scan(&status, &errMsg))
	assert.Equal(t, "failed", status)
	assert.Equal(t, "synthetic publisher error", errMsg)
	assert.Equal(t, 1, hookCalled)
}

func TestProcessor_NoHookConfiguredStillMarksFailed(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, 1)

	pub := &fakePublisher{failTimes: 10}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM orchestrator_outbox WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "failed", status)
}

// permanentFailingPublisher returns an error wrapping events.ErrPermanent on
// every call, simulating a deterministic-failure payload (e.g., corrupt data,
// invalid params that won't be fixed by retrying).
type permanentFailingPublisher struct {
	calls int
}

func (p *permanentFailingPublisher) Publish(_ context.Context, _ *outbox.Entry) error {
	p.calls++
	return fmt.Errorf("validation failed: %w", pkgevents.ErrPermanent)
}

// TestProcessor_PermanentErrorShortCircuitsRetries verifies that a publish
// error wrapping events.ErrPermanent bypasses the retry budget and goes
// straight to MarkFailed + TerminalFailureHook on the first attempt, even
// when MaxRetries would otherwise allow more attempts.
func TestProcessor_PermanentErrorShortCircuitsRetries(t *testing.T) {
	db := dbForTest(t)
	// MaxRetries = 5 so the row has plenty of budget; permanent error must
	// override and terminate on attempt 1.
	id := seedRow(t, db, 5)

	hookCalled := 0
	hook := outbox.TerminalFailureHook(func(_ context.Context, e *outbox.Entry, cause error) error {
		hookCalled++
		assert.Equal(t, id, e.ID)
		assert.True(t, errors.Is(cause, pkgevents.ErrPermanent), "hook must receive the permanent error")
		return nil
	})
	pub := &permanentFailingPublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, hook, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	var rc int
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM orchestrator_outbox WHERE id=$1`, id).Scan(&status, &rc))
	assert.Equal(t, "failed", status, "permanent error must mark row failed even with retries remaining")
	assert.Equal(t, 0, rc, "retry_count must NOT be incremented on permanent error")
	assert.Equal(t, 1, hookCalled, "terminal failure hook must fire on permanent error")
	assert.Equal(t, 1, pub.calls, "publisher must be called exactly once before short-circuit")
}

// TestProcessor_BatchSuccessesShareOneProcessedAt verifies the whole successful
// subset of a batch is flipped in a single UPDATE: a single statement stamps
// every row's processed_at from one NOW() call, so all rows carry the same
// timestamp. Per-row updates would produce distinct timestamps.
func TestProcessor_BatchSuccessesShareOneProcessedAt(t *testing.T) {
	db := dbForTest(t)
	const n = 25
	for i := 0; i < n; i++ {
		seedRow(t, db, 3)
	}

	pub := &batchFakePublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{BatchSize: n})
	require.NoError(t, p.ProcessBatch(context.Background()))

	assert.Equal(t, 1, pub.batchCalls, "batch publisher path must be used")

	var processedCount, distinctTimestamps int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM orchestrator_outbox WHERE status='processed'`).Scan(&processedCount))
	require.NoError(t, db.QueryRow(`SELECT count(DISTINCT processed_at) FROM orchestrator_outbox WHERE status='processed'`).Scan(&distinctTimestamps))
	assert.Equal(t, n, processedCount, "every row must be processed")
	assert.Equal(t, 1, distinctTimestamps, "one UPDATE => one processed_at shared by all successes")
}

// TestProcessor_BatchFailureIsolatesFailedRow injects a publish failure for one
// row mid-batch. Only the failed row stays pending with an incremented retry
// count; every other row is processed.
func TestProcessor_BatchFailureIsolatesFailedRow(t *testing.T) {
	db := dbForTest(t)
	ids := make([]uuid.UUID, 0, 5)
	for i := 0; i < 5; i++ {
		ids = append(ids, seedRow(t, db, 3))
	}
	failID := ids[2]

	pub := &batchFakePublisher{failIDs: map[uuid.UUID]error{failID: errors.New("xadd mid-batch boom")}}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{BatchSize: 5})
	require.NoError(t, p.ProcessBatch(context.Background()))

	for _, id := range ids {
		var status string
		var rc int
		require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM orchestrator_outbox WHERE id=$1`, id).Scan(&status, &rc))
		if id == failID {
			assert.Equal(t, "pending", status, "failed row stays pending")
			assert.Equal(t, 1, rc, "failed row retry_count incremented")
		} else {
			assert.Equal(t, "processed", status, "sibling rows processed")
			assert.Equal(t, 0, rc, "sibling rows untouched retry_count")
		}
	}
}

// TestProcessor_DrainClearsBacklogInOneTick seeds far more rows than one batch
// holds and runs the processor for a single tick window; the drain loop must
// clear the whole backlog before the next tick rather than one batch per tick.
func TestProcessor_DrainClearsBacklogInOneTick(t *testing.T) {
	db := dbForTest(t)
	const total = 250
	const batch = 50
	for i := 0; i < total; i++ {
		seedRow(t, db, 3)
	}

	pub := &batchFakePublisher{}
	// A long tick guarantees only one tick fires inside the run window, so a
	// pass that drains everything proves the back-to-back drain loop, not the
	// ticker, did the work.
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(),
		outbox.ProcessorConfig{BatchSize: batch, Tick: 50 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	var pending int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM orchestrator_outbox WHERE status='pending'`).Scan(&pending))
	assert.Equal(t, 0, pending, "drain loop must clear the entire backlog within the run window")
	assert.GreaterOrEqual(t, pub.batchCalls, total/batch, "each full batch is its own pipelined publish")
}
