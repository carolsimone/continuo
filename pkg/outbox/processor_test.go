// pkg/outbox/processor_test.go
package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
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

// exhaustedRetryCount is the retry_count of a row whose next failure exhausts
// its retry budget: the execution_outbox max_retries column defaults to 13, and
// a failure is terminal once retry_count+1 reaches it.
const exhaustedRetryCount = 12

// seedRow inserts one pending row that has already failed retryCount times.
// max_retries stays at its column default.
func seedRow(t *testing.T, db *sqlx.DB, retryCount int) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, aggregate_type, aggregate_id, event_type, payload, stream_name, retry_count)
		VALUES ($1, 'pkg-outbox-test', $2, 'test_event', '{}', 'pkg-outbox-test-stream', $3)`, testOutboxTable),
		id, uuid.New(), retryCount)
	require.NoError(t, err)
	return id
}

func TestProcessor_SuccessMarksProcessed(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, 0)

	pub := &fakePublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM `+testOutboxTable+` WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "processed", status)
	assert.Equal(t, 1, pub.calls)
}

func TestProcessor_TransientErrorIncrementsRetry(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, 0)

	pub := &fakePublisher{failTimes: 1}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	var rc int
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM `+testOutboxTable+` WHERE id=$1`, id).Scan(&status, &rc))
	assert.Equal(t, "scheduled", status)
	assert.Equal(t, 1, rc)
}

func TestProcessor_RetryBudgetExhaustedMarksFailedAndCallsHook(t *testing.T) {
	db := dbForTest(t)
	// The row is one failure short of its budget, so the first failure exhausts it.
	id := seedRow(t, db, exhaustedRetryCount)

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
	require.NoError(t, db.QueryRow(`SELECT status, error_message FROM `+testOutboxTable+` WHERE id=$1`, id).Scan(&status, &errMsg))
	assert.Equal(t, "failed", status)
	assert.Equal(t, "synthetic publisher error", errMsg)
	assert.Equal(t, 1, hookCalled)
}

func TestProcessor_NoHookConfiguredStillMarksFailed(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, exhaustedRetryCount)

	pub := &fakePublisher{failTimes: 10}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM `+testOutboxTable+` WHERE id=$1`, id).Scan(&status))
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
	// The row has its whole retry budget left; a permanent error must override
	// it and terminate on attempt 1.
	id := seedRow(t, db, 0)

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
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM `+testOutboxTable+` WHERE id=$1`, id).Scan(&status, &rc))
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
		seedRow(t, db, 0)
	}

	pub := &batchFakePublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{BatchSize: n})
	require.NoError(t, p.ProcessBatch(context.Background()))

	assert.Equal(t, 1, pub.batchCalls, "batch publisher path must be used")

	var processedCount, distinctTimestamps int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM `+testOutboxTable+` WHERE status='processed'`).Scan(&processedCount))
	require.NoError(t, db.QueryRow(`SELECT count(DISTINCT processed_at) FROM `+testOutboxTable+` WHERE status='processed'`).Scan(&distinctTimestamps))
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
		ids = append(ids, seedRow(t, db, 0))
	}
	failID := ids[2]

	pub := &batchFakePublisher{failIDs: map[uuid.UUID]error{failID: errors.New("xadd mid-batch boom")}}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{BatchSize: 5})
	require.NoError(t, p.ProcessBatch(context.Background()))

	for _, id := range ids {
		var status string
		var rc int
		require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM `+testOutboxTable+` WHERE id=$1`, id).Scan(&status, &rc))
		if id == failID {
			assert.Equal(t, "scheduled", status, "transiently-failed row moves to scheduled")
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
		seedRow(t, db, 0)
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
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM `+testOutboxTable+` WHERE status='pending'`).Scan(&pending))
	assert.Equal(t, 0, pending, "drain loop must clear the entire backlog within the run window")
	assert.GreaterOrEqual(t, pub.batchCalls, total/batch, "each full batch is its own pipelined publish")
}

// A transient error must reschedule (future next_attempt_at, status
// 'scheduled' rather than terminal 'failed'), NOT mark failed and NOT write a
// dead-letter — even across a single ProcessBatch.
func TestProcessor_TransientErrorReschedulesWithBackoff(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, 0)

	pub := &fakePublisher{failTimes: 1}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	var rc int
	var next *time.Time
	require.NoError(t, db.QueryRow(
		`SELECT status, retry_count, next_attempt_at FROM `+testOutboxTable+` WHERE id=$1`, id,
	).Scan(&status, &rc, &next))
	assert.Equal(t, "scheduled", status)
	assert.Equal(t, 1, rc)
	require.NotNil(t, next, "transient failure must set next_attempt_at")
	assert.True(t, next.After(time.Now()), "next_attempt_at must be in the future")

	var deadLetters int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM `+testOutboxTable+` WHERE event_type=$1`, outbox.DeadLetterEventType,
	).Scan(&deadLetters))
	assert.Equal(t, 0, deadLetters, "transient (non-exhausted) failure must not dead-letter")
}

// The production incident (#280): a transient outage spanning several attempts,
// then recovery, must end in 'processed' — never 'failed'.
func TestProcessor_TransientOutageThenRecoveryReachesProcessed(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, 0)

	// Fail the first 3 publish attempts, then succeed — simulating a ~outage.
	// Zero backoff so the test drives attempts back-to-back without waiting.
	pub := &fakePublisher{failTimes: 3}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(),
		outbox.ProcessorConfig{RetryBaseDelay: time.Nanosecond, RetryMaxDelay: time.Nanosecond})

	for i := 0; i < 5; i++ { // more cycles than failures; each re-selects the due row
		require.NoError(t, p.ProcessBatch(context.Background()))
	}

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM `+testOutboxTable+` WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "processed", status, "row must recover to processed, never failed (issue #280)")
}

// Permanent error: terminal on attempt #1, retry_count stays 0, and a dead-letter
// row with failure_kind=permanent is written.
func TestProcessor_PermanentErrorDeadLettersImmediately(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, 0)

	pub := &permanentFailingPublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	var rc int
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM `+testOutboxTable+` WHERE id=$1`, id).Scan(&status, &rc))
	assert.Equal(t, "failed", status)
	assert.Equal(t, 0, rc, "permanent error must not consume retries")
	assert.Equal(t, 1, pub.calls, "publisher called exactly once")

	var kind string
	require.NoError(t, db.QueryRow(
		`SELECT payload->>'failure_kind' FROM `+testOutboxTable+` WHERE event_type=$1`, outbox.DeadLetterEventType,
	).Scan(&kind))
	assert.Equal(t, string(model.DeadLetterKindPermanent), kind)
}

// Transient budget exhaustion: after MaxRetries, terminal with a
// failure_kind=transient_exhausted dead-letter.
func TestProcessor_TransientExhaustionDeadLetters(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, exhaustedRetryCount) // the next failure exhausts the budget

	pub := &fakePublisher{failTimes: 10}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM `+testOutboxTable+` WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "failed", status)

	var kind string
	require.NoError(t, db.QueryRow(
		`SELECT payload->>'failure_kind' FROM `+testOutboxTable+` WHERE event_type=$1`, outbox.DeadLetterEventType,
	).Scan(&kind))
	assert.Equal(t, string(model.DeadLetterKindTransientExhausted), kind)
}

// Loop guard: a dead-letter row that itself fails to publish must NOT spawn a
// second dead-letter — it just parks (or reschedules) as any other row.
func TestProcessor_DeadLetterRowDoesNotSpawnAnotherDeadLetter(t *testing.T) {
	db := dbForTest(t)
	// Seed a dead-letter row directly, one failure short of its budget so its failure is terminal.
	dlID := uuid.New()
	_, err := db.Exec(
		`INSERT INTO `+testOutboxTable+` (id, aggregate_type, aggregate_id, event_type, payload, stream_name, retry_count)
		 VALUES ($1, $2, $3, $4, '{"failure_kind":"permanent"}'::jsonb, $5, 12)`,
		dlID, outbox.DeadLetterAggregateType, uuid.New(), outbox.DeadLetterEventType, streams.OutboxDeadLetterV1,
	)
	require.NoError(t, err)

	pub := &fakePublisher{failTimes: 10}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var count int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM `+testOutboxTable+` WHERE event_type=$1`, outbox.DeadLetterEventType,
	).Scan(&count))
	assert.Equal(t, 1, count, "the failing dead-letter row must not create a second dead-letter")
}
