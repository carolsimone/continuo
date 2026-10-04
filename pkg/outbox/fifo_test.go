package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// orderPublisher records every published row id in publish order and counts
// batch calls.
type orderPublisher struct {
	mu      sync.Mutex
	ids     []uuid.UUID
	batches int
}

func (p *orderPublisher) Publish(_ context.Context, e *outbox.Entry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ids = append(p.ids, e.ID)
	return nil
}

func (p *orderPublisher) PublishBatch(ctx context.Context, entries []*outbox.Entry) []error {
	p.mu.Lock()
	p.batches++
	p.mu.Unlock()
	errs := make([]error, len(entries))
	for i, e := range entries {
		errs[i] = p.Publish(ctx, e)
	}
	return errs
}

// sequentialPublisher publishes one row per call, as the Publish-only services
// do. The first attempt at a row named in failOnce returns that error; ids
// records the rows it published, in publish order.
type sequentialPublisher struct {
	mu       sync.Mutex
	failOnce map[uuid.UUID]error
	ids      []uuid.UUID
}

func (p *sequentialPublisher) Publish(_ context.Context, e *outbox.Entry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err, ok := p.failOnce[e.ID]; ok {
		delete(p.failOnce, e.ID)
		return err
	}
	p.ids = append(p.ids, e.ID)
	return nil
}

func createRows(t *testing.T, db *sqlx.DB, aggregate uuid.UUID, n int) []uuid.UUID {
	t.Helper()
	tx := db.MustBegin()
	repo := outbox.NewPostgresRepository(tx, testOutboxTable, newTestLogger())
	ids := make([]uuid.UUID, n)
	for i := range ids {
		e := &outbox.Entry{AggregateType: "pkg-outbox-test", AggregateID: aggregate, EventType: "e", Payload: []byte(`{}`), StreamName: "s"}
		require.NoError(t, repo.Create(context.Background(), e))
		ids[i] = e.ID
	}
	require.NoError(t, tx.Commit())
	return ids
}

// rowState reads one row's status and retry_count.
func rowState(t *testing.T, db *sqlx.DB, id uuid.UUID) (string, int) {
	t.Helper()
	var status string
	var retries int
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM `+testOutboxTable+` WHERE id = $1`, id).Scan(&status, &retries))
	return status, retries
}

func TestFIFO_SiblingsInOneBatchPublishTogetherInOrder(t *testing.T) {
	db := dbForTest(t)
	ids := createRows(t, db, uuid.New(), 3)
	pub := &orderPublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))
	assert.Equal(t, ids, pub.ids)
	assert.Equal(t, 1, pub.batches)
}

func TestFIFO_OlderSiblingLockedByAnotherRelayWithholdsYounger(t *testing.T) {
	db := dbForTest(t)
	agg := uuid.New()
	ids := createRows(t, db, agg, 2)
	other := createRows(t, db, uuid.New(), 1)

	peer := db.MustBegin()
	_, err := peer.Exec(`SELECT id FROM `+testOutboxTable+` WHERE id = $1 FOR UPDATE`, ids[0])
	require.NoError(t, err)

	pub := &orderPublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))
	assert.Equal(t, other, pub.ids, "the younger sibling waits while its elder is held elsewhere")

	require.NoError(t, peer.Rollback())
	require.NoError(t, p.ProcessBatch(context.Background()))
	assert.Equal(t, append(other, ids...), pub.ids)
}

func TestFIFO_OlderSiblingWaitingOutARetryWithholdsYounger(t *testing.T) {
	db := dbForTest(t)
	ids := createRows(t, db, uuid.New(), 2)
	_, err := db.Exec(`UPDATE `+testOutboxTable+` SET status = 'scheduled', next_attempt_at = now() + interval '1 hour' WHERE id = $1`, ids[0])
	require.NoError(t, err)
	pub := &orderPublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))
	assert.Empty(t, pub.ids)
}

func TestFIFO_OneAggregateBacklogDrainsInFullBatches(t *testing.T) {
	db := dbForTest(t)
	ids := createRows(t, db, uuid.New(), 300)
	pub := &orderPublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{BatchSize: 100})
	for i := 0; i < 3; i++ {
		require.NoError(t, p.ProcessBatch(context.Background()))
	}
	assert.Equal(t, ids, pub.ids)
	assert.Equal(t, 3, pub.batches, "one round trip per batch, not per row")
}

// A whole batch of rows queued behind an aggregate's row that waits out a
// retry must not hold up the rest of the table: the claim leaves those rows
// out, so the batch reaches the other aggregate's row.
func TestFIFO_AggregateWaitingOutARetryDoesNotStarveOthers(t *testing.T) {
	db := dbForTest(t)
	waiting := createRows(t, db, uuid.New(), 4)
	_, err := db.Exec(`UPDATE `+testOutboxTable+` SET status = 'scheduled', retry_count = 1, next_attempt_at = now() + interval '1 hour' WHERE id = $1`, waiting[0])
	require.NoError(t, err)
	other := createRows(t, db, uuid.New(), 1)

	pub := &orderPublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{BatchSize: 2})
	require.NoError(t, p.ProcessBatch(context.Background()))

	assert.Equal(t, other, pub.ids, "the other aggregate's row publishes while the waiting aggregate's rows stay queued")
	for _, id := range waiting[1:] {
		status, retries := rowState(t, db, id)
		assert.Equal(t, "pending", status)
		assert.Equal(t, 0, retries)
	}
}

// On the per-entry publish path a row that fails to publish stops the younger
// rows of its aggregate in the same batch: they are not sent and stay pending,
// untouched, so none reaches the stream before the failed row. Once the failed
// row's retry is due it publishes first and its younger rows follow in order.
func TestFIFO_FailedElderInBatchWithholdsYoungerSiblings(t *testing.T) {
	db := dbForTest(t)
	ids := createRows(t, db, uuid.New(), 3)
	other := createRows(t, db, uuid.New(), 1)

	pub := &sequentialPublisher{failOnce: map[uuid.UUID]error{ids[0]: errors.New("xadd: connection refused")}}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(),
		outbox.ProcessorConfig{RetryBaseDelay: time.Nanosecond, RetryMaxDelay: time.Nanosecond})
	require.NoError(t, p.ProcessBatch(context.Background()))

	assert.Equal(t, other, pub.ids, "only the other aggregate's row is published")
	status, retries := rowState(t, db, ids[0])
	assert.Equal(t, "scheduled", status)
	assert.Equal(t, 1, retries)
	for _, id := range ids[1:] {
		status, retries := rowState(t, db, id)
		assert.Equal(t, "pending", status, "a younger row is left as it was")
		assert.Equal(t, 0, retries, "a younger row is not charged a retry")
	}

	require.NoError(t, p.ProcessBatch(context.Background()))
	assert.Equal(t, append(append([]uuid.UUID{}, other...), ids...), pub.ids)
}

// Create stamps created_at itself, so rows publish in the order they were
// created whatever CreatedAt a caller put on the entry.
func TestFIFO_CreateOwnsTheOrderingKey(t *testing.T) {
	db := dbForTest(t)
	agg := uuid.New()
	callerTime := time.Now()
	tx := db.MustBegin()
	repo := outbox.NewPostgresRepository(tx, testOutboxTable, newTestLogger())
	var ids []uuid.UUID
	for _, createdAt := range []time.Time{callerTime, callerTime, callerTime.Add(-time.Hour)} {
		e := &outbox.Entry{AggregateType: "pkg-outbox-test", AggregateID: agg, EventType: "e", Payload: []byte(`{}`), StreamName: "s", CreatedAt: createdAt}
		require.NoError(t, repo.Create(context.Background(), e))
		ids = append(ids, e.ID)
	}
	require.NoError(t, tx.Commit())

	pub := &orderPublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))
	assert.Equal(t, ids, pub.ids)
}

// A drain runs batches back to back while they publish rows. A row held behind
// an elder that is dead-lettered in one batch publishes in the next batch of
// the same drain, followed by the dead-letter row, instead of waiting for the
// next wake.
func TestFIFO_DrainKeepsGoingWhileBatchesPublish(t *testing.T) {
	db := dbForTest(t)
	ids := createRows(t, db, uuid.New(), 2)
	other := createRows(t, db, uuid.New(), 1)

	pub := &sequentialPublisher{failOnce: map[uuid.UUID]error{ids[0]: fmt.Errorf("bad payload: %w", pkgevents.ErrPermanent)}}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{BatchSize: 10})
	p.DrainForTest(context.Background())

	var deadLetter uuid.UUID
	require.NoError(t, db.Get(&deadLetter, `SELECT id FROM `+testOutboxTable+` WHERE event_type = $1`, outbox.DeadLetterEventType))
	assert.Equal(t, []uuid.UUID{other[0], ids[1], deadLetter}, pub.ids)
}
