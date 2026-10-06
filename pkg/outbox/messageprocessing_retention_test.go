package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests run against the same DB harness (dbForTest) as the outbox tests:
// continuo_execution holds both execution_outbox and message_processing, so the
// dedup retention DELETE can be exercised here without a second harness.
//
// DeleteOlderThan is table-wide by design, so on this shared database
// every test seeds only rows whose stream_name starts with testStreamPrefix,
// asserts each seeded row's fate by id, and bounds the pruner's returned count
// from below by the rows the test owns.

// pruneUntilIdle calls the pruner until a call deletes nothing, so every
// eligible aged row, however many other aged rows share the table, is gone
// afterwards. It returns the total number of rows deleted.
func pruneUntilIdle(t *testing.T, pruner messageprocessing.Pruner) int64 {
	t.Helper()
	var total int64
	for i := 0; i < 1000; i++ {
		n, err := pruner.DeleteOlderThan(context.Background(), 7*24*time.Hour, 100)
		require.NoError(t, err)
		if n == 0 {
			return total
		}
		total += n
	}
	t.Fatal("pruner still deleting after 1000 passes")
	return total
}

func seedDedupRow(t *testing.T, db *sqlx.DB, state string, updatedAt time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(
		`INSERT INTO message_processing (id, message_id, stream_name, state, payload, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, '{}'::jsonb, $5, $5)`,
		id, uuid.New().String(), testStreamPrefix+"-dedup", state, updatedAt,
	)
	require.NoError(t, err)
	return id
}

func dedupRowExists(t *testing.T, db *sqlx.DB, id uuid.UUID) bool {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM message_processing WHERE id=$1`, id).Scan(&n))
	return n > 0
}

// TestDeleteOlderThan_PurgesAgedRowsInAnyStateKeepsRecent verifies the
// dedup retention delete removes every row older than the window, whether it
// is completed, acked or still processing, and keeps recent rows. A committed
// row in any state records a handled message, so its age alone decides.
func TestDeleteOlderThan_PurgesAgedRowsInAnyStateKeepsRecent(t *testing.T) {
	db := dbForTest(t)
	pruner := messageprocessing.NewPruner(db, testOutboxTable, newTestLogger())

	aged := time.Now().Add(-10 * 24 * time.Hour)
	recent := time.Now().Add(-1 * time.Hour)
	oldCompleted := seedDedupRow(t, db, messageprocessing.StateCompleted, aged)
	oldAcked := seedDedupRow(t, db, messageprocessing.StateAcked, aged)
	oldProcessing := seedDedupRow(t, db, messageprocessing.StateProcessing, aged)
	recentCompleted := seedDedupRow(t, db, messageprocessing.StateCompleted, recent)
	recentProcessing := seedDedupRow(t, db, messageprocessing.StateProcessing, recent)

	n := pruneUntilIdle(t, pruner)
	assert.GreaterOrEqual(t, n, int64(3), "the three aged rows are deleted")

	assert.False(t, dedupRowExists(t, db, oldCompleted), "aged completed row purged")
	assert.False(t, dedupRowExists(t, db, oldAcked), "aged acked row purged")
	assert.False(t, dedupRowExists(t, db, oldProcessing), "aged processing row purged")
	assert.True(t, dedupRowExists(t, db, recentCompleted), "recent completed row kept")
	assert.True(t, dedupRowExists(t, db, recentProcessing), "recent processing row kept")
}

// seedOutboxRowReferencing inserts a minimal execution_outbox row with the
// given status that references message_processing_id — reproducing how a real
// outbox row is created in the same handler transaction that marks its
// triggering message complete.
func seedOutboxRowReferencing(t *testing.T, db *sqlx.DB, messageProcessingID uuid.UUID, status string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(
		`INSERT INTO `+testOutboxTable+`
			(id, message_processing_id, aggregate_type, aggregate_id, event_type, payload, stream_name, status, processed_at)
		 VALUES ($1, $2, 'run', gen_random_uuid(), 'test_event', '{}'::jsonb, $4, $3, CASE WHEN $3 = 'processed' THEN NOW() ELSE NULL END)`,
		id, messageProcessingID, status, testStreamPrefix+"-stream",
	)
	require.NoError(t, err)
	return id
}

// TestDeleteOlderThan_SkipsRowsStillReferencedByOutbox reproduces the
// production FK violation (the foreign key from execution_outbox's
// message_processing_id to message_processing, Postgres error 23503): a
// terminal, aged message_processing row that a dead-lettered ('failed')
// execution_outbox row still references must not
// be selected for deletion, since 'failed' rows are permanent (dead-letter
// retention excludes them, by design — see pkg/outbox.DeleteProcessedOlderThan)
// and would otherwise make the row un-purgeable forever while poisoning every
// batched DELETE that selects it.
func TestDeleteOlderThan_SkipsRowsStillReferencedByOutbox(t *testing.T) {
	db := dbForTest(t)
	pruner := messageprocessing.NewPruner(db, testOutboxTable, newTestLogger())

	referencedByFailed := seedDedupRow(t, db, messageprocessing.StateCompleted, time.Now().Add(-10*24*time.Hour))
	seedOutboxRowReferencing(t, db, referencedByFailed, "failed")

	unreferenced := seedDedupRow(t, db, messageprocessing.StateCompleted, time.Now().Add(-10*24*time.Hour))

	n := pruneUntilIdle(t, pruner)
	assert.GreaterOrEqual(t, n, int64(1), "the unreferenced row is purged")

	assert.True(t, dedupRowExists(t, db, referencedByFailed),
		"row still referenced by a dead-lettered outbox row is kept")
	assert.False(t, dedupRowExists(t, db, unreferenced), "unreferenced aged row purged")
}

// TestDeleteOlderThan_PurgesRowOnceOutboxReferenceIsGone confirms the
// exclusion is not permanent: once the referencing outbox row is removed (the
// normal case — outbox retention purges 'processed' rows past its own
// window), the message_processing row becomes eligible again on the next
// sweep.
func TestDeleteOlderThan_PurgesRowOnceOutboxReferenceIsGone(t *testing.T) {
	db := dbForTest(t)
	pruner := messageprocessing.NewPruner(db, testOutboxTable, newTestLogger())

	id := seedDedupRow(t, db, messageprocessing.StateCompleted, time.Now().Add(-10*24*time.Hour))
	outboxID := seedOutboxRowReferencing(t, db, id, "processed")

	pruneUntilIdle(t, pruner)
	assert.True(t, dedupRowExists(t, db, id), "still referenced, not purged")

	_, err := db.Exec(`DELETE FROM `+testOutboxTable+` WHERE id = $1`, outboxID)
	require.NoError(t, err)

	n := pruneUntilIdle(t, pruner)
	assert.GreaterOrEqual(t, n, int64(1), "purged once the outbox reference is gone")
	assert.False(t, dedupRowExists(t, db, id))
}
