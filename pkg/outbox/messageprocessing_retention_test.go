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

// This test runs against the same DB harness (dbForTest) as the outbox tests:
// the orchestrator migration set creates message_processing, so the dedup
// retention DELETE can be exercised here without a second harness.

func seedDedupRow(t *testing.T, db *sqlx.DB, state string, updatedAt time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(
		`INSERT INTO message_processing (id, message_id, stream_name, state, payload, created_at, updated_at)
		 VALUES ($1, $2, 'x:v1', $3, '{}'::jsonb, $4, $4)`,
		id, uuid.New().String(), state, updatedAt,
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

// TestDeleteTerminalOlderThan_PurgesAgedTerminalKeepsRest verifies the dedup
// retention delete removes only terminal rows (completed/acked) older than the
// window, leaving recent terminal rows and any still-'processing' row in place.
func TestDeleteTerminalOlderThan_PurgesAgedTerminalKeepsRest(t *testing.T) {
	db := dbForTest(t)
	defer db.Exec(`TRUNCATE message_processing CASCADE`)
	pruner := messageprocessing.NewPruner(db, testOutboxTable, newTestLogger())

	oldCompleted := seedDedupRow(t, db, "completed", time.Now().Add(-10*24*time.Hour))
	oldAcked := seedDedupRow(t, db, "acked", time.Now().Add(-10*24*time.Hour))
	recentCompleted := seedDedupRow(t, db, "completed", time.Now().Add(-1*time.Hour))
	oldProcessing := seedDedupRow(t, db, "processing", time.Now().Add(-10*24*time.Hour))

	n, err := pruner.DeleteTerminalOlderThan(context.Background(), 7*24*time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n, "both aged terminal rows deleted")

	assert.False(t, dedupRowExists(t, db, oldCompleted), "aged completed purged")
	assert.False(t, dedupRowExists(t, db, oldAcked), "aged acked purged")
	assert.True(t, dedupRowExists(t, db, recentCompleted), "recent terminal kept")
	assert.True(t, dedupRowExists(t, db, oldProcessing), "in-flight processing row never purged")
}

// seedOutboxRowReferencing inserts a minimal orchestrator_outbox row with the
// given status that references message_processing_id — reproducing how a real
// outbox row is created in the same handler transaction that marks its
// triggering message complete.
func seedOutboxRowReferencing(t *testing.T, db *sqlx.DB, messageProcessingID uuid.UUID, status string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(
		`INSERT INTO orchestrator_outbox
			(id, message_processing_id, aggregate_type, aggregate_id, event_type, payload, stream_name, status, processed_at)
		 VALUES ($1, $2, 'run', gen_random_uuid(), 'test_event', '{}'::jsonb, 'x:v1', $3, CASE WHEN $3 = 'processed' THEN NOW() ELSE NULL END)`,
		id, messageProcessingID, status,
	)
	require.NoError(t, err)
	return id
}

// TestDeleteTerminalOlderThan_SkipsRowsStillReferencedByOutbox reproduces the
// production FK violation (orchestrator_outbox_message_processing_id_fkey,
// Postgres error 23503): a terminal, aged message_processing row that a
// dead-lettered ('failed') orchestrator_outbox row still references must not
// be selected for deletion, since 'failed' rows are permanent (dead-letter
// retention excludes them, by design — see pkg/outbox.DeleteProcessedOlderThan)
// and would otherwise make the row un-purgeable forever while poisoning every
// batched DELETE that selects it.
func TestDeleteTerminalOlderThan_SkipsRowsStillReferencedByOutbox(t *testing.T) {
	db := dbForTest(t)
	defer db.Exec(`TRUNCATE orchestrator_outbox, message_processing CASCADE`)
	pruner := messageprocessing.NewPruner(db, testOutboxTable, newTestLogger())

	referencedByFailed := seedDedupRow(t, db, "completed", time.Now().Add(-10*24*time.Hour))
	seedOutboxRowReferencing(t, db, referencedByFailed, "failed")

	unreferenced := seedDedupRow(t, db, "completed", time.Now().Add(-10*24*time.Hour))

	n, err := pruner.DeleteTerminalOlderThan(context.Background(), 7*24*time.Hour, 100)
	require.NoError(t, err, "delete must not fail with a foreign key violation")
	assert.Equal(t, int64(1), n, "only the unreferenced row is purged")

	assert.True(t, dedupRowExists(t, db, referencedByFailed),
		"row still referenced by a dead-lettered outbox row is kept")
	assert.False(t, dedupRowExists(t, db, unreferenced), "unreferenced aged row purged")
}

// TestDeleteTerminalOlderThan_PurgesRowOnceOutboxReferenceIsGone confirms the
// exclusion is not permanent: once the referencing outbox row is removed (the
// normal case — outbox retention purges 'processed' rows past its own
// window), the message_processing row becomes eligible again on the next
// sweep.
func TestDeleteTerminalOlderThan_PurgesRowOnceOutboxReferenceIsGone(t *testing.T) {
	db := dbForTest(t)
	defer db.Exec(`TRUNCATE orchestrator_outbox, message_processing CASCADE`)
	pruner := messageprocessing.NewPruner(db, testOutboxTable, newTestLogger())

	id := seedDedupRow(t, db, "completed", time.Now().Add(-10*24*time.Hour))
	outboxID := seedOutboxRowReferencing(t, db, id, "processed")

	n, err := pruner.DeleteTerminalOlderThan(context.Background(), 7*24*time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n, "still referenced, not purged")
	assert.True(t, dedupRowExists(t, db, id))

	_, err = db.Exec(`DELETE FROM orchestrator_outbox WHERE id = $1`, outboxID)
	require.NoError(t, err)

	n, err = pruner.DeleteTerminalOlderThan(context.Background(), 7*24*time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "purged once the outbox reference is gone")
	assert.False(t, dedupRowExists(t, db, id))
}
