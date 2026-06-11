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
	pruner := messageprocessing.NewPruner(db, newTestLogger())

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
