package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedProcessedRow inserts a processed outbox row with an explicit processed_at,
// so a test can place a row inside or outside the retention window.
func seedProcessedRow(t *testing.T, db *sqlx.DB, processedAt time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(
		`INSERT INTO orchestrator_outbox
		   (id, aggregate_type, aggregate_id, event_type, payload, stream_name, status, processed_at)
		 VALUES ($1, 'task', $2, 'x', '{}'::jsonb, 'x:v1', 'processed', $3)`,
		id, uuid.New(), processedAt,
	)
	require.NoError(t, err)
	return id
}

func rowExists(t *testing.T, db *sqlx.DB, id uuid.UUID) bool {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM orchestrator_outbox WHERE id=$1`, id).Scan(&n))
	return n > 0
}

// TestDeleteProcessedOlderThan_PurgesOldKeepsRecentAndPending verifies the
// outbox retention delete removes only processed rows older than the window,
// leaving recent-processed and pending rows in place.
func TestDeleteProcessedOlderThan_PurgesOldKeepsRecentAndPending(t *testing.T) {
	db := dbForTest(t)
	prune := outbox.OutboxRetentionTarget(db, testOutboxTable, newTestLogger()).Prune

	old := seedProcessedRow(t, db, time.Now().Add(-10*24*time.Hour)) // outside a 7-day window
	recent := seedProcessedRow(t, db, time.Now().Add(-1*time.Hour))  // inside the window
	pendingID := seedRow(t, db, 3)                                   // never processed

	n, err := prune(context.Background(), 7*24*time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "exactly the aged processed row is deleted")

	assert.False(t, rowExists(t, db, old), "aged processed row purged")
	assert.True(t, rowExists(t, db, recent), "recent processed row kept")
	assert.True(t, rowExists(t, db, pendingID), "pending row kept")
}

// TestDeleteProcessedOlderThan_RespectsLimit verifies the delete is bounded by
// limit so a large backlog drains over multiple bounded statements.
func TestDeleteProcessedOlderThan_RespectsLimit(t *testing.T) {
	db := dbForTest(t)
	prune := outbox.OutboxRetentionTarget(db, testOutboxTable, newTestLogger()).Prune

	for i := 0; i < 5; i++ {
		seedProcessedRow(t, db, time.Now().Add(-10*24*time.Hour))
	}

	n, err := prune(context.Background(), 7*24*time.Hour, 2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n, "delete is capped at the limit")

	var remaining int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM orchestrator_outbox WHERE status='processed'`).Scan(&remaining))
	assert.Equal(t, 3, remaining, "remaining aged rows await the next bounded pass")
}
