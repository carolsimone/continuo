package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ListUnplannedActive names the pending and running runs whose planned_at is
// NULL, and no planned or finished run.
func TestRunRepository_ListUnplannedActive(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	insert := func(status string, planned bool) uuid.UUID {
		id := uuid.New()
		var plannedAt any
		if planned {
			plannedAt = time.Now()
		}
		_, err := db.Exec(`INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, planned_at) VALUES ($1, $2, $3, $4)`,
			id, "unplanned-"+id.String()[:8], status, plannedAt)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, id) })
		return id
	}
	pending, running := insert("pending", false), insert("running", false)
	plannedRunning, finished := insert("running", true), insert("failed", false)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	got, err := postgres.NewRunRepository(tx, postgres.NewSchedulerTrackerRepository(db, discardLogger())).ListUnplannedActive(ctx)
	require.NoError(t, err)

	assert.Subset(t, got, []uuid.UUID{pending, running})
	assert.NotContains(t, got, plannedRunning)
	assert.NotContains(t, got, finished)
}
