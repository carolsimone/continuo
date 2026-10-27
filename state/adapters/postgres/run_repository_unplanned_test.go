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

// ListUnplannedActive names the tenant's pending and running runs whose planned_at is
// NULL, and no planned or finished run.
func TestRunRepository_ListUnplannedActive(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := "unplanned-" + uuid.NewString()
	insert := func(status string, planned bool) uuid.UUID {
		id := uuid.New()
		var plannedAt any
		if planned {
			plannedAt = time.Now()
		}
		_, err := db.Exec(`INSERT INTO runs (tenant_id, run_id, schedule_name, status, planned_at) VALUES ($5, $1, $2, $3, $4)`,
			id, "unplanned-"+id.String()[:8], status, plannedAt, tenant)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, id) })
		return id
	}
	pending, running := insert("pending", false), insert("running", false)
	plannedRunning, finished := insert("running", true), insert("failed", false)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	runs := postgres.NewRunRepository(tx, postgres.NewRunTableRepository(db, discardLogger()))
	got, err := runs.ListUnplannedActive(ctx, tenant)
	require.NoError(t, err)

	assert.Equal(t, []uuid.UUID{pending, running}, got)
	assert.NotContains(t, got, plannedRunning)
	assert.NotContains(t, got, finished)
	theirs, err := runs.ListUnplannedActive(ctx, "unplanned-"+uuid.NewString())
	require.NoError(t, err)
	assert.Empty(t, theirs, "another tenant has none of the tenant's runs")
}
