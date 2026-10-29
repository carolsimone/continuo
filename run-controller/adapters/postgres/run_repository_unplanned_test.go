package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	repository "github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ListUnplannedActive names the pending and running runs of every tenant whose
// planned_at is NULL, each with its own tenant, oldest first, and no planned
// or finished run.
func TestRunRepository_ListUnplannedActive(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant, other := "unplanned-"+uuid.NewString(), "unplanned-"+uuid.NewString()
	created := time.Now().Add(-time.Hour)
	insert := func(tenant, status string, planned bool) repository.RunRef {
		id := uuid.New()
		var plannedAt any
		if planned {
			plannedAt = time.Now()
		}
		created = created.Add(time.Second)
		_, err := db.Exec(`INSERT INTO runs (tenant_id, run_id, schedule_name, status, planned_at, created_at) VALUES ($5, $1, $2, $3, $4, $6)`,
			id, "unplanned-"+id.String()[:8], status, plannedAt, tenant, created)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, id) })
		return repository.RunRef{TenantID: tenant, RunID: id}
	}
	pending, running := insert(tenant, "pending", false), insert(tenant, "running", false)
	plannedRunning, finished := insert(tenant, "running", true), insert(tenant, "failed", false)
	theirs := insert(other, "running", false)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	got, err := postgres.NewRunRepository(db, discardLogger()).WithTx(tx).ListUnplannedActive(ctx)
	require.NoError(t, err)

	ours := keep(got, func(r repository.RunRef) string { return r.TenantID }, []string{tenant, other})
	assert.Equal(t, []repository.RunRef{pending, running, theirs}, ours, "every tenant's unplanned runs, oldest first")
	assert.NotContains(t, got, plannedRunning)
	assert.NotContains(t, got, finished)
}
