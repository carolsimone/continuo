//go:build integration

package deployer_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/execution-controller/service/deployer"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestReconciler(db *sqlx.DB) *deployer.Reconciler {
	adm := func(exec outbox.Executor) repository.AdmissionRepository {
		return postgres.NewAdmissionRepository(exec, testLogger())
	}
	return deployer.NewReconciler(db, adm, testLogger(), deployer.ReconcilerConfig{})
}

// age moves id to status, last changed ago.
func age(t *testing.T, db *sqlx.DB, id uuid.UUID, status string, ago time.Duration) {
	t.Helper()
	_, err := db.Exec(`UPDATE deployments SET status = $2, state_changed_at = NOW() - make_interval(secs => $3) WHERE id = $1`,
		id, status, ago.Seconds())
	require.NoError(t, err)
}

func jobNameOf(t *testing.T, db *sqlx.DB, id uuid.UUID) string {
	t.Helper()
	var name string
	require.NoError(t, db.Get(&name, `SELECT job_name FROM deployments WHERE id = $1`, id))
	return name
}

func TestReconciler_ReturnsStaleReservationsToPending(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	stale, fresh := seedJob(t, db, 3, 0), seedJob(t, db, 3, 0)
	age(t, db, stale, "reserved", 5*time.Minute)
	age(t, db, fresh, "reserved", 10*time.Second)

	require.NoError(t, newTestReconciler(db).ReconcileOnce(context.Background()))

	assert.Equal(t, "pending", countStatusOf(t, db, stale), "a reservation whose launcher is gone gives its slot back; the claim re-admits it")
	assert.Equal(t, "reserved", countStatusOf(t, db, fresh), "a fresh reservation belongs to a live launcher")
	assert.Equal(t, 0, outboxCountByType(t, db, "task_status_updated"), "the reconciler reports nothing")
}
