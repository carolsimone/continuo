//go:build integration

package deployer_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/execution-controller/service/deployer"
	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeInventory struct {
	jobs  map[string]ports.JobState
	calls int
}

func (f *fakeInventory) ListJobs(context.Context) (map[string]ports.JobState, error) {
	f.calls++
	return f.jobs, nil
}

func newTestReconciler(db *sqlx.DB, inv ports.JobInventory) *deployer.Reconciler {
	adm := func(exec outbox.Executor) repository.AdmissionRepository {
		return postgres.NewAdmissionRepository(exec, testLogger())
	}
	repo := func(exec outbox.Executor) repository.DeploymentRepository {
		return postgres.NewDeploymentsRepository(exec, testLogger())
	}
	return deployer.NewReconciler(db, inv, adm, repo, testLogger(), deployer.ReconcilerConfig{})
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

	require.NoError(t, newTestReconciler(db, &fakeInventory{}).ReconcileOnce(context.Background()))

	assert.Equal(t, "pending", countStatusOf(t, db, stale), "a reservation whose launcher is gone gives its slot back; the claim re-admits it")
	assert.Equal(t, "reserved", countStatusOf(t, db, fresh), "a fresh reservation belongs to a live launcher")
	assert.Equal(t, 0, outboxCountByType(t, db, "check_delayed"), "the reconciler starts no work")
}

func TestReconciler_MissingJobGetsOneCheckTicket(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	age(t, db, id, "running", 5*time.Minute)
	r := newTestReconciler(db, &fakeInventory{jobs: map[string]ports.JobState{}})

	require.NoError(t, r.ReconcileOnce(context.Background()))
	assert.Equal(t, 1, outboxCountByType(t, db, "check_delayed"), "the status check reports the vanished Job")
	assert.Equal(t, "running", countStatusOf(t, db, id), "the reconciler does not report by itself")

	require.NoError(t, r.ReconcileOnce(context.Background()))
	assert.Equal(t, 1, outboxCountByType(t, db, "check_delayed"), "one ticket per grace period, not per pass")
}

func TestReconciler_FinishedUnobservedJobReleasesTheSlotSilently(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	old, recent, active := seedJob(t, db, 3, 0), seedJob(t, db, 3, 0), seedJob(t, db, 3, 0)
	for _, id := range []uuid.UUID{old, recent, active} {
		age(t, db, id, "running", 5*time.Minute)
	}
	inv := &fakeInventory{jobs: map[string]ports.JobState{
		jobNameOf(t, db, old):    {Finished: true, FinishedAt: time.Now().Add(-5 * time.Minute)},
		jobNameOf(t, db, recent): {Finished: true, FinishedAt: time.Now().Add(-20 * time.Second)},
		jobNameOf(t, db, active): {},
	}}

	require.NoError(t, newTestReconciler(db, inv).ReconcileOnce(context.Background()))

	var status string
	var outcome *string
	require.NoError(t, db.QueryRow(`SELECT status, outcome FROM deployments WHERE id = $1`, old).Scan(&status, &outcome))
	assert.Equal(t, "done", status)
	assert.Nil(t, outcome, "the outcome stays unset so a redriven status check can still report it")
	assert.Equal(t, "running", countStatusOf(t, db, recent), "its status check may still be on the way")
	assert.Equal(t, "running", countStatusOf(t, db, active))
	assert.Equal(t, 0, outboxCountByType(t, db, "check_delayed"))
	assert.Equal(t, 0, outboxCountByType(t, db, "task_status_updated"))
}

func TestReconciler_SkipsTheJobListingWhenNothingIsStarted(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	inv := &fakeInventory{}
	require.NoError(t, newTestReconciler(db, inv).ReconcileOnce(context.Background()))
	assert.Equal(t, 0, inv.calls)
}
