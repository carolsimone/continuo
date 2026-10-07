//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedProduction inserts a pending production row of run scheduleID, due at due.
func seedProduction(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID, due time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(`INSERT INTO deployments (id, task_id, schedule_id, job_params, next_attempt_at)
		VALUES ($1, $2, $3, '{}'::jsonb, $4)`, id, uuid.New(), scheduleID, due)
	require.NoError(t, err)
	return id
}

// seedCandidate inserts a pending validation row of release releaseID, due at due.
func seedCandidate(t *testing.T, db *sqlx.DB, releaseID string, due time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(`INSERT INTO deployments (id, task_id, schedule_id, job_params, mode, release_id, node_id, next_attempt_at)
		VALUES ($1, $2, $3, '{}'::jsonb, 'validation', $4, $5, $6)`,
		id, uuid.New(), uuid.New(), releaseID, uuid.NewString(), due)
	require.NoError(t, err)
	return id
}

func setStatus(t *testing.T, db *sqlx.DB, id uuid.UUID, status string, changedAgo time.Duration) {
	t.Helper()
	_, err := db.Exec(`UPDATE deployments SET status = $2, state_changed_at = NOW() - make_interval(secs => $3) WHERE id = $1`,
		id, status, changedAgo.Seconds())
	require.NoError(t, err)
}

func statusOf(t *testing.T, db *sqlx.DB, id uuid.UUID) string {
	t.Helper()
	var s string
	require.NoError(t, db.Get(&s, `SELECT status FROM deployments WHERE id = $1`, id))
	return s
}

func TestAdmission_ReserveNext_FairOrder(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	now := time.Now()
	runA, runB := uuid.New(), uuid.New()
	a1 := seedProduction(t, db, runA, now.Add(-10*time.Minute))
	a2 := seedProduction(t, db, runA, now.Add(-9*time.Minute))
	seedProduction(t, db, runA, now.Add(-8*time.Minute))
	b1 := seedProduction(t, db, runB, now.Add(-2*time.Minute))
	seedProduction(t, db, runB, now.Add(-1*time.Minute))
	r1 := seedCandidate(t, db, "rel-fair", now.Add(-5*time.Minute))
	r2 := seedCandidate(t, db, "rel-fair", now.Add(-4*time.Minute))
	notDue := seedProduction(t, db, uuid.New(), now.Add(time.Hour))

	ids, err := postgres.NewAdmissionRepository(db, testLogger()).ReserveNext(context.Background(), 4)
	require.NoError(t, err)
	// Lanes alternate (candidate, production); in production, run B takes its
	// first turn before run A's second, although A's rows are older.
	assert.ElementsMatch(t, []uuid.UUID{r1, a1, r2, b1}, ids)
	for _, id := range ids {
		assert.Equal(t, "reserved", statusOf(t, db, id))
	}
	assert.Equal(t, "pending", statusOf(t, db, a2))
	assert.Equal(t, "pending", statusOf(t, db, notDue), "a row whose next attempt is not due waits")
}

func TestAdmission_CountInFlight(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	for _, s := range []string{"pending", "blocked", "reserved", "starting", "running", "done", "failed", "skipped"} {
		setStatus(t, db, seedProduction(t, db, uuid.New(), time.Now()), s, 0)
	}
	n, err := postgres.NewAdmissionRepository(db, testLogger()).CountInFlight(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, n)
}

func TestAdmission_LockScope_SerialisesClaimers(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	tx1, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx1.Rollback() }()
	require.NoError(t, postgres.NewAdmissionRepository(tx1, testLogger()).LockScope(ctx, model.ScopeGlobal))

	locked := make(chan error, 1)
	go func() {
		tx2, err := db.BeginTxx(ctx, nil)
		if err != nil {
			locked <- err
			return
		}
		defer func() { _ = tx2.Rollback() }()
		locked <- postgres.NewAdmissionRepository(tx2, testLogger()).LockScope(ctx, model.ScopeGlobal)
	}()

	select {
	case <-locked:
		t.Fatal("a second claimer took the scope while the first held it")
	case <-time.After(500 * time.Millisecond):
	}
	require.NoError(t, tx1.Commit())
	select {
	case err := <-locked:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the second claimer never got the scope")
	}
}

func TestAdmission_LockScope_MissingScopeFails(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	err := postgres.NewAdmissionRepository(db, testLogger()).LockScope(context.Background(), "tenant-without-a-record")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no capacity record")
}

func TestAdmission_GetReserved_SkipsLockedAndNonReserved(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	id := seedProduction(t, db, uuid.New(), time.Now())

	_, err := postgres.NewAdmissionRepository(db, testLogger()).GetReserved(ctx, id)
	require.ErrorIs(t, err, sql.ErrNoRows, "a pending row is not launched")

	setStatus(t, db, id, "reserved", 0)
	tx1, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx1.Rollback() }()
	dep, err := postgres.NewAdmissionRepository(tx1, testLogger()).GetReserved(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, model.StatusReserved, dep.Status())

	_, err = postgres.NewAdmissionRepository(db, testLogger()).GetReserved(ctx, id)
	require.ErrorIs(t, err, sql.ErrNoRows, "a row another launcher holds is skipped")
}

func TestAdmission_ReturnStaleReservedAndListStarted(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	staleReserved := seedProduction(t, db, uuid.New(), time.Now())
	setStatus(t, db, staleReserved, "reserved", 5*time.Minute)
	setStatus(t, db, seedProduction(t, db, uuid.New(), time.Now()), "reserved", 10*time.Second)
	staleRunning := seedProduction(t, db, uuid.New(), time.Now())
	setStatus(t, db, staleRunning, "running", 5*time.Minute)
	staleStarting := seedProduction(t, db, uuid.New(), time.Now())
	setStatus(t, db, staleStarting, "starting", 5*time.Minute)
	setStatus(t, db, seedProduction(t, db, uuid.New(), time.Now()), "running", 10*time.Second)
	setStatus(t, db, seedProduction(t, db, uuid.New(), time.Now()), "done", 5*time.Minute)

	repo := postgres.NewAdmissionRepository(db, testLogger())
	returned, err := repo.ReturnStaleReserved(ctx, 2*time.Minute)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{staleReserved}, returned)
	assert.Equal(t, "pending", statusOf(t, db, staleReserved), "a stale reservation gives its slot back")

	started, err := repo.ListStarted(ctx, 2*time.Minute)
	require.NoError(t, err)
	var ids []uuid.UUID
	for _, d := range started {
		ids = append(ids, d.ID())
	}
	assert.ElementsMatch(t, []uuid.UUID{staleRunning, staleStarting}, ids)

	require.NoError(t, repo.TouchStateChanged(ctx, staleRunning))
	started, err = repo.ListStarted(ctx, 2*time.Minute)
	require.NoError(t, err)
	require.Len(t, started, 1, "touching a row restarts its staleness clock")
	assert.Equal(t, staleStarting, started[0].ID())

	dep, err := repo.GetStarted(ctx, staleRunning)
	require.NoError(t, err)
	assert.Equal(t, model.StatusRunning, dep.Status())
	_, err = repo.GetStarted(ctx, staleReserved)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestAdmission_ReturnStaleReserved_SkipsAReservationBeingLaunched(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	id := seedProduction(t, db, uuid.New(), time.Now())
	setStatus(t, db, id, "reserved", 5*time.Minute)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = postgres.NewAdmissionRepository(tx, testLogger()).GetReserved(ctx, id) // a launcher holds it
	require.NoError(t, err)

	returned, err := postgres.NewAdmissionRepository(db, testLogger()).ReturnStaleReserved(ctx, 2*time.Minute)
	require.NoError(t, err)
	assert.Empty(t, returned, "a reservation a launcher holds is not taken from it")
}

func TestRepo_GetByJobName_NewestRow(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	cmd := validCmd()
	cmd.JobName = "dbt-public-orders-r1"
	older := model.NewDeployment(cmd, nil, time.Now().Add(-time.Minute))
	newer := model.NewDeployment(cmd, nil, time.Now())
	require.NoError(t, repo.Add(ctx, older))
	require.NoError(t, repo.Add(ctx, newer))

	got, err := repo.GetByJobName(ctx, "dbt-public-orders-r1")
	require.NoError(t, err)
	assert.Equal(t, newer.ID(), got.ID())
	assert.Equal(t, "dbt-public-orders-r1", got.JobName())

	_, err = repo.GetByJobName(ctx, "no-such-job")
	require.ErrorIs(t, err, sql.ErrNoRows)
}
