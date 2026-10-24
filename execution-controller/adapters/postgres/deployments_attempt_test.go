//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Each attempt of a task is accepted once, whatever deployment carries it.
func TestRepo_AddAttempt_AcceptsEachAttemptOnce(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	cmd := validCmd()
	cmd.Attempt = 1

	added, err := repo.AddAttempt(ctx, model.NewDeployment(cmd, nil, time.Now()))
	require.NoError(t, err)
	assert.True(t, added)
	added, err = repo.AddAttempt(ctx, model.NewDeployment(cmd, nil, time.Now()))
	require.NoError(t, err)
	assert.False(t, added, "the attempt's row exists")

	next := cmd
	next.Attempt = 2
	added, err = repo.AddAttempt(ctx, model.NewDeployment(next, nil, time.Now()))
	require.NoError(t, err)
	assert.True(t, added, "another attempt of the task is its own")
	var n int
	require.NoError(t, db.Get(&n, `SELECT count(*) FROM deployments WHERE task_id = $1`, cmd.TaskID))
	assert.Equal(t, 2, n)
}

// A deployment's attempt is read from its job_params; a release leg's row has
// none.
func TestRepo_TheAttemptColumnIsTheCommandsAttempt(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	cmd := validCmd()
	cmd.Attempt = 3
	_, err := repo.AddAttempt(ctx, model.NewDeployment(cmd, nil, time.Now()))
	require.NoError(t, err)
	leg := model.NewValidationDeployment(validValidationCmd("rel-"+uuid.NewString(), "node-1"), nil, time.Now(), false)
	require.NoError(t, repo.Add(ctx, leg))

	var attempt int
	require.NoError(t, db.Get(&attempt, `SELECT attempt FROM deployments WHERE task_id = $1`, cmd.TaskID))
	assert.Equal(t, 3, attempt)
	var legAttempt sql.NullInt32
	require.NoError(t, db.Get(&legAttempt, `SELECT attempt FROM deployments WHERE id = $1`, leg.ID()))
	assert.False(t, legAttempt.Valid)
}

// A task's attempt is found by its number, an attempt it does not hold is
// reported not found, and a later attempt is seen.
func TestRepo_LoadByTaskAttemptAndHasAttemptAfter(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	cmd := validCmd()
	cmd.Attempt = 1
	d := model.NewDeployment(cmd, nil, time.Now())
	_, err := repo.AddAttempt(ctx, d)
	require.NoError(t, err)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck
	got, found, err := postgres.NewDeploymentsRepository(tx, testLogger()).LoadByTaskAttempt(ctx, cmd.TaskID, 1)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, d.ID(), got.ID())
	got, found, err = postgres.NewDeploymentsRepository(tx, testLogger()).LoadByTaskAttempt(ctx, cmd.TaskID, 0)
	require.NoError(t, err, "an attempt the task does not hold is not an error")
	assert.False(t, found)
	assert.Nil(t, got)

	for attempt, want := range map[int]bool{0: true, 1: false, 2: false} {
		later, err := repo.HasAttemptAfter(ctx, cmd.TaskID, attempt)
		require.NoError(t, err)
		assert.Equal(t, want, later, "a later attempt than %d", attempt)
	}
}

// The report last emitted for an attempt survives a save and a load.
func TestRepo_TheLastReportRoundTrips(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	cmd := validCmd()
	accepted := time.Now().UTC().Truncate(time.Microsecond)
	d := model.NewDeployment(cmd, nil, accepted)
	d.RecordReport(d.ReportOf(pkgmodel.AttemptStatusQueued, ""))
	_, err := repo.AddAttempt(ctx, d)
	require.NoError(t, err)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck
	txRepo := postgres.NewDeploymentsRepository(tx, testLogger())
	got, found, err := txRepo.LoadByTaskAttempt(ctx, cmd.TaskID, 0)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, got.LastReport())
	assert.Equal(t, pkgmodel.AttemptStatusQueued, got.LastReport().Status)
	assert.True(t, accepted.Equal(*got.LastReport().QueuedAt))

	code := int32(1)
	got.Observe(model.AttemptFacts{ExitCode: &code, Reason: pkgmodel.FailureReasonExitCode})
	got.RecordReport(got.ReportOf(pkgmodel.AttemptStatusFailed, "boom"))
	require.NoError(t, txRepo.Save(ctx, got))
	var class string
	require.NoError(t, tx.Get(&class, `SELECT last_report->>'failure_class' FROM deployments WHERE id = $1`, got.ID()))
	assert.Equal(t, "task", class, "the stored report is the payload it was emitted as")
}

// A stopped attempt is stored done with its outcome, and a started one that was
// asked to stop keeps the request with its facts.
func TestRepo_AStoppedAttemptRoundTrips(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)

	cmd := validCmd()
	cmd.Attempt = 1
	added, err := repo.AddAttempt(ctx, model.NewStoppedDeployment(cmd, nil, now))
	require.NoError(t, err)
	require.True(t, added)
	got, found, err := repo.LoadByTaskAttempt(ctx, cmd.TaskID, 1)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, model.StatusDone, got.Status())
	assert.Equal(t, "stopped", got.Outcome())
	require.NotNil(t, got.Facts().StopRequestedAt)
	assert.True(t, now.Equal(*got.Facts().StopRequestedAt))

	running := validCmd()
	running.Attempt = 1
	d := model.NewAdoptedDeployment(running, nil, now)
	_, err = repo.AddAttempt(ctx, d)
	require.NoError(t, err)
	effect, err := d.Stop(now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, model.StopTerminate, effect)
	require.NoError(t, repo.Save(ctx, d))
	got, _, err = repo.LoadByTaskAttempt(ctx, running.TaskID, 1)
	require.NoError(t, err)
	assert.Equal(t, model.StatusTerminating, got.Status())
	require.NotNil(t, got.Facts().StopRequestedAt)
	assert.True(t, now.Add(time.Minute).Equal(*got.Facts().StopRequestedAt))
}
