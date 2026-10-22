//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// addAt adds a production deployment of jobName and moves it to status.
func addAt(t *testing.T, ctx context.Context, repo interface {
	Add(context.Context, *model.Deployment) error
	Save(context.Context, *model.Deployment) error
}, jobName string, status model.Status) *model.Deployment {
	t.Helper()
	cmd := validCmd()
	cmd.JobName = jobName
	dep := model.NewDeployment(cmd, nil, time.Now())
	require.NoError(t, repo.Add(ctx, dep))
	steps := map[model.Status][]func() error{
		model.StatusReserved:    {dep.Reserve},
		model.StatusStarting:    {dep.Reserve, func() error { return dep.MarkStarted(time.Now()) }},
		model.StatusRunning:     {dep.Reserve, func() error { return dep.MarkStarted(time.Now()) }, func() error { _, err := dep.MarkRunning(); return err }},
		model.StatusTerminating: {dep.Reserve, func() error { return dep.MarkStarted(time.Now()) }, func() error { return dep.BeginTermination("start_timeout", "") }},
		model.StatusDone:        {dep.Reserve, func() error { return dep.MarkStarted(time.Now()) }, func() error { return dep.Finish("ok", time.Now()) }},
	}
	for _, step := range steps[status] {
		require.NoError(t, step())
	}
	require.NoError(t, repo.Save(ctx, dep))
	return dep
}

func TestObserved_ListsTheJobsOfStartedDeployments(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	for _, s := range []model.Status{model.StatusPending, model.StatusReserved, model.StatusStarting,
		model.StatusRunning, model.StatusTerminating, model.StatusDone} {
		addAt(t, ctx, repo, "job-"+string(s), s)
	}

	released := addAt(t, ctx, repo, "job-released", model.StatusStarting)
	_, err := db.Exec(`UPDATE deployments SET status = 'done' WHERE id = $1`, released.ID())
	require.NoError(t, err)

	names, err := postgres.NewObservedDeployments(db, testLogger()).ListObservedJobNames(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"job-starting", "job-running", "job-terminating"}, names,
		"only started deployments are listed; a done row is not, with or without its outcome")
}

func TestObserved_GetByJobNameDoesNotWaitForALockedRow(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	dep := addAt(t, ctx, repo, "job-locked", model.StatusRunning)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = postgres.NewDeploymentsRepository(tx, testLogger()).LoadByJobName(ctx, "job-locked")
	require.NoError(t, err)

	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	got, err := postgres.NewObservedDeployments(db, testLogger()).GetByJobName(readCtx, "job-locked")
	require.NoError(t, err, "a read outside the transaction does not wait for its lock")
	assert.Equal(t, dep.ID(), got.ID())

	_, err = postgres.NewObservedDeployments(db, testLogger()).GetByJobName(ctx, "no-such-job")
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestObserved_StateAgeIsReadByTheDatabaseClock(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	dep := addAt(t, ctx, postgres.NewDeploymentsRepository(db, testLogger()), "job-aged", model.StatusStarting)
	_, err := db.Exec(`UPDATE deployments SET state_changed_at = NOW() - interval '3 minutes' WHERE id = $1`, dep.ID())
	require.NoError(t, err)

	age, err := postgres.NewObservedDeployments(db, testLogger()).StateAge(ctx, dep.ID())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, age, 3*time.Minute)
	assert.Less(t, age, 4*time.Minute)
}
