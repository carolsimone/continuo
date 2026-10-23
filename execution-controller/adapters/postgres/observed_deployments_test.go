//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
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

// A Job name ended undeployed when its newest row is failed and never
// created or adopted the Job: the creation failed for good, or the dispatch
// was refused. A row that is waiting, holding a slot or done, or that is
// followed by a newer row of the name that has not failed, does not count.
func TestObservedDeployments_EndedUndeployed(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	backoff := model.BackoffPolicy{Base: time.Second, Cap: time.Minute}
	at := time.Now().Add(-time.Hour)
	add := func(jobName string, created time.Time, status model.Status) {
		t.Helper()
		cmd := validCmd()
		cmd.JobName = jobName
		d := model.NewDeployment(cmd, nil, created)
		require.NoError(t, repo.Add(ctx, d))
		switch status {
		case model.StatusPending:
		case model.StatusReserved:
			require.NoError(t, d.Reserve())
		case model.StatusFailed:
			require.NoError(t, d.Reserve())
			_, err := d.RegisterFailure(created, true, "the API refused the Job", backoff)
			require.NoError(t, err)
		case model.StatusDone:
			require.NoError(t, d.Reserve())
			require.NoError(t, d.MarkStarted(created))
			require.NoError(t, d.Finish("ok", created))
		}
		require.NoError(t, repo.Save(ctx, d))
	}
	add("job-failed", at, model.StatusFailed)
	add("job-pending", at, model.StatusPending)
	add("job-reserved", at, model.StatusReserved)
	add("job-done", at, model.StatusDone)
	add("job-requeued", at, model.StatusFailed)
	add("job-requeued", at.Add(time.Minute), model.StatusPending)
	add("job-failed-again", at, model.StatusDone)
	add("job-failed-again", at.Add(time.Minute), model.StatusFailed)
	refusedCmd := validCmd()
	refusedCmd.JobName = "job-refused"
	refused := model.NewRefusedDeployment(refusedCmd, nil, pkgmodel.FailureReasonAttemptLost, "lost", at)
	refused.RecordReport(refused.ReportOf(pkgmodel.AttemptStatusFailed, "lost"))
	added, err := repo.AddAttempt(ctx, refused)
	require.NoError(t, err)
	require.True(t, added)

	observed := postgres.NewObservedDeployments(db, testLogger())
	for name, want := range map[string]bool{
		"job-failed": true, "job-refused": true, "job-failed-again": true,
		"job-pending": false, "job-reserved": false, "job-done": false, "job-requeued": false, "no-such-job": false,
	} {
		got, err := observed.EndedUndeployed(ctx, name)
		require.NoError(t, err)
		assert.Equal(t, want, got, name)
	}
}

// A deployment that never created or adopted a Job, though it carries the
// Job's name — a refused dispatch, an attempt whose creation failed on the
// name, one not created yet — never stands for the Job: both lookups by Job
// name return the deployment whose Job it is, so the observer keeps following
// it.
func TestJobNameLookups_ReturnTheDeploymentThatOwnsTheJob(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	owner := addAt(t, ctx, repo, "job-owned", model.StatusRunning)

	later := time.Now().Add(time.Minute)
	ofAnotherTask := func() command.DeployTask {
		cmd := validCmd()
		cmd.JobName = "job-owned"
		return cmd
	}
	for _, reason := range []pkgmodel.FailureReason{pkgmodel.FailureReasonInvalidNodeType, pkgmodel.FailureReasonAttemptLost} {
		refused := model.NewRefusedDeployment(ofAnotherTask(), nil, reason, string(reason), later)
		refused.RecordReport(refused.ReportOf(pkgmodel.AttemptStatusFailed, string(reason)))
		added, err := repo.AddAttempt(ctx, refused)
		require.NoError(t, err)
		require.True(t, added)
	}
	collided := model.NewDeployment(ofAnotherTask(), nil, later)
	require.NoError(t, repo.Add(ctx, collided))
	require.NoError(t, collided.Reserve())
	_, err := collided.RegisterFailure(later, true, "a Job of the name exists for another task or attempt",
		model.BackoffPolicy{Base: time.Second, Cap: time.Minute})
	require.NoError(t, err)
	collided.Observe(model.AttemptFacts{Reason: pkgmodel.FailureReasonJobNameCollision})
	require.NoError(t, repo.Save(ctx, collided))
	require.NoError(t, repo.Add(ctx, model.NewDeployment(ofAnotherTask(), nil, later)))

	got, err := repo.LoadByJobName(ctx, "job-owned")
	require.NoError(t, err)
	assert.Equal(t, owner.ID(), got.ID(), "the locked lookup returns the Job's owner")
	got, err = postgres.NewObservedDeployments(db, testLogger()).GetByJobName(ctx, "job-owned")
	require.NoError(t, err)
	assert.Equal(t, owner.ID(), got.ID(), "the observer's lookup returns the Job's owner")
	assert.True(t, got.AwaitingResult())
}
