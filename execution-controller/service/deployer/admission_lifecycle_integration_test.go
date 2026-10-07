//go:build integration

package deployer_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	"github.com/carolsimone/continuo/execution-controller/service/outcomes"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	"github.com/carolsimone/continuo/execution-controller/test/fakes"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runInUoW runs fn inside one transaction and commits it, failing the test on
// any error.
func runInUoW(t *testing.T, db *sqlx.DB, fn func(u uow.UnitOfWork) error) {
	t.Helper()
	ctx := context.Background()
	u := postgres.NewPostgresUnitOfWork(db, slog.Default())
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, fn(u))
	require.NoError(t, u.Commit())
}

// loadDeployment reads the Job name of the deployments row id and loads the
// aggregate the job-status handler would load for that Job.
func loadDeployment(t *testing.T, db *sqlx.DB, id uuid.UUID) *model.Deployment {
	t.Helper()
	var jobName string
	require.NoError(t, db.Get(&jobName, `SELECT job_params->>'job_name' FROM deployments WHERE id = $1`, id))
	var dep *model.Deployment
	runInUoW(t, db, func(u uow.UnitOfWork) error {
		var err error
		dep, err = u.DeploymentsRepo().GetByJobName(context.Background(), jobName)
		return err
	})
	return dep
}

// newJobHandlerObserving builds the job-status handler over a Kubernetes
// client that reports every Job as status and carries no Job metadata, the way
// a production Job without labels is observed.
func newJobHandlerObserving(db *sqlx.DB, status model.JobStatus) *handlers.JobStatusHandler {
	observer := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(context.Context, string, string) (*model.JobResult, error) {
			return &model.JobResult{Status: status}, nil
		},
		GetJobMetaFunc: func(context.Context, string, string) (map[string]string, map[string]string, error) {
			return map[string]string{}, map[string]string{}, nil
		},
	}
	return handlers.NewJobStatusHandler(observer, &fakes.FakeLogUploader{},
		&handlers.JobStatusConfig{K8sNamespace: "default", CheckDelaySeconds: 1, LogTailLines: 5, ErrorMessageMaxLen: 100},
		postgres.NewCancelledSchedulesRepository(db), outcomes.NewRecorder(slog.Default()), slog.Default())
}

func rowStatusAndOutcome(t *testing.T, db *sqlx.DB, id uuid.UUID) (status, outcome string) {
	t.Helper()
	require.NoError(t, db.QueryRow(`SELECT status, COALESCE(outcome, '') FROM deployments WHERE id = $1`, id).Scan(&status, &outcome))
	return status, outcome
}

// TestAdmission_DuplicateCheckAfterTerminal_ReportsOnce launches a production
// deployment, observes its Job succeed, then delivers a second identical status
// check: the row is done with outcome ok, its slot is free, and the second check
// adds no outbox row.
func TestAdmission_DuplicateCheckAfterTerminal_ReportsOnce(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	seeded := enqueueProductionDeployment(t, db, "job-dup-ok")
	id := seeded.ID()
	require.NoError(t, newTestDispatcher(db, &fakeDeployer{}, 5).ProcessBatch(ctx))
	require.Equal(t, "starting", countStatusOf(t, db, id))

	dep := loadDeployment(t, db, id)
	require.False(t, dep.HasOutcome())
	cmd := checkCommandForTask(t, db, dep)
	handler := newJobHandlerObserving(db, model.JobStatusSucceeded)

	runInUoW(t, db, func(u uow.UnitOfWork) error { return handler.Handle(ctx, u, cmd, uuid.Nil) })
	status, outcome := rowStatusAndOutcome(t, db, id)
	assert.Equal(t, "done", status)
	assert.Equal(t, "ok", outcome)
	reported := outboxCountByType(t, db, event.EventTypeTaskStatusUpdated)
	recorded := outboxCountByType(t, db, event.EventTypeTaskExecutionRecorded)
	require.Equal(t, 1, reported)

	runInUoW(t, db, func(u uow.UnitOfWork) error { return handler.Handle(ctx, u, cmd, uuid.Nil) })
	assert.Equal(t, reported, outboxCountByType(t, db, event.EventTypeTaskStatusUpdated), "the second check reports nothing")
	assert.Equal(t, recorded, outboxCountByType(t, db, event.EventTypeTaskExecutionRecorded))
}

// TestAdmission_DuplicateCheckAfterFailure_QueuesOneRetry delivers the same
// status check twice for a failed Job with retry budget left: one FAILED
// announcement and one retry deployment result.
func TestAdmission_DuplicateCheckAfterFailure_QueuesOneRetry(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	seeded := enqueueProductionDeployment(t, db, "job-dup-fail")
	id := seeded.ID()
	require.NoError(t, newTestDispatcher(db, &fakeDeployer{}, 5).ProcessBatch(ctx))

	dep := loadDeployment(t, db, id)
	cmd := checkCommandForTask(t, db, dep)
	handler := newJobHandlerObserving(db, model.JobStatusFailed)

	for range 2 {
		runInUoW(t, db, func(u uow.UnitOfWork) error { return handler.Handle(ctx, u, cmd, uuid.Nil) })
	}

	status, outcome := rowStatusAndOutcome(t, db, id)
	assert.Equal(t, "done", status)
	assert.Equal(t, "failed", outcome)
	assert.Equal(t, 1, outboxCountByType(t, db, event.EventTypeTaskStatusUpdated), "one FAILED announcement")
	var pending int
	require.NoError(t, db.Get(&pending, `SELECT COUNT(*) FROM deployments WHERE task_id = $1 AND status = 'pending'`, cmd.TaskID))
	assert.Equal(t, 1, pending, "one retry deployment")
}
