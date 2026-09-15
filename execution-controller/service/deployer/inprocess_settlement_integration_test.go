//go:build integration

package deployer_test

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	"github.com/carolsimone/continuo/execution-controller/service/outcomes"
	"github.com/carolsimone/continuo/execution-controller/service/validation"
	"github.com/carolsimone/continuo/execution-controller/test/fakes"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// This file proves that each Redis hop PR 2 collapsed into an in-process call
// still commits atomically: the job-status handler's observation and the
// state it settles (a validation node's outcome + per-node/aggregate
// projection, or a production retry's new deployments row + its FAILED
// announcement) land in ONE transaction, so a crash between the two effects
// cannot leave a half state. Each test opens its own *postgres.PostgresUnitOfWork,
// calls the handler, asserts on a SEPARATE connection (the *sqlx.DB pool) that
// nothing is visible while the transaction is still open, then commits and
// asserts everything is visible together.

// enqueueValidationDeployment inserts one pending, root (no in-set upstream)
// mode=validation deployments row for (releaseID, nodeID), backdated a minute
// so it reads as due regardless of host/container clock skew between the Go
// process and the testcontainer Postgres — the same backdating seedJob uses
// in dispatcher_test.go.
func enqueueValidationDeployment(t *testing.T, db *sqlx.DB, releaseID, nodeID string) {
	t.Helper()
	cmd := command.ValidationDeployTask{
		ReleaseID:       releaseID,
		NodeID:          nodeID,
		ServiceName:     "svc",
		SchemaName:      "public",
		TableName:       nodeID,
		NodeType:        "dbt-model",
		ImageTag:        "sha-test",
		JobName:         handlers.BuildValidationJobName(releaseID, nodeID),
		CandidateSchema: "_candidate_" + releaseID,
	}
	dep := model.NewValidationDeployment(cmd, nil, time.Now().Add(-time.Minute), false)
	require.NoError(t, postgres.NewDeploymentsRepository(db, testLogger()).Add(context.Background(), dep))
}

// enqueueProductionDeployment inserts one pending mode=production deployments
// row for a fresh task/schedule identity, jobName, and a task-level retry
// budget of (0, 2), backdated so it is immediately due. It returns the
// aggregate so the caller can key later lookups off its TaskID.
func enqueueProductionDeployment(t *testing.T, db *sqlx.DB, jobName string) *model.Deployment {
	t.Helper()
	cmd := command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public",
		TableName: "orders", JobName: jobName, NodeType: "dbt-model",
		ImageTag: "sha-abc", TaskRetryCount: 0, TaskMaxRetries: 2,
	}
	dep := model.NewDeployment(cmd, nil, time.Now().Add(-time.Minute))
	require.NoError(t, postgres.NewDeploymentsRepository(db, testLogger()).Add(context.Background(), dep))
	return dep
}

// TestValidationTerminal_OutcomeAndPerNodeResultCommitTogether proves Task 17's
// collapse: the job-status handler's observation of a terminal validation Job
// records the node's outcome AND settles the leg (per-node projection, and —
// since this is the release's only node — the aggregate) in the SAME
// transaction. Before Commit, a query on a separate connection sees neither
// the outcome nor the projection/aggregate rows; after Commit, all three are
// present together.
func TestValidationTerminal_OutcomeAndPerNodeResultCommitTogether(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	releaseID, nodeID := "rel-tx", "svc.a"
	enqueueValidationDeployment(t, db, releaseID, nodeID)

	fk := &fakeDeployer{}
	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(ctx))
	require.Equal(t, 1, fk.validationDeployCalls, "the dispatcher created the validation Job and wrote the first check ticket")

	observer := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(context.Context, string, string) (*model.JobResult, error) {
			return &model.JobResult{Status: model.JobStatusSucceeded}, nil
		},
		GetJobMetaFunc: func(context.Context, string, string) (map[string]string, map[string]string, error) {
			return map[string]string{"mode": pkgevents.ModeValidation},
				map[string]string{pkgmodel.AnnotationReleaseID: releaseID, pkgmodel.AnnotationNodeID: nodeID}, nil
		},
	}
	handler := handlers.NewJobStatusHandler(observer, &fakes.FakeLogUploader{},
		&handlers.JobStatusConfig{K8sNamespace: "default", CheckDelaySeconds: 1, LogTailLines: 5, ErrorMessageMaxLen: 100, DefaultTaskMaxRetries: 2},
		postgres.NewCancelledSchedulesRepository(db), outcomes.NewRecorder(slog.Default()), slog.Default())

	u := postgres.NewPostgresUnitOfWork(db, slog.Default())
	require.NoError(t, u.Begin(ctx))
	cmd := checkCommandFor(t, db, releaseID, nodeID)
	require.NoError(t, handler.Handle(ctx, u, cmd, uuid.Nil))

	// db is the connection pool; u.tx holds its own checked-out connection for
	// the still-open transaction, so these queries run on a different
	// connection and can only see committed state.
	var outcomeBefore sql.NullString
	require.NoError(t, db.Get(&outcomeBefore,
		`SELECT outcome FROM deployments WHERE release_id=$1 AND node_id=$2 AND mode='validation'`, releaseID, nodeID))
	require.False(t, outcomeBefore.Valid, "outcome must not be visible before commit")
	var kindsBefore []string
	require.NoError(t, db.Select(&kindsBefore,
		`SELECT event_type FROM execution_outbox WHERE stream_name=$1 ORDER BY created_at`, streams.ValidationResultV1))
	require.Empty(t, kindsBefore, "per-node/aggregate rows must not be visible before commit")

	require.NoError(t, u.Commit())

	var outcome string
	require.NoError(t, db.Get(&outcome,
		`SELECT outcome FROM deployments WHERE release_id=$1 AND node_id=$2 AND mode='validation'`, releaseID, nodeID))
	require.Equal(t, "ok", outcome)
	var kinds []string
	require.NoError(t, db.Select(&kinds,
		`SELECT event_type FROM execution_outbox WHERE stream_name=$1 ORDER BY created_at`, streams.ValidationResultV1))
	require.Equal(t, []string{validation.EventTypeValidationNodeResult, validation.EventTypeValidationCompleted}, kinds,
		"per-node row and the aggregate (single node) commit with the outcome")
}

// TestProductionFailureBelowBudget_RetryRowCommitsWithFailedAnnouncement proves
// Task 18's collapse: a retryable production Job failure re-queues the -rN
// retry as a new pending deployments row via createDeployment in the SAME
// transaction as the FAILED task_status_updated/task_execution_recorded
// announcement rows. Before Commit, a query on a separate connection sees
// neither the retry row nor the announcement rows; after Commit, both are
// present together.
func TestProductionFailureBelowBudget_RetryRowCommitsWithFailedAnnouncement(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	dep := enqueueProductionDeployment(t, db, "job-r")
	fk := &fakeDeployer{}
	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(ctx))
	require.Equal(t, 1, fk.deployCalls, "the dispatcher created the production Job and wrote the first check ticket")

	observer := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(context.Context, string, string) (*model.JobResult, error) {
			return &model.JobResult{Status: model.JobStatusFailed}, nil
		},
		GetJobMetaFunc: func(context.Context, string, string) (map[string]string, map[string]string, error) {
			return map[string]string{}, map[string]string{}, nil
		},
	}
	handler := handlers.NewJobStatusHandler(observer, &fakes.FakeLogUploader{},
		&handlers.JobStatusConfig{K8sNamespace: "default", CheckDelaySeconds: 1, LogTailLines: 5, ErrorMessageMaxLen: 100, DefaultTaskMaxRetries: 2},
		postgres.NewCancelledSchedulesRepository(db), outcomes.NewRecorder(slog.Default()), slog.Default())

	u := postgres.NewPostgresUnitOfWork(db, slog.Default())
	require.NoError(t, u.Begin(ctx))
	cmd := checkCommandForTask(t, db, dep) // RetryCount 0, MaxRetries 2 — retryable
	require.NoError(t, handler.Handle(ctx, u, cmd, uuid.Nil))

	taskID, err := uuid.Parse(dep.Command().TaskID)
	require.NoError(t, err)

	var pendingBefore int
	require.NoError(t, db.Get(&pendingBefore,
		`SELECT COUNT(*) FROM deployments WHERE task_id=$1 AND status='pending'`, taskID))
	require.Equal(t, 0, pendingBefore, "the -rN retry row must not be visible before commit")
	// Scoped to the two announcement event types (rather than every pending
	// outbox row for the aggregate) because the dispatcher's first check_delayed
	// ticket for this task already committed before this transaction opened —
	// counting all pending rows would count that pre-existing row too.
	var outboxBefore []string
	require.NoError(t, db.Select(&outboxBefore,
		`SELECT event_type FROM execution_outbox WHERE aggregate_id=$1 AND event_type IN ($2,$3)`,
		taskID, event.EventTypeTaskStatusUpdated, event.EventTypeTaskExecutionRecorded))
	require.Empty(t, outboxBefore, "the FAILED announcement rows must not be visible before commit")

	require.NoError(t, u.Commit())

	var jobNames []string
	require.NoError(t, db.Select(&jobNames,
		`SELECT job_params->>'job_name' FROM deployments WHERE task_id=$1 AND status='pending'`, taskID))
	require.Equal(t, []string{"job-r-r1"}, jobNames, "the retry row carries the -r1 job name")
	var types []string
	require.NoError(t, db.Select(&types,
		`SELECT event_type FROM execution_outbox WHERE aggregate_id=$1 AND status='pending' ORDER BY created_at`, taskID))
	require.Contains(t, types, event.EventTypeTaskStatusUpdated, "the FAILED announcement commits alongside the retry row")
	require.Contains(t, types, event.EventTypeTaskExecutionRecorded)
}
