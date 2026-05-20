//go:build integration

package deployer_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/deploy"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/service/deployer"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDeployer implements domain/deploy.Deployer.
type fakeDeployer struct {
	deployErr   error
	deployCalls int
	active      int
}

func (f *fakeDeployer) Deploy(_ context.Context, _ deploy.JobSpec) error {
	f.deployCalls++
	return f.deployErr
}
func (f *fakeDeployer) CountActive(_ context.Context) (int, error) { return f.active, nil }

// newTestDispatcher builds a Dispatcher whose repo factory is the real Postgres
// adapter bound to the per-batch tx, with a fake Deployer.
func newTestDispatcher(db *sqlx.DB, fk *fakeDeployer, maxConcurrent int) *deployer.Dispatcher {
	return deployer.NewDispatcher(
		db, fk,
		func(exec outbox.Executor) repository.DeploymentRepository {
			return postgres.NewDeploymentsRepository(exec, testLogger())
		},
		maxConcurrent, testLogger(), deployer.DispatcherConfig{},
	)
}

// seedJob inserts a deployable pending row (valid command in job_params) with a
// chosen deploy-attempt budget, due one minute ago.
func seedJob(t *testing.T, db *sqlx.DB, maxRetries, retryCount int) uuid.UUID {
	t.Helper()
	id := uuid.New()
	payload, err := json.Marshal(command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public",
		TableName: "orders", JobName: "dbt-public-orders", NodeType: "dbt-model",
		ImageTag: "sha-abc", TaskRetryCount: 0, TaskMaxRetries: 2,
	})
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO executor_deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at)
		 VALUES ($1, $2, $3, $4, $5, $6, NOW() - interval '1 minute')`,
		id, uuid.New(), uuid.New(), payload, maxRetries, retryCount)
	require.NoError(t, err)
	return id
}

func outboxCountByType(t *testing.T, db *sqlx.DB, eventType string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM executor_outbox WHERE event_type=$1`, eventType).Scan(&n))
	return n
}

func TestDispatcher_SuccessWritesRunningAndDeployed(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{active: 0}

	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	assert.Equal(t, 1, fk.deployCalls)
	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM executor_deployments WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "deployed", status)
	assert.Equal(t, 1, outboxCountByType(t, db, "task_status_updated"))
	assert.Equal(t, 1, outboxCountByType(t, db, "node_deployed"))
	assert.Equal(t, 0, outboxCountByType(t, db, "node_updated"))
}

func TestDispatcher_TransientErrorReschedules(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{deployErr: errors.New("apiserver down")}

	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	var status string
	var rc int
	var na time.Time
	require.NoError(t, db.QueryRow(`SELECT status, retry_count, next_attempt_at FROM executor_deployments WHERE id=$1`, id).Scan(&status, &rc, &na))
	assert.Equal(t, "pending", status)
	assert.Equal(t, 1, rc)
	assert.True(t, na.After(time.Now()), "next_attempt_at pushed into the future")
	assert.Equal(t, 0, outboxCountByType(t, db, "task_status_updated"), "no announcement on transient retry")
}

func TestDispatcher_BudgetExhaustedWritesFailed(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 2)
	fk := &fakeDeployer{deployErr: errors.New("apiserver down")}

	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM executor_deployments WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "failed", status)
	assert.Equal(t, 1, outboxCountByType(t, db, "task_status_updated"))
	assert.Equal(t, 1, outboxCountByType(t, db, "node_updated"))
	assert.Equal(t, 0, outboxCountByType(t, db, "node_deployed"))
}

func TestDispatcher_PermanentErrorWritesFailedImmediately(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{deployErr: errors.Join(errors.New("bad"), pkgevents.ErrPermanent)}

	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM executor_deployments WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "failed", status, "permanent error skips the retry budget")
}

func TestDispatcher_CapZeroHeadroomDeploysNothing(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{active: 5}

	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	assert.Equal(t, 0, fk.deployCalls, "no deploys when cap reached")
	var status string
	var rc int
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM executor_deployments WHERE id=$1`, id).Scan(&status, &rc))
	assert.Equal(t, "pending", status, "throttled row stays pending")
	assert.Equal(t, 0, rc, "throttle is not a retry — retry_count unchanged")
}

func TestDispatcher_HeadroomLimitsBatch(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	for i := 0; i < 5; i++ {
		seedJob(t, db, 3, 0)
	}
	fk := &fakeDeployer{active: 3}

	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	assert.Equal(t, 2, fk.deployCalls, "only headroom (cap-active) rows deployed")
	var deployed int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM executor_deployments WHERE status='deployed'`).Scan(&deployed))
	assert.Equal(t, 2, deployed)
}

func TestDispatcher_CorruptedJobParamsMarksFailedWithRowIdentity(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	scheduleID := uuid.New()
	// Valid JSONB, but a JSON string — cannot unmarshal into DeployTask.
	_, err := db.Exec(
		`INSERT INTO executor_deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at)
		 VALUES ($1, $2, $3, '"corrupt"'::jsonb, 3, 0, NOW() - interval '1 minute')`,
		uuid.New(), taskID, scheduleID)
	require.NoError(t, err)

	fk := &fakeDeployer{active: 0}
	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	assert.Equal(t, 0, fk.deployCalls, "deploy never attempted when payload is corrupt")

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM executor_deployments WHERE id=$1`,
		// the row id differs from taskID; look it up by task_id
		mustDeploymentID(t, db, taskID)).Scan(&status))
	assert.Equal(t, "failed", status)
	assert.Equal(t, 1, outboxCountByType(t, db, "task_status_updated"))
	assert.Equal(t, 1, outboxCountByType(t, db, "node_updated"))
	assert.Equal(t, 0, outboxCountByType(t, db, "node_deployed"))

	// The FAILED announcement must carry the row's task_id (identity fallback).
	var payload []byte
	require.NoError(t, db.QueryRow(
		`SELECT payload FROM executor_outbox WHERE event_type='task_status_updated' LIMIT 1`).Scan(&payload))
	var got struct {
		TaskID     string `json:"task_id"`
		ScheduleID string `json:"schedule_id"`
		Status     string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(payload, &got))
	assert.Equal(t, taskID.String(), got.TaskID, "FAILED announcement uses the row's task_id")
	assert.Equal(t, scheduleID.String(), got.ScheduleID)
	assert.Equal(t, "FAILED", got.Status)
}

func mustDeploymentID(t *testing.T, db *sqlx.DB, taskID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	require.NoError(t, db.QueryRow(`SELECT id FROM executor_deployments WHERE task_id=$1`, taskID).Scan(&id))
	return id
}

// seedDeployableAt inserts a deployable pending row due at the given time.
func seedDeployableAt(t *testing.T, db *sqlx.DB, nextAttempt time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	payload, err := json.Marshal(command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public",
		TableName: "orders", JobName: "dbt-public-orders", NodeType: "dbt-model",
		ImageTag: "sha-abc", TaskMaxRetries: 2,
	})
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO executor_deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at)
		 VALUES ($1, $2, $3, $4, 3, 0, $5)`,
		id, uuid.New(), uuid.New(), payload, nextAttempt)
	require.NoError(t, err)
	return id
}

// countingFailSaveRepo wraps a real repository and fails the Nth Save call,
// to exercise per-row transaction isolation.
type countingFailSaveRepo struct {
	repository.DeploymentRepository
	count  *int
	failAt int
}

func (r *countingFailSaveRepo) Save(ctx context.Context, d *model.Deployment) error {
	*r.count++
	if *r.count == r.failAt {
		return errors.New("save boom")
	}
	return r.DeploymentRepository.Save(ctx, d)
}

// TestDispatcher_PerRowTransaction_FailureDoesNotRollBackOthers verifies that
// each deployment commits in its own transaction: when the second deployment's
// Save fails, the first remains deployed (committed) rather than rolling back
// with it — the behaviour a single batch-wide transaction would NOT give.
func TestDispatcher_PerRowTransaction_FailureDoesNotRollBackOthers(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	older := seedDeployableAt(t, db, time.Now().Add(-2*time.Minute))
	newer := seedDeployableAt(t, db, time.Now().Add(-1*time.Minute))

	saveCount := 0
	factory := func(exec outbox.Executor) repository.DeploymentRepository {
		return &countingFailSaveRepo{
			DeploymentRepository: postgres.NewDeploymentsRepository(exec, testLogger()),
			count:                &saveCount,
			failAt:               2, // the second deployment's Save fails
		}
	}
	fk := &fakeDeployer{active: 0}
	disp := deployer.NewDispatcher(db, fk, factory, 50, testLogger(), deployer.DispatcherConfig{})

	require.Error(t, disp.ProcessBatch(context.Background()), "second row's Save error surfaces")

	var olderStatus, newerStatus string
	require.NoError(t, db.QueryRow(`SELECT status FROM executor_deployments WHERE id=$1`, older).Scan(&olderStatus))
	require.NoError(t, db.QueryRow(`SELECT status FROM executor_deployments WHERE id=$1`, newer).Scan(&newerStatus))
	assert.Equal(t, "deployed", olderStatus, "first deployment committed in its own transaction")
	assert.Equal(t, "pending", newerStatus, "second deployment's transaction rolled back — stays pending for retry")
	assert.Equal(t, 2, fk.deployCalls)
}
