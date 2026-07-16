//go:build integration

package deployer_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/deploy"
	"github.com/carolsimone/continuo/executor-controller/domain/event"
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
	deployErr             error
	deployCalls           int
	validationDeployCalls int
	// specs records every production spec deployed, so a test can assert what
	// the Job was told to run and which row accounts for it.
	specs []deploy.JobSpec
}

func (f *fakeDeployer) Deploy(_ context.Context, spec deploy.JobSpec) error {
	f.deployCalls++
	f.specs = append(f.specs, spec)
	return f.deployErr
}
func (f *fakeDeployer) DeployValidation(_ context.Context, _ deploy.ValidationJobSpec) error {
	f.validationDeployCalls++
	return f.deployErr
}
func (f *fakeDeployer) DeploySeedBuild(_ context.Context, _ deploy.ValidationJobSpec) error {
	f.validationDeployCalls++
	return f.deployErr
}
func (f *fakeDeployer) DeployCompile(_ context.Context, _ deploy.ValidationJobSpec) error {
	f.validationDeployCalls++
	return f.deployErr
}

// newTestDispatcher builds a Dispatcher whose repo factory is the real Postgres
// adapter bound to the per-batch tx, with a fake Deployer.
func newTestDispatcher(db *sqlx.DB, fk deploy.Deployer, maxConcurrent int) *deployer.Dispatcher {
	return deployer.NewDispatcher(
		db, fk,
		func(exec outbox.Executor) repository.DeploymentRepository {
			return postgres.NewDeploymentsRepository(exec, testLogger())
		},
		func(exec outbox.Executor) repository.ValidationAggregateRepository {
			return postgres.NewValidationAggregateRepository(exec)
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

// seedUndeployableJob inserts a pending row whose job_params carry an empty
// task_id. IsDeployable() is false for such a row, and its task_id is not a
// parseable UUID, so it exercises the degraded-identity dispatch path.
func seedUndeployableJob(t *testing.T, db *sqlx.DB) uuid.UUID {
	t.Helper()
	id := uuid.New()
	payload, err := json.Marshal(command.DeployTask{
		TaskID: "", ScheduleID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public",
		TableName: "orders", JobName: "dbt-public-orders", NodeType: "dbt-model",
		ImageTag: "sha-abc", TaskRetryCount: 0, TaskMaxRetries: 2,
	})
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO executor_deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at)
		 VALUES ($1, $2, $3, $4, 3, 0, NOW() - interval '1 minute')`,
		id, uuid.New(), uuid.New(), payload)
	require.NoError(t, err)
	return id
}

// TestDispatcher_UndeployableRowSettlesAndAnnounces pins that a row the executor
// cannot run is marked failed and persisted in the same batch that discovers it.
// An undeployable row's task_id is not necessarily a UUID; if that degraded
// identity aborted the dispatch, the row would never leave 'pending' and would be
// re-dispatched on every tick forever, with its FAILED pair never written.
func TestDispatcher_UndeployableRowSettlesAndAnnounces(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedUndeployableJob(t, db)
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	assert.Equal(t, 0, fk.deployCalls, "an undeployable row is never handed to the deployer")
	var status string
	var msg *string
	require.NoError(t, db.QueryRow(
		`SELECT status, error_message FROM executor_deployments WHERE id=$1`, id).Scan(&status, &msg))
	assert.Equal(t, "failed", status, "the undeployable row is settled, not left pending")
	require.NotNil(t, msg)
	assert.Contains(t, *msg, "not deployable")

	assert.Equal(t, 1, outboxCountByType(t, db, "task_status_updated"))
	assert.Equal(t, 1, outboxCountByType(t, db, "node_updated"))
	assert.Equal(t, 0, outboxCountByType(t, db, "node_deployed"))

	// The announcements key on uuid.Nil: the row recovered no parseable task
	// identity, and a missing key must not cost the system its terminal events.
	var aggID uuid.UUID
	require.NoError(t, db.QueryRow(
		`SELECT aggregate_id FROM executor_outbox WHERE event_type='node_updated'`).Scan(&aggID))
	assert.Equal(t, uuid.Nil, aggID)

	// A settled row is not due again: the next tick finds nothing to dispatch.
	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))
	assert.Equal(t, 1, outboxCountByType(t, db, "node_updated"), "settled row is not re-dispatched")
}

func TestDispatcher_SuccessWritesDeployedOnly(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	assert.Equal(t, 1, fk.deployCalls)
	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM executor_deployments WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "deployed", status)
	// k8s-controller now owns the RUNNING announcement; the deploy path emits only
	// the node_deployed trigger that starts k8s polling.
	assert.Equal(t, 0, outboxCountByType(t, db, "task_status_updated"), "deploy path no longer announces RUNNING")
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

// holdSlots marks n rows as holding an execution slot, standing in for work
// already running — whether as a Kubernetes Job or under a worker lease. Both
// spend the same budget, so the dispatcher must see them.
func holdSlots(t *testing.T, db *sqlx.DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, err := db.Exec(
			`INSERT INTO executor_deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at, status, slot_reserved_at)
			 VALUES ($1, $2, $3, '{}'::jsonb, 3, 0, NOW(), 'deployed', NOW())`,
			uuid.New(), uuid.New(), uuid.New())
		require.NoError(t, err)
	}
}

func TestDispatcher_CapZeroHeadroomDeploysNothing(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	holdSlots(t, db, 5)
	fk := &fakeDeployer{}

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
	holdSlots(t, db, 3)
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	assert.Equal(t, 2, fk.deployCalls, "only headroom (cap - slots already held) rows deployed")
	var deployed int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM executor_deployments WHERE status='deployed' AND job_params::text <> '{}'`).Scan(&deployed))
	assert.Equal(t, 2, deployed)
}

// TestDispatcher_WorkerHeldSlotsThrottleJobs is the shared-budget invariant: a
// slot taken by a worker lease must throttle the Jobs path exactly as a Job's
// own slot does. Counting live Kubernetes Jobs would miss worker-held slots
// entirely and let the executor run past its cap.
func TestDispatcher_WorkerHeldSlotsThrottleJobs(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	seedJob(t, db, 3, 0)
	_, err := db.Exec(
		`INSERT INTO executor_deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at, status, execution_mode, pool_key, slot_reserved_at)
		 VALUES ($1, $2, $3, '{}'::jsonb, 3, 0, NOW(), 'running', 'workers', 'pool-1', NOW())`,
		uuid.New(), uuid.New(), uuid.New())
	require.NoError(t, err)

	fk := &fakeDeployer{}
	require.NoError(t, newTestDispatcher(db, fk, 1).ProcessBatch(context.Background()))

	assert.Equal(t, 0, fk.deployCalls, "a worker lease holds the only slot; the Jobs path must wait")
}

// TestDispatcher_ReservationHoldsSlotAndNamesTheJob pins the accounting a
// dispatched Job depends on: it holds a slot from reservation onward, and the
// Job names the row holding it so the Job's terminal status can release it.
func TestDispatcher_ReservationHoldsSlotAndNamesTheJob(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	require.Len(t, fk.specs, 1)
	assert.Equal(t, id.String(), fk.specs[0].ExecutorDeploymentID,
		"the Job must name the row accounting for it")

	var reserved, released *time.Time
	require.NoError(t, db.QueryRow(
		`SELECT slot_reserved_at, slot_released_at FROM executor_deployments WHERE id=$1`, id).
		Scan(&reserved, &released))
	assert.NotNil(t, reserved, "a deployed Job holds its slot")
	assert.Nil(t, released, "the slot stays held until the Job reports terminal")
}

// TestDispatcher_TransientFailureHandsTheSlotBack keeps a failed dispatch from
// consuming capacity: no Job runs, so nothing will ever report terminal for it.
func TestDispatcher_TransientFailureHandsTheSlotBack(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{deployErr: errors.New("apiserver down")}

	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	var status string
	var released *time.Time
	require.NoError(t, db.QueryRow(
		`SELECT status, slot_released_at FROM executor_deployments WHERE id=$1`, id).
		Scan(&status, &released))
	assert.Equal(t, "pending", status, "rescheduled for another attempt")
	assert.NotNil(t, released, "the failed attempt hands its slot back")
}

// TestDispatcher_UndeployableRowHandsTheSlotBack covers the other no-Job exit:
// a row the executor can never run must not keep the slot it reserved.
func TestDispatcher_UndeployableRowHandsTheSlotBack(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedUndeployableJob(t, db)
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	var released *time.Time
	require.NoError(t, db.QueryRow(
		`SELECT slot_released_at FROM executor_deployments WHERE id=$1`, id).Scan(&released))
	assert.NotNil(t, released, "a row that will never run holds no slot")
}

// TestDispatcher_StaleDispatchIsRedriven covers the crash between the Job create
// and the transaction recording it. The reservation is left in 'dispatching';
// the next tick repeats the idempotent create and finishes the transition,
// without ever releasing the slot of a Job that may already be running.
func TestDispatcher_StaleDispatchIsRedriven(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	_, err := db.Exec(
		`UPDATE executor_deployments SET status='dispatching', slot_reserved_at = NOW() - interval '10 minutes' WHERE id=$1`, id)
	require.NoError(t, err)

	fk := &fakeDeployer{}
	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	assert.Equal(t, 1, fk.deployCalls, "the stranded dispatch repeats its idempotent Job create")
	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM executor_deployments WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "deployed", status, "the stranded dispatch is finalized")
}

// TestDispatcher_FreshDispatchIsNotRedriven keeps a dispatch that is merely slow
// from being re-driven underneath the tick still working on it.
func TestDispatcher_FreshDispatchIsNotRedriven(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	_, err := db.Exec(
		`UPDATE executor_deployments SET status='dispatching', slot_reserved_at = NOW() WHERE id=$1`, id)
	require.NoError(t, err)

	fk := &fakeDeployer{}
	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	assert.Equal(t, 0, fk.deployCalls, "a dispatch inside the recovery window is left alone")
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

	fk := &fakeDeployer{}
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
// each deployment commits in its own transactions: when the second deployment's
// settle fails, the first remains deployed rather than rolling back with it —
// the behaviour a single batch-wide transaction would NOT give.
//
// Each deployment Saves twice: once to take its slot, once to record the Job.
// Failing the fourth Save therefore fails the second deployment's settle, after
// its Job was created. That row stays in 'dispatching' still holding its slot,
// which is the point of the reservation: its Job may be running, so a later tick
// re-drives it rather than releasing capacity that is genuinely in use.
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
			failAt:               4, // the second deployment's settle Save fails
		}
	}
	aggFactory := func(exec outbox.Executor) repository.ValidationAggregateRepository {
		return postgres.NewValidationAggregateRepository(exec)
	}
	fk := &fakeDeployer{}
	disp := deployer.NewDispatcher(db, fk, factory, aggFactory, 50, testLogger(), deployer.DispatcherConfig{})

	require.Error(t, disp.ProcessBatch(context.Background()), "second row's Save error surfaces")

	var olderStatus, newerStatus string
	require.NoError(t, db.QueryRow(`SELECT status FROM executor_deployments WHERE id=$1`, older).Scan(&olderStatus))
	require.NoError(t, db.QueryRow(`SELECT status FROM executor_deployments WHERE id=$1`, newer).Scan(&newerStatus))
	assert.Equal(t, "deployed", olderStatus, "first deployment committed in its own transaction")
	assert.Equal(t, "dispatching", newerStatus, "second deployment's settle rolled back; its reservation stands for recovery")
	assert.Equal(t, 2, fk.deployCalls)
}

// seedPromoteSeedJob inserts a due pending promote-seed row. Promote-seed Jobs
// are fire-and-forget prod seeds with no state-bound run.
func seedPromoteSeedJob(t *testing.T, db *sqlx.DB) uuid.UUID {
	t.Helper()
	id := uuid.New()
	payload, err := json.Marshal(command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(),
		ScheduleName: "promote", ServiceName: "dbt", SchemaName: "public",
		TableName: "fx", JobName: "dbt-public-fx", NodeType: "dbt-seed",
		ImageTag: "sha-abc", Mode: pkgevents.ModePromoteSeed,
	})
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO executor_deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at)
		 VALUES ($1, $2, $3, $4, 3, 0, NOW() - interval '1 minute')`,
		id, uuid.New(), uuid.New(), payload)
	require.NoError(t, err)
	return id
}

// nodeDeployedPayload decodes the single node_deployed outbox row.
func nodeDeployedPayload(t *testing.T, db *sqlx.DB) event.JobDeployed {
	t.Helper()
	var raw []byte
	require.NoError(t, db.QueryRow(
		`SELECT payload FROM executor_outbox WHERE event_type='node_deployed'`).Scan(&raw))
	var p event.JobDeployed
	require.NoError(t, json.Unmarshal(raw, &p))
	return p
}

// TestDispatcher_PromoteSeedIsObserved pins that a promote-seed Job is status
// checked. It emits no state-bound lifecycle events, but without a node.deployed
// trigger k8s-controller — which never polls — would never observe its terminal
// status, and the slot it reserved would never be released.
func TestDispatcher_PromoteSeedIsObserved(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedPromoteSeedJob(t, db)
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	assert.Equal(t, 1, outboxCountByType(t, db, "node_deployed"),
		"promote-seed must be observed so its slot is released")
	assert.Equal(t, 0, outboxCountByType(t, db, "task_status_updated"),
		"promote-seed has no state run: it announces no task status")

	p := nodeDeployedPayload(t, db)
	assert.Equal(t, id.String(), p.ExecutorDeploymentID)
	assert.Equal(t, pkgevents.ModePromoteSeed, p.Mode)
}

// TestDispatcher_NodeDeployedNamesTheSlotOwner lets the Job's terminal check
// release the right row without re-reading the Job, which may be TTL-reaped by
// the time it settles.
func TestDispatcher_NodeDeployedNamesTheSlotOwner(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	p := nodeDeployedPayload(t, db)
	assert.Equal(t, id.String(), p.ExecutorDeploymentID)
}

// concurrentDeployer counts deploys and the peak number in flight at once. It is
// safe for the concurrent Dispatchers of
// TestDispatcher_ConcurrentDispatchersNeverOvershootTheCap to share.
type concurrentDeployer struct {
	mu       sync.Mutex
	calls    int
	inFlight int
	peak     int
}

func (f *concurrentDeployer) Deploy(_ context.Context, _ deploy.JobSpec) error {
	f.mu.Lock()
	f.calls++
	f.inFlight++
	if f.inFlight > f.peak {
		f.peak = f.inFlight
	}
	f.mu.Unlock()

	// Hold the "Job" open briefly so overlapping dispatchers genuinely coincide;
	// a reservation is held across this window, exactly as in production.
	time.Sleep(20 * time.Millisecond)

	f.mu.Lock()
	f.inFlight--
	f.mu.Unlock()
	return nil
}
func (f *concurrentDeployer) DeployValidation(_ context.Context, _ deploy.ValidationJobSpec) error {
	return nil
}
func (f *concurrentDeployer) DeploySeedBuild(_ context.Context, _ deploy.ValidationJobSpec) error {
	return nil
}
func (f *concurrentDeployer) DeployCompile(_ context.Context, _ deploy.ValidationJobSpec) error {
	return nil
}

// TestDispatcher_ConcurrentDispatchersNeverOvershootTheCap is the no-overshoot
// proof for the dispatcher itself. Several replicas drain one queue at once with
// far more work due than the cap allows. Each reservation reads the slot count
// and takes its slot under the same capacity lock, so the readings are
// serialized rather than racy samples: no two dispatchers can spend the same
// free slot. Exactly the cap's worth of work may start, and no more.
func TestDispatcher_ConcurrentDispatchersNeverOvershootTheCap(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	const (
		limit       = 4
		dispatchers = 8
		dueRows     = 20
	)
	for i := 0; i < dueRows; i++ {
		seedJob(t, db, 3, 0)
	}

	fk := &concurrentDeployer{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, dispatchers)
	for i := 0; i < dispatchers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := newTestDispatcher(db, fk, limit).ProcessBatch(context.Background()); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	assert.LessOrEqual(t, fk.peak, limit, "more Jobs ran at once than the cap allows")
	assert.Equal(t, limit, fk.calls, "exactly the cap's worth of work started")

	var held int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM executor_deployments WHERE slot_reserved_at IS NOT NULL AND slot_released_at IS NULL`).
		Scan(&held))
	assert.Equal(t, limit, held, "every started Job holds exactly one slot, and none leaked")
}

// TestDispatcher_MixedJobAndWorkerReservationsShareOneBudget proves the two
// paths draw from one pool: with the cap already part-consumed by worker leases,
// the Jobs path may start only the remainder. Counting live Kubernetes Jobs
// would see none of the worker-held slots and start the full cap on top of them.
func TestDispatcher_MixedJobAndWorkerReservationsShareOneBudget(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	const limit = 5
	const workerHeld = 3
	for i := 0; i < 10; i++ {
		seedJob(t, db, 3, 0)
	}
	for i := 0; i < workerHeld; i++ {
		_, err := db.Exec(
			`INSERT INTO executor_deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at, status, execution_mode, pool_key, slot_reserved_at)
			 VALUES ($1, $2, $3, '{}'::jsonb, 3, 0, NOW(), 'running', 'workers', 'pool-a', NOW())`,
			uuid.New(), uuid.New(), uuid.New())
		require.NoError(t, err)
	}

	fk := &fakeDeployer{}
	require.NoError(t, newTestDispatcher(db, fk, limit).ProcessBatch(context.Background()))

	assert.Equal(t, limit-workerHeld, fk.deployCalls,
		"the Jobs path may take only the slots the workers left")

	var held int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM executor_deployments WHERE slot_reserved_at IS NOT NULL AND slot_released_at IS NULL`).
		Scan(&held))
	assert.Equal(t, limit, held, "Jobs and workers together consume exactly the one budget")
}
