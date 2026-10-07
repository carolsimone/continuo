//go:build integration

package deployer_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/deploy"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/execution-controller/serialization"
	"github.com/carolsimone/continuo/execution-controller/service/deployer"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDeployer implements domain/deploy.Deployer. It is safe for concurrent
// use: dispatchers running in parallel share one instance.
type fakeDeployer struct {
	mu                    sync.Mutex
	deployErr             error
	deployCalls           int
	validationDeployCalls int
	delay                 time.Duration // slept inside every Deploy* call
}

func (f *fakeDeployer) record(counter *int) error {
	f.mu.Lock()
	*counter++
	err := f.deployErr
	f.mu.Unlock()
	time.Sleep(f.delay)
	return err
}

func (f *fakeDeployer) Deploy(_ context.Context, _ deploy.JobSpec) error {
	return f.record(&f.deployCalls)
}
func (f *fakeDeployer) DeployValidation(_ context.Context, _ deploy.ValidationJobSpec) error {
	return f.record(&f.validationDeployCalls)
}
func (f *fakeDeployer) DeploySeedBuild(_ context.Context, _ deploy.ValidationJobSpec) error {
	return f.record(&f.validationDeployCalls)
}
func (f *fakeDeployer) DeployCompile(_ context.Context, _ deploy.ValidationJobSpec) error {
	return f.record(&f.validationDeployCalls)
}

// calls is the number of Deploy* calls of every kind.
func (f *fakeDeployer) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deployCalls + f.validationDeployCalls
}

func repoFactory(exec outbox.Executor) repository.DeploymentRepository {
	return postgres.NewDeploymentsRepository(exec, testLogger())
}

func aggRepoFactory(exec outbox.Executor) repository.ValidationAggregateRepository {
	return postgres.NewValidationAggregateRepository(exec)
}

func admissionFactory(exec outbox.Executor) repository.AdmissionRepository {
	return postgres.NewAdmissionRepository(exec, testLogger())
}

// newTestDispatcher builds a Dispatcher whose repo factories are the real
// Postgres adapters bound to the claim or launch tx, with a fake Deployer.
func newTestDispatcher(db *sqlx.DB, fk *fakeDeployer, maxConcurrent int) *deployer.Dispatcher {
	return newTestDispatcherWith(db, fk, maxConcurrent, deployer.DispatcherConfig{}, admissionFactory)
}

// newTestDispatcherWith builds a Dispatcher with an explicit config and
// admission repository factory.
func newTestDispatcherWith(db *sqlx.DB, fk *fakeDeployer, maxConcurrent int, cfg deployer.DispatcherConfig, admission deployer.AdmissionRepoFactory) *deployer.Dispatcher {
	return deployer.NewDispatcher(db, fk, repoFactory, aggRepoFactory, admission, maxConcurrent, testLogger(), cfg)
}

// seedJob inserts a deployable pending row (valid command in job_params) with a
// chosen deploy-attempt budget, due one minute ago.
func seedJob(t *testing.T, db *sqlx.DB, maxRetries, retryCount int) uuid.UUID {
	t.Helper()
	return seedJobNamed(t, db, "dbt-public-orders-"+uuid.NewString()[:8], maxRetries, retryCount)
}

// seedJobNamed is seedJob with an explicit Job name.
func seedJobNamed(t *testing.T, db *sqlx.DB, name string, maxRetries, retryCount int) uuid.UUID {
	t.Helper()
	id := uuid.New()
	payload, err := json.Marshal(serialization.DeployTaskFromDomain(command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public",
		TableName: "orders", JobName: name, NodeType: "dbt-model",
		ImageTag: "sha-abc", TaskRetryCount: 0, TaskMaxRetries: 2,
	}))
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at)
		 VALUES ($1, $2, $3, $4, $5, $6, NOW() - interval '1 minute')`,
		id, uuid.New(), uuid.New(), payload, maxRetries, retryCount)
	require.NoError(t, err)
	return id
}

// failedAnnouncementRetryCount decodes the retry_count of the
// task_status_updated row the dispatcher wrote for a terminal deploy failure.
func failedAnnouncementRetryCount(t *testing.T, db *sqlx.DB) int32 {
	t.Helper()
	var payload []byte
	require.NoError(t, db.QueryRow(
		`SELECT payload FROM execution_outbox WHERE event_type='task_status_updated' LIMIT 1`).Scan(&payload))
	var got pkgevents.TaskStatusUpdated
	require.NoError(t, json.Unmarshal(payload, &got))
	return got.RetryCount
}

// seedRunning inserts a deployable row already holding a slot.
func seedRunning(t *testing.T, db *sqlx.DB) uuid.UUID {
	t.Helper()
	id := seedJob(t, db, 3, 0)
	_, err := db.Exec(`UPDATE deployments SET status = 'running' WHERE id = $1`, id)
	require.NoError(t, err)
	return id
}

func countByStatus(t *testing.T, db *sqlx.DB, status string) int {
	t.Helper()
	var n int
	require.NoError(t, db.Get(&n, `SELECT count(*) FROM deployments WHERE status = $1`, status))
	return n
}

func countStatusOf(t *testing.T, db *sqlx.DB, id uuid.UUID) string {
	t.Helper()
	var status string
	require.NoError(t, db.Get(&status, `SELECT status FROM deployments WHERE id = $1`, id))
	return status
}

func outboxCountByType(t *testing.T, db *sqlx.DB, eventType string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM execution_outbox WHERE event_type=$1`, eventType).Scan(&n))
	return n
}

func TestDispatcher_SuccessMarksStartedAndWritesFirstCheck(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	assert.Equal(t, 1, fk.deployCalls)
	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM deployments WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "starting", status)
	// The job-status handler owns the RUNNING announcement; the deploy path writes
	// only the first check_delayed ticket that starts k8s polling.
	assert.Equal(t, 0, outboxCountByType(t, db, "task_status_updated"), "deploy path no longer announces RUNNING")
	assert.Equal(t, 1, outboxCountByType(t, db, "check_delayed"))
	assert.Equal(t, 0, outboxCountByType(t, db, "node_updated"))
}

// TestDispatch_WritesCheckDelayedTicketOnDeploySuccess proves a successful
// production deploy writes exactly one check_delayed outbox row — the first
// self-scheduled status check — rather than a separate per-Job-create trigger.
func TestDispatch_WritesCheckDelayedTicketOnDeploySuccess(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	payload, err := json.Marshal(serialization.DeployTaskFromDomain(command.DeployTask{
		TaskID: taskID.String(), ScheduleID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public",
		TableName: "orders", JobName: "job-check-ticket", NodeType: "dbt-model",
		ImageTag: "sha-abc", TaskRetryCount: 0, TaskMaxRetries: 2,
	}))
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at)
		 VALUES ($1, $2, $3, $4, $5, $6, NOW() - interval '1 minute')`,
		uuid.New(), taskID, uuid.New(), payload, 3, 0)
	require.NoError(t, err)

	fk := &fakeDeployer{}
	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	var rows []struct {
		EventType  string          `db:"event_type"`
		StreamName string          `db:"stream_name"`
		Payload    json.RawMessage `db:"payload"`
	}
	require.NoError(t, db.Select(&rows, `SELECT event_type, stream_name, payload FROM execution_outbox WHERE aggregate_id = $1`, taskID))
	require.Len(t, rows, 1)
	require.Equal(t, event.EventTypeCheckDelayed, rows[0].EventType)
	require.Equal(t, streams.CheckK8sV1, rows[0].StreamName)

	var dto serialization.JobCheckRequestDTO
	require.NoError(t, json.Unmarshal(rows[0].Payload, &dto))
	require.Equal(t, "job-check-ticket", dto.JobName)
	require.False(t, dto.RunningAnnounced)
	// The first check is due after the short first-check delay (default 1s),
	// not the 10s re-check cadence, so a fast Job is still observed running.
	require.InDelta(t, time.Now().Add(1*time.Second).Unix(), dto.CheckAfter, 3)
}

// TestDispatch_FirstCheckCarriesSecretRef proves the first check ticket of a
// production deploy keeps the task's Secret name, so a retry rebuilt from the
// ticket still mounts it.
func TestDispatch_FirstCheckCarriesSecretRef(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	payload, err := json.Marshal(serialization.DeployTaskFromDomain(command.DeployTask{
		TaskID: taskID.String(), ScheduleID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public",
		TableName: "orders", JobName: "job-secret-ref", NodeType: string(pkg_model.NodeTypePythonApi),
		ImageTag: "sha-abc", SecretRef: "continuo-api-fx", TaskRetryCount: 0, TaskMaxRetries: 2,
	}))
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at)
		 VALUES ($1, $2, $3, $4, $5, $6, NOW() - interval '1 minute')`,
		uuid.New(), taskID, uuid.New(), payload, 3, 0)
	require.NoError(t, err)

	require.NoError(t, newTestDispatcher(db, &fakeDeployer{}, 50).ProcessBatch(context.Background()))

	var raw json.RawMessage
	require.NoError(t, db.Get(&raw, `SELECT payload FROM execution_outbox WHERE aggregate_id = $1`, taskID))
	var dto serialization.JobCheckRequestDTO
	require.NoError(t, json.Unmarshal(raw, &dto))
	require.Equal(t, "continuo-api-fx", dto.SecretRef)
}

// TestDispatch_FirstCheckUsesFirstCheckDelay proves the dispatcher schedules
// the first status check with FirstCheckDelay while CheckDelay (the cadence
// between re-checks of a still-running Job) stays untouched. With a single
// 10s delay a Job that finishes in a few seconds is never observed running,
// so the task jumps from pending straight to its terminal status and the UI
// never shows it as running.
func TestDispatch_FirstCheckUsesFirstCheckDelay(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	payload, err := json.Marshal(serialization.DeployTaskFromDomain(command.DeployTask{
		TaskID: taskID.String(), ScheduleID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public",
		TableName: "orders", JobName: "job-first-check", NodeType: "dbt-model",
		ImageTag: "sha-abc", TaskRetryCount: 0, TaskMaxRetries: 2,
	}))
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at)
		 VALUES ($1, $2, $3, $4, $5, $6, NOW() - interval '1 minute')`,
		uuid.New(), taskID, uuid.New(), payload, 3, 0)
	require.NoError(t, err)

	d := newTestDispatcherWith(db, &fakeDeployer{}, 50,
		deployer.DispatcherConfig{CheckDelay: 60 * time.Second, FirstCheckDelay: 3 * time.Second}, admissionFactory)
	require.NoError(t, d.ProcessBatch(context.Background()))

	var raw json.RawMessage
	require.NoError(t, db.Get(&raw, `SELECT payload FROM execution_outbox WHERE aggregate_id = $1`, taskID))
	var dto serialization.JobCheckRequestDTO
	require.NoError(t, json.Unmarshal(raw, &dto))
	require.InDelta(t, time.Now().Add(3*time.Second).Unix(), dto.CheckAfter, 2)
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
	require.NoError(t, db.QueryRow(`SELECT status, retry_count, next_attempt_at FROM deployments WHERE id=$1`, id).Scan(&status, &rc, &na))
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
	require.NoError(t, db.QueryRow(`SELECT status FROM deployments WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "failed", status)
	assert.Equal(t, 1, outboxCountByType(t, db, "task_status_updated"))
	assert.Equal(t, 1, outboxCountByType(t, db, "node_updated"))
	assert.Equal(t, 0, outboxCountByType(t, db, "check_delayed"))

	// seedJob's task budget is 2; a deploy failure spends it.
	assert.Equal(t, int32(2), failedAnnouncementRetryCount(t, db), "the FAILED status reports the task budget as spent")
}

func TestDispatcher_PermanentErrorWritesFailedImmediately(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{deployErr: errors.Join(errors.New("bad"), pkgevents.ErrPermanent)}

	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM deployments WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "failed", status, "permanent error skips the retry budget")
	assert.Equal(t, int32(2), failedAnnouncementRetryCount(t, db), "the FAILED status reports the task budget as spent")
}

func TestDispatcher_CapZeroHeadroomDeploysNothing(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	for i := 0; i < 5; i++ {
		seedRunning(t, db)
	}
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	assert.Equal(t, 0, fk.calls(), "no deploys when cap reached")
	var status string
	var rc int
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM deployments WHERE id=$1`, id).Scan(&status, &rc))
	assert.Equal(t, "pending", status, "throttled row stays pending")
	assert.Equal(t, 0, rc, "throttle is not a retry — retry_count unchanged")
}

func TestDispatcher_HeadroomLimitsBatch(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	for i := 0; i < 3; i++ {
		seedRunning(t, db)
	}
	for i := 0; i < 5; i++ {
		seedJob(t, db, 3, 0)
	}
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	assert.Equal(t, 2, fk.calls(), "only headroom (cap minus in-flight rows) deployed")
	assert.Equal(t, 2, countByStatus(t, db, "starting"))
}

func TestDispatcher_CorruptedJobParamsMarksFailedWithRowIdentity(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	scheduleID := uuid.New()
	// Valid JSONB, but a JSON string — cannot unmarshal into DeployTask.
	_, err := db.Exec(
		`INSERT INTO deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at)
		 VALUES ($1, $2, $3, '"corrupt"'::jsonb, 3, 0, NOW() - interval '1 minute')`,
		uuid.New(), taskID, scheduleID)
	require.NoError(t, err)

	fk := &fakeDeployer{}
	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	assert.Equal(t, 0, fk.calls(), "deploy never attempted when payload is corrupt")

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM deployments WHERE id=$1`,
		// the row id differs from taskID; look it up by task_id
		mustDeploymentID(t, db, taskID)).Scan(&status))
	assert.Equal(t, "failed", status)
	assert.Equal(t, 1, outboxCountByType(t, db, "task_status_updated"))
	assert.Equal(t, 1, outboxCountByType(t, db, "node_updated"))
	assert.Equal(t, 0, outboxCountByType(t, db, "check_delayed"))

	// The FAILED announcement must carry the row's task_id (identity fallback).
	var payload []byte
	require.NoError(t, db.QueryRow(
		`SELECT payload FROM execution_outbox WHERE event_type='task_status_updated' LIMIT 1`).Scan(&payload))
	var got struct {
		TaskID     string `json:"task_id"`
		ScheduleID string `json:"schedule_id"`
		Status     string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(payload, &got))
	assert.Equal(t, taskID.String(), got.TaskID, "FAILED announcement uses the row's task_id")
	assert.Equal(t, scheduleID.String(), got.ScheduleID)
	assert.Equal(t, "FAILED", got.Status)

	// Corrupt job_params carry no task budget, so the default budget is reported spent.
	assert.Equal(t, pkgevents.DefaultTaskMaxRetries, failedAnnouncementRetryCount(t, db))
}

func mustDeploymentID(t *testing.T, db *sqlx.DB, taskID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	require.NoError(t, db.QueryRow(`SELECT id FROM deployments WHERE task_id=$1`, taskID).Scan(&id))
	return id
}

// seedDeployableAt inserts a deployable pending row due at the given time.
func seedDeployableAt(t *testing.T, db *sqlx.DB, nextAttempt time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	payload, err := json.Marshal(serialization.DeployTaskFromDomain(command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public",
		TableName: "orders", JobName: "dbt-public-orders", NodeType: "dbt-model",
		ImageTag: "sha-abc", TaskMaxRetries: 2,
	}))
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO deployments (id, task_id, schedule_id, job_params, max_retries, retry_count, next_attempt_at)
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
	fk := &fakeDeployer{}
	disp := deployer.NewDispatcher(db, fk, factory, aggRepoFactory, admissionFactory, 50, testLogger(), deployer.DispatcherConfig{})

	require.Error(t, disp.ProcessBatch(context.Background()), "second row's Save error surfaces")

	var olderStatus, newerStatus string
	require.NoError(t, db.QueryRow(`SELECT status FROM deployments WHERE id=$1`, older).Scan(&olderStatus))
	require.NoError(t, db.QueryRow(`SELECT status FROM deployments WHERE id=$1`, newer).Scan(&newerStatus))
	assert.Equal(t, "starting", olderStatus, "first deployment committed in its own transaction")
	assert.Equal(t, "reserved", newerStatus, "second deployment's launch rolled back — its reservation stands until the reconciler returns it to pending")
	assert.Equal(t, 2, fk.deployCalls)
}

func TestDispatcher_CapCountsInFlightRows(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	for i := 0; i < 3; i++ {
		seedRunning(t, db)
	}
	for i := 0; i < 5; i++ {
		seedJob(t, db, 3, 0)
	}
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 4).ProcessBatch(context.Background()))

	assert.Equal(t, 1, fk.calls(), "three running rows leave one slot under a cap of four")
	assert.Equal(t, 1, countByStatus(t, db, "starting"))
	assert.Equal(t, 4, countByStatus(t, db, "pending"))

	require.NoError(t, newTestDispatcher(db, fk, 2).ProcessBatch(context.Background()))
	assert.Equal(t, 1, fk.calls(), "a cap lowered below what is in flight admits nothing")
}

// pausingAdmission holds a claim between its count and its reservation until
// resume closes (or a second passes), so a test can run a second claimer in
// that window.
type pausingAdmission struct {
	repository.AdmissionRepository
	counted chan<- struct{}
	resume  <-chan struct{}
}

func (p pausingAdmission) CountInFlight(ctx context.Context) (int, error) {
	n, err := p.AdmissionRepository.CountInFlight(ctx)
	p.counted <- struct{}{}
	select {
	case <-p.resume:
	case <-time.After(time.Second):
	}
	return n, err
}

func TestDispatcher_ConcurrentClaimsNeverExceedTheCap(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	for i := 0; i < 10; i++ {
		seedJob(t, db, 3, 0)
	}
	fk := &fakeDeployer{}
	counted := make(chan struct{}, 1)
	resume := make(chan struct{})
	pausing := func(exec outbox.Executor) repository.AdmissionRepository {
		return pausingAdmission{postgres.NewAdmissionRepository(exec, testLogger()), counted, resume}
	}
	first := newTestDispatcherWith(db, fk, 3, deployer.DispatcherConfig{}, pausing)
	second := newTestDispatcherWith(db, fk, 3, deployer.DispatcherConfig{}, admissionFactory)

	firstDone := make(chan error, 1)
	go func() { firstDone <- first.ProcessBatch(context.Background()) }()
	<-counted // the first claimer has counted 0 in flight and holds the scope
	secondDone := make(chan error, 1)
	go func() {
		err := second.ProcessBatch(context.Background())
		close(resume)
		secondDone <- err
	}()
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)

	// Without the scope lock the second claimer counts 0 too, reserves three
	// rows while the first waits, and the first then reserves three more.
	assert.Equal(t, 3, countByStatus(t, db, "starting"))
	assert.Equal(t, 7, countByStatus(t, db, "pending"))
	assert.Equal(t, 3, fk.calls())
}

func TestDispatcher_TransientFailureFreesTheSlot(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{deployErr: errors.New("api server timeout")}

	require.NoError(t, newTestDispatcher(db, fk, 1).ProcessBatch(context.Background()))

	assert.Equal(t, 1, fk.calls(), "the row is not re-reserved in the same pass")
	var status string
	var retry int
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM deployments WHERE id = $1`, id).Scan(&status, &retry))
	assert.Equal(t, "pending", status)
	assert.Equal(t, 1, retry)
	assert.Equal(t, 0, countByStatus(t, db, "reserved"))
}

// TestDispatcher_ReadmitsAReturnedReservation: a reservation whose dispatcher
// died goes back to pending, and the next claim admits it like any queued row.
// If the dead launcher had already created the Job, the k8s adapter reports
// AlreadyExists as success (TestCreateJob_AlreadyExistsIsSuccess).
func TestDispatcher_ReadmitsAReturnedReservation(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	_, err := db.Exec(`UPDATE deployments SET status = 'reserved', state_changed_at = NOW() - interval '5 minutes' WHERE id = $1`, id)
	require.NoError(t, err)
	returned, err := postgres.NewAdmissionRepository(db, testLogger()).ReturnStaleReserved(context.Background(), 2*time.Minute)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{id}, returned)
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 5).ProcessBatch(context.Background()))

	assert.Equal(t, "starting", countStatusOf(t, db, id))
	assert.Equal(t, 1, fk.calls())
	assert.Equal(t, 1, outboxCountByType(t, db, "check_delayed"))
}

type chanWaker chan struct{}

func (c chanWaker) Wake() <-chan struct{} { return c }

func TestDispatcher_WakesWithoutWaitingForTheTick(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	fk := &fakeDeployer{}
	w := make(chanWaker, 1)
	d := newTestDispatcherWith(db, fk, 5, deployer.DispatcherConfig{Tick: time.Hour, Waker: w}, admissionFactory)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	defer func() { cancel(); <-done }()

	time.Sleep(300 * time.Millisecond) // the start-up pass finds nothing
	id := seedJob(t, db, 3, 0)
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, "pending", countStatusOf(t, db, id), "no tick within an hour, so nothing runs until woken")

	w <- struct{}{}
	require.Eventually(t, func() bool { return countStatusOf(t, db, id) == "starting" }, 5*time.Second, 50*time.Millisecond)
}
