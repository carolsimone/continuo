//go:build integration

package deployer_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
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

// absentJobs is a JobLookup that finds no Job of any name.
type absentJobs struct{}

func (absentJobs) ObserveLive(context.Context, string) (model.JobObservation, error) {
	return model.JobObservation{}, nil
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

func cancelledFactory(exec outbox.Executor) repository.CancelledRunsRepository {
	return postgres.NewCancelledRunsRepository(exec)
}

// newTestDispatcher builds a Dispatcher whose repo factories are the real
// Postgres adapters bound to the claim or launch tx, with a fake Deployer.
func newTestDispatcher(db *sqlx.DB, fk *fakeDeployer, maxConcurrent int) *deployer.Dispatcher {
	return newTestDispatcherWith(db, fk, maxConcurrent, deployer.DispatcherConfig{}, admissionFactory)
}

// newTestDispatcherWith builds a Dispatcher with an explicit config and
// admission repository factory.
func newTestDispatcherWith(db *sqlx.DB, fk *fakeDeployer, maxConcurrent int, cfg deployer.DispatcherConfig, admission deployer.AdmissionRepoFactory) *deployer.Dispatcher {
	return deployer.NewDispatcher(db, fk, absentJobs{}, repoFactory, aggRepoFactory, admission, cancelledFactory, maxConcurrent, testLogger(), cfg)
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
		TaskID: uuid.New().String(), RunID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public",
		TableName: "orders", JobName: name, NodeType: "dbt-model",
		ImageTag: "sha-abc", Attempt: 0,
	}))
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO deployments (id, task_id, run_id, job_params, max_retries, retry_count, next_attempt_at, tenant_id)
		 VALUES ($1, $2, $3, $4, $5, $6, NOW() - interval '1 minute', 'default')`,
		id, uuid.New(), uuid.New(), payload, maxRetries, retryCount)
	require.NoError(t, err)
	return id
}

// failedReport decodes the task.status.updated:v2 report the dispatcher wrote
// for an attempt whose Job could not be created.
func failedReport(t *testing.T, db *sqlx.DB) pkgevents.TaskStatusReport {
	t.Helper()
	var payload []byte
	require.NoError(t, db.QueryRow(
		`SELECT payload FROM execution_outbox WHERE event_type = $1 LIMIT 1`, event.EventTypeTaskStatusReport).Scan(&payload))
	_, report, err := outbox.UnwrapTenant(payload)
	require.NoError(t, err)
	var got pkgevents.TaskStatusReport
	require.NoError(t, json.Unmarshal(report, &got))
	return got
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

func TestDispatcher_SuccessMarksStartedAndWritesNothing(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 0)
	fk := &fakeDeployer{}

	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	assert.Equal(t, 1, fk.deployCalls)
	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM deployments WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "starting", status)
	// The observer follows the created Job and announces RUNNING once its main
	// container starts; the deploy path writes nothing.
	var outboxRows int
	require.NoError(t, db.Get(&outboxRows, `SELECT count(*) FROM execution_outbox`))
	assert.Zero(t, outboxRows)
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
	assert.Equal(t, 0, outboxCountByType(t, db, event.EventTypeTaskStatusReport), "no report on transient retry")
}

func TestDispatcher_BudgetExhaustedReportsCreateFailed(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id := seedJob(t, db, 3, 2)
	fk := &fakeDeployer{deployErr: errors.New("apiserver down")}

	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM deployments WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "failed", status)
	assert.Equal(t, 1, outboxCountByType(t, db, event.EventTypeTaskStatusReport))
	assert.Equal(t, 0, outboxCountByType(t, db, "node_updated"), "a deploy failure writes its report only")

	got := failedReport(t, db)
	assert.Equal(t, pkgmodel.AttemptStatusFailed, got.Status)
	assert.Equal(t, "infra", got.FailureClass, "Kubernetes refused the Job on every try: run-controller may retry it")
	assert.Equal(t, "create_failed", got.FailureReason)
	assert.Equal(t, "apiserver down", got.ErrorMessage)
}

// The error message reported for an attempt whose Job could not be created is
// cut to ErrorMessageMaxLen, as the observer cuts a failed attempt's.
func TestDispatcher_ACreationFailureReportsATruncatedMessage(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	seedJob(t, db, 3, 2)
	long := strings.Repeat("apiserver down ", 10)
	fk := &fakeDeployer{deployErr: errors.New(long)}
	disp := newTestDispatcherWith(db, fk, 50, deployer.DispatcherConfig{ErrorMessageMaxLen: 64}, admissionFactory)

	require.NoError(t, disp.ProcessBatch(context.Background()))

	got := failedReport(t, db)
	assert.Equal(t, "create_failed", got.FailureReason)
	assert.Equal(t, long[:64]+"...[truncated]", got.ErrorMessage)
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
	got := failedReport(t, db)
	assert.Equal(t, "invalid", got.FailureClass, "a dispatch that cannot run is not retried")
	assert.Equal(t, "malformed_dispatch", got.FailureReason)
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
	runID := uuid.New()
	// Valid JSONB, but a JSON string — cannot unmarshal into DeployTask.
	_, err := db.Exec(
		`INSERT INTO deployments (id, task_id, run_id, job_params, max_retries, retry_count, next_attempt_at, tenant_id)
		 VALUES ($1, $2, $3, '"corrupt"'::jsonb, 3, 0, NOW() - interval '1 minute', 'default')`,
		uuid.New(), taskID, runID)
	require.NoError(t, err)

	fk := &fakeDeployer{}
	require.NoError(t, newTestDispatcher(db, fk, 50).ProcessBatch(context.Background()))

	assert.Equal(t, 0, fk.calls(), "deploy never attempted when payload is corrupt")
	var reason string
	require.NoError(t, db.Get(&reason, `SELECT facts->>'failure_reason' FROM deployments WHERE task_id = $1`, taskID))
	assert.Equal(t, "malformed_dispatch", reason, "a dispatch that does not decode cannot run")

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM deployments WHERE id=$1`,
		// the row id differs from taskID; look it up by task_id
		mustDeploymentID(t, db, taskID)).Scan(&status))
	assert.Equal(t, "failed", status)
	assert.Equal(t, 1, outboxCountByType(t, db, event.EventTypeTaskStatusReport))
	assert.Equal(t, 0, outboxCountByType(t, db, "node_updated"), "a deploy failure writes its report only")

	// The report carries the row's task_id and run (identity fallback).
	got := failedReport(t, db)
	assert.Equal(t, taskID.String(), got.TaskID, "the report uses the row's task_id")
	assert.Equal(t, runID.String(), got.RunID)
	assert.Equal(t, pkgmodel.AttemptStatusFailed, got.Status)
	assert.Equal(t, "malformed_dispatch", got.FailureReason)
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
		TaskID: uuid.New().String(), RunID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public",
		TableName: "orders", JobName: "dbt-public-orders", NodeType: "dbt-model",
		ImageTag: "sha-abc",
	}))
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO deployments (id, task_id, run_id, job_params, max_retries, retry_count, next_attempt_at, tenant_id)
		 VALUES ($1, $2, $3, $4, 3, 0, $5, 'default')`,
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
	disp := deployer.NewDispatcher(db, fk, absentJobs{}, factory, aggRepoFactory, admissionFactory, cancelledFactory, 50, testLogger(), deployer.DispatcherConfig{})

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

// A deployment whose Job cannot be created records why: Kubernetes refused it
// on every try, or the dispatch itself cannot run.
func TestDispatcher_RecordsWhyTheJobCouldNotBeCreated(t *testing.T) {
	for name, c := range map[string]struct {
		err        error
		retryCount int
		want       string
	}{
		"refused on every try": {err: errors.New("apiserver down"), retryCount: 2, want: "create_failed"},
		"a name collision":     {err: errors.Join(deploy.ErrJobNameCollision, pkgevents.ErrPermanent), want: "job_name_collision"},
		"an undeclared type":   {err: errors.Join(deploy.ErrInvalidNodeType, pkgevents.ErrPermanent), want: "invalid_node_type"},
		"no timeout":           {err: errors.Join(deploy.ErrMalformedDispatch, pkgevents.ErrPermanent), want: "malformed_dispatch"},
		"another field":        {err: errors.Join(errors.New("image_tag missing"), pkgevents.ErrPermanent), want: "malformed_dispatch"},
	} {
		t.Run(name, func(t *testing.T) {
			db, cleanup := setupPostgres(t)
			defer cleanup()
			id := seedJob(t, db, 3, c.retryCount)

			require.NoError(t, newTestDispatcher(db, &fakeDeployer{deployErr: c.err}, 50).ProcessBatch(context.Background()))

			var reason string
			require.NoError(t, db.Get(&reason, `SELECT facts->>'failure_reason' FROM deployments WHERE id = $1`, id))
			assert.Equal(t, c.want, reason)
			assert.Equal(t, "failed", countStatusOf(t, db, id))
		})
	}
}

// errAPITimeout is how the Kubernetes API answers a request whose answer is
// lost: the request may still have been carried out.
var errAPITimeout = errors.New("the server was unable to return a response in the time allotted, but may still be processing the request")

// lossyAPI is a Kubernetes API whose answers can be lost, serving as both the
// Deployer and the JobLookup. Deploy acts as the k8s adapter does: it reads
// the Job of the name first, adopts it when it runs the same task and
// attempt, refuses it as a collision when it runs another, and otherwise
// creates it. A creation stores the Job yet answers with a timeout, unless
// refuseCreates is set, when it stores nothing. Every read times out while
// readsDown is set, and so do the next failReads reads.
type lossyAPI struct {
	mu            sync.Mutex
	jobs          map[string]model.JobObservation
	refuseCreates bool
	readsDown     bool
	failReads     int
	liveReads     int
}

func newLossyAPI() *lossyAPI { return &lossyAPI{jobs: map[string]model.JobObservation{}} }

func (a *lossyAPI) read(jobName string) (model.JobObservation, error) {
	if a.failReads > 0 {
		a.failReads--
		return model.JobObservation{}, errAPITimeout
	}
	if a.readsDown {
		return model.JobObservation{}, errAPITimeout
	}
	return a.jobs[jobName], nil
}

func (a *lossyAPI) Deploy(_ context.Context, spec deploy.JobSpec) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	job, err := a.read(spec.JobName)
	if err != nil {
		return fmt.Errorf("failed to check if job exists: %w", err)
	}
	if job.JobPresent {
		if job.RunsAttempt(spec.TaskID, spec.Attempt) {
			return nil
		}
		return errors.Join(deploy.ErrJobNameCollision, pkgevents.ErrPermanent)
	}
	if a.refuseCreates {
		return errors.New("admission webhook unavailable")
	}
	a.jobs[spec.JobName] = model.JobObservation{JobPresent: true, JobTaskID: spec.TaskID, JobAttempt: strconv.Itoa(spec.Attempt)}
	return errAPITimeout
}

func (a *lossyAPI) DeployValidation(context.Context, deploy.ValidationJobSpec) error {
	return errors.New("lossyAPI creates no release leg")
}
func (a *lossyAPI) DeploySeedBuild(context.Context, deploy.ValidationJobSpec) error {
	return errors.New("lossyAPI creates no release leg")
}
func (a *lossyAPI) DeployCompile(context.Context, deploy.ValidationJobSpec) error {
	return errors.New("lossyAPI creates no release leg")
}

func (a *lossyAPI) ObserveLive(_ context.Context, jobName string) (model.JobObservation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.liveReads++
	return a.read(jobName)
}

// setJob puts a Job of jobName running taskID's attempt in the API.
func (a *lossyAPI) setJob(jobName, taskID string, attempt int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.jobs[jobName] = model.JobObservation{JobPresent: true, JobTaskID: taskID, JobAttempt: strconv.Itoa(attempt)}
}

func (a *lossyAPI) set(f func(*lossyAPI)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	f(a)
}

// seedAttempt inserts a pending row of attempt 1 of a new task, due one minute
// ago, that has already failed retryCount creation tries of its budget of 3,
// and returns its id and command.
func seedAttempt(t *testing.T, db *sqlx.DB, retryCount int) (uuid.UUID, command.DeployTask) {
	t.Helper()
	id := uuid.New()
	cmd := command.DeployTask{TenantID: "default",
		TaskID: uuid.NewString(), RunID: uuid.NewString(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public",
		TableName: "orders", JobName: "dbt-public-orders-" + uuid.NewString()[:8] + "-r1", NodeType: "dbt-model",
		ImageTag: "sha-abc", Operation: "run", Attempt: 1, TimeoutSeconds: 3600,
	}
	payload, err := json.Marshal(serialization.DeployTaskFromDomain(cmd))
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO deployments (id, task_id, run_id, job_params, max_retries, retry_count, next_attempt_at, tenant_id)
		 VALUES ($1, $2, $3, $4, 3, $5, NOW() - interval '1 minute', 'default')`,
		id, cmd.TaskID, cmd.RunID, payload, retryCount)
	require.NoError(t, err)
	return id, cmd
}

// dueNow makes a row rescheduled after a transient failure due again.
func dueNow(t *testing.T, db *sqlx.DB, id uuid.UUID) {
	t.Helper()
	_, err := db.Exec(`UPDATE deployments SET next_attempt_at = NOW() - interval '1 minute' WHERE id = $1`, id)
	require.NoError(t, err)
}

// retryCountOf reads a row's retry_count.
func retryCountOf(t *testing.T, db *sqlx.DB, id uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, db.Get(&n, `SELECT retry_count FROM deployments WHERE id = $1`, id))
	return n
}

// A creation whose answer is lost may have created the Job. While the Job
// cannot be read, the last failed try reports nothing: the deployment stays
// reserved, as it was, and the reconciler returns it to the queue. Once the
// API answers again, the Job the lost creation made is adopted and no second
// Job is created.
func TestDispatcher_ALostCreationAnswerIsReconciledUntilTheJobIsRead(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	id, cmd := seedAttempt(t, db, 0)
	api := newLossyAPI()
	disp := deployer.NewDispatcher(db, api, api, repoFactory, aggRepoFactory, admissionFactory, cancelledFactory, 50, testLogger(), deployer.DispatcherConfig{})

	// The first try creates the Job, but its answer is lost.
	require.NoError(t, disp.ProcessBatch(ctx))
	require.Equal(t, "pending", countStatusOf(t, db, id))
	require.True(t, api.jobs[cmd.JobName].RunsAttempt(cmd.TaskID, cmd.Attempt), "the lost creation stored the Job")

	// The API stops answering reads: the second and the last try fail.
	api.set(func(a *lossyAPI) { a.readsDown = true })
	dueNow(t, db, id)
	require.NoError(t, disp.ProcessBatch(ctx))
	require.Equal(t, 2, retryCountOf(t, db, id))
	dueNow(t, db, id)
	require.Error(t, disp.ProcessBatch(ctx), "the last try cannot tell whether the Job exists")

	assert.Equal(t, "reserved", countStatusOf(t, db, id), "the deployment stays as it was")
	assert.Equal(t, 2, retryCountOf(t, db, id), "the unconfirmed failure is not registered")
	assert.Zero(t, outboxCountByType(t, db, event.EventTypeTaskStatusReport), "no failure is reported while the Job may exist")

	// The reconciler returns the reservation; the API answers again.
	_, err := db.Exec(`UPDATE deployments SET state_changed_at = NOW() - interval '5 minutes' WHERE id = $1`, id)
	require.NoError(t, err)
	require.NoError(t, deployer.NewReconciler(db, admissionFactory, testLogger(), deployer.ReconcilerConfig{}).ReconcileOnce(ctx))
	api.set(func(a *lossyAPI) { a.readsDown = false })
	require.NoError(t, disp.ProcessBatch(ctx))

	assert.Equal(t, "starting", countStatusOf(t, db, id), "the Job the lost creation made is adopted")
	assert.Zero(t, outboxCountByType(t, db, event.EventTypeTaskStatusReport))
	assert.Len(t, api.jobs, 1, "no second Job is created")
}

// When the last try fails but the live read that follows finds the attempt's
// Job, the Job is adopted instead of the attempt being reported failed.
func TestDispatcher_TheLastFailedTryAdoptsTheJobALiveReadFinds(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	id, cmd := seedAttempt(t, db, 2)
	api := newLossyAPI()
	api.setJob(cmd.JobName, cmd.TaskID, cmd.Attempt)
	api.set(func(a *lossyAPI) { a.failReads = 1 }) // the try's own read of the Job times out

	require.NoError(t, deployer.NewDispatcher(db, api, api, repoFactory, aggRepoFactory, admissionFactory, cancelledFactory, 50,
		testLogger(), deployer.DispatcherConfig{}).ProcessBatch(context.Background()))

	assert.Equal(t, 1, api.liveReads)
	assert.Equal(t, "starting", countStatusOf(t, db, id))
	var deployed bool
	require.NoError(t, db.Get(&deployed, `SELECT deployed_at IS NOT NULL FROM deployments WHERE id = $1`, id))
	assert.True(t, deployed)
	assert.Zero(t, outboxCountByType(t, db, event.EventTypeTaskStatusReport))
}

// The last failed try reports the attempt failed once a live read shows what
// holds the Job's name: create_failed when no Job exists, job_name_collision
// when another task's or attempt's Job does.
func TestDispatcher_TheLastFailedTryIsReportedFromALiveRead(t *testing.T) {
	for name, c := range map[string]struct {
		setup      func(api *lossyAPI, cmd command.DeployTask)
		wantReason string
		wantClass  string
	}{
		"no Job": {
			setup:      func(api *lossyAPI, _ command.DeployTask) { api.set(func(a *lossyAPI) { a.refuseCreates = true }) },
			wantReason: "create_failed", wantClass: "infra",
		},
		"another task's Job": {
			setup: func(api *lossyAPI, cmd command.DeployTask) {
				api.setJob(cmd.JobName, uuid.NewString(), cmd.Attempt)
				api.set(func(a *lossyAPI) { a.failReads = 1 })
			},
			wantReason: "job_name_collision", wantClass: "invalid",
		},
	} {
		t.Run(name, func(t *testing.T) {
			db, cleanup := setupPostgres(t)
			defer cleanup()
			id, cmd := seedAttempt(t, db, 2)
			api := newLossyAPI()
			c.setup(api, cmd)

			require.NoError(t, deployer.NewDispatcher(db, api, api, repoFactory, aggRepoFactory, admissionFactory, cancelledFactory, 50,
				testLogger(), deployer.DispatcherConfig{}).ProcessBatch(context.Background()))

			assert.Equal(t, 1, api.liveReads, "the Job is read before the attempt is reported")
			assert.Equal(t, "failed", countStatusOf(t, db, id))
			got := failedReport(t, db)
			assert.Equal(t, c.wantReason, got.FailureReason)
			assert.Equal(t, c.wantClass, got.FailureClass)
		})
	}
}

// An attempt whose Job name another task's running Job holds fails as
// job_name_collision without standing for that Job: both lookups by the name
// still return the deployment whose Job it is.
func TestDispatcher_ACollidingAttemptLeavesTheJobToItsOwner(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	name := "dbt-public-orders-" + uuid.NewString()[:8]
	owner := seedJobNamed(t, db, name, 3, 0)
	_, err := db.Exec(`UPDATE deployments SET status = 'running', deployed_at = NOW() - interval '1 minute',
		created_at = NOW() - interval '1 minute' WHERE id = $1`, owner)
	require.NoError(t, err)
	colliding := seedJobNamed(t, db, name, 3, 0)

	require.NoError(t, newTestDispatcher(db, &fakeDeployer{deployErr: errors.Join(deploy.ErrJobNameCollision, pkgevents.ErrPermanent)}, 50).
		ProcessBatch(ctx))

	var reason string
	require.NoError(t, db.Get(&reason, `SELECT facts->>'failure_reason' FROM deployments WHERE id = $1`, colliding))
	require.Equal(t, "job_name_collision", reason)
	got, err := postgres.NewDeploymentsRepository(db, testLogger()).LoadByJobName(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, owner, got.ID())
	got, err = postgres.NewObservedDeployments(db, testLogger()).GetByJobName(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, owner, got.ID())
}

// fencingAdmission fences run, on another connection, once the claim has
// reserved its rows and before the claim commits: the launch then finds a
// reserved row of a fenced run whose fence never ended it.
type fencingAdmission struct {
	repository.AdmissionRepository
	db  *sqlx.DB
	run uuid.UUID
}

func (f fencingAdmission) ReserveNext(ctx context.Context, limit int) ([]uuid.UUID, error) {
	ids, err := f.AdmissionRepository.ReserveNext(ctx, limit)
	if err != nil {
		return nil, err
	}
	if _, err := f.db.ExecContext(ctx, `INSERT INTO cancelled_runs (run_id) VALUES ($1)`, f.run); err != nil {
		return nil, err
	}
	return ids, nil
}

// countedStops is a ports.StopMetrics that counts the attempts reported
// stopped.
type countedStops struct{ stopped int }

func (c *countedStops) AttemptStopped() { c.stopped++ }
func (*countedStops) JobReaped(string)  {}

// The launch reads the fence under the row lock: a reserved row of a run
// fenced after it was claimed creates no Job, ends done with the outcome
// stopped and is reported stopped, so it never stays reserved.
func TestLaunch_AFencedReservedRowEndsStoppedWithoutAJob(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	runID := uuid.New()
	cmd := command.DeployTask{TenantID: "default", TaskID: uuid.NewString(), RunID: runID.String(), ScheduleName: "daily",
		ServiceName: "dbt", SchemaName: "public", TableName: "orders", JobName: "dbt-public-orders-" + runID.String()[:8],
		NodeType: "dbt-model", ImageTag: "sha-abc", Operation: "run", TimeoutSeconds: 3600}
	dep := model.NewDeployment(cmd, nil, time.Now().Add(-time.Minute))
	added, err := repoFactory(db).AddAttempt(ctx, dep)
	require.NoError(t, err)
	require.True(t, added)
	fk := &fakeDeployer{}
	fencing := func(exec outbox.Executor) repository.AdmissionRepository {
		return fencingAdmission{AdmissionRepository: admissionFactory(exec), db: db, run: runID}
	}

	stops := &countedStops{}
	lookup := &liveAPI{}
	require.NoError(t, deployer.NewDispatcher(db, fk, lookup, repoFactory, aggRepoFactory, fencing, cancelledFactory, 50,
		testLogger(), deployer.DispatcherConfig{StopMetrics: stops}).ProcessBatch(ctx))

	assert.Zero(t, fk.calls(), "no Job is created for a fenced run")
	assert.Equal(t, 1, lookup.reads, "the API is read for a Job of the attempt before it ends stopped")
	assert.Equal(t, 1, stops.stopped, "the stopped attempt is counted once its launch commits")
	got, found, err := repoFactory(db).LoadByTaskAttempt(ctx, cmd.TaskID, 0)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, model.StatusDone, got.Status())
	assert.Equal(t, model.OutcomeStopped, got.Outcome())
	assert.NotNil(t, got.Facts().StopRequestedAt)
	assert.Nil(t, got.DeployedAt())
	require.Equal(t, 1, outboxCountByType(t, db, event.EventTypeTaskStatusReport))
	report := failedReport(t, db)
	assert.Equal(t, pkgmodel.AttemptStatusStopped, report.Status)
	assert.Empty(t, report.FailureReason)
	assert.Equal(t, cmd.JobName, report.JobName)
}

// fencedLaunch is a reserved attempt of a run fenced after the claim reserved
// it, and the dispatcher that launches it.
type fencedLaunch struct {
	db    *sqlx.DB
	cmd   command.DeployTask
	fk    *fakeDeployer
	stops *countedStops
	term  *committedDeletes
}

// committedDeletes is a ports.JobTerminator that records each Job it is asked
// to delete with the status its row has, read on another connection, at that
// moment.
type committedDeletes struct {
	db       *sqlx.DB
	names    []string
	statuses []string
}

func (c *committedDeletes) DeleteJob(ctx context.Context, name string) error {
	var status string
	if err := c.db.GetContext(ctx, &status, `SELECT status FROM deployments WHERE job_name = $1`, name); err != nil {
		return err
	}
	c.names, c.statuses = append(c.names, name), append(c.statuses, status)
	return nil
}

// liveAPI is a JobLookup answering with the Jobs it maps by name, or err, and
// counting reads.
type liveAPI struct {
	jobs  map[string]model.JobObservation
	err   error
	reads int
}

func (l *liveAPI) ObserveLive(_ context.Context, name string) (model.JobObservation, error) {
	l.reads++
	return l.jobs[name], l.err
}

func newFencedLaunch(t *testing.T) *fencedLaunch {
	t.Helper()
	db, cleanup := setupPostgres(t)
	t.Cleanup(cleanup)
	runID := uuid.New()
	cmd := command.DeployTask{TenantID: "default", TaskID: uuid.NewString(), RunID: runID.String(), ScheduleName: "daily",
		ServiceName: "dbt", SchemaName: "public", TableName: "orders", JobName: "dbt-public-orders-" + runID.String()[:8],
		NodeType: "dbt-model", ImageTag: "sha-abc", Operation: "run", TimeoutSeconds: 3600}
	added, err := repoFactory(db).AddAttempt(context.Background(), model.NewDeployment(cmd, nil, time.Now().Add(-time.Minute)))
	require.NoError(t, err)
	require.True(t, added)
	return &fencedLaunch{db: db, cmd: cmd, fk: &fakeDeployer{}, stops: &countedStops{}, term: &committedDeletes{db: db}}
}

// run claims the attempt, fences its run before the claim commits, and
// launches it, reading the attempt's Job from lookup.
func (f *fencedLaunch) run(lookup *liveAPI) error {
	runID := uuid.MustParse(f.cmd.RunID)
	fencing := func(exec outbox.Executor) repository.AdmissionRepository {
		return fencingAdmission{AdmissionRepository: admissionFactory(exec), db: f.db, run: runID}
	}
	return deployer.NewDispatcher(f.db, f.fk, lookup, repoFactory, aggRepoFactory, fencing, cancelledFactory, 50, testLogger(),
		deployer.DispatcherConfig{StopMetrics: f.stops, Terminator: f.term}).ProcessBatch(context.Background())
}

func (f *fencedLaunch) row(t *testing.T) *model.Deployment {
	t.Helper()
	got, found, err := repoFactory(f.db).LoadByTaskAttempt(context.Background(), f.cmd.TaskID, 0)
	require.NoError(t, err)
	require.True(t, found)
	return got
}

// A reserved row of a fenced run whose Job already exists (a launch that
// created it was lost before its commit, and the row was claimed again)
// creates no Job and reports nothing yet: it adopts the Job as terminating
// with the stop recorded, and the Job is deleted once the launch commits, so
// the observer reports the attempt stopped once its pods are gone.
func TestLaunch_AFencedReservedRowWhoseJobExistsIsAdoptedTerminating(t *testing.T) {
	f := newFencedLaunch(t)
	started := time.Now().Add(-time.Minute)
	lookup := &liveAPI{jobs: map[string]model.JobObservation{f.cmd.JobName: {JobPresent: true, JobTaskID: f.cmd.TaskID,
		JobAttempt: "0", JobRunID: f.cmd.RunID, Pods: []model.PodObservation{{Name: "pod-1", Phase: model.PodRunning,
			Main: model.ContainerObservation{Name: "dbt-job", StartedAt: &started}}}}}}

	require.NoError(t, f.run(lookup))

	assert.Zero(t, f.fk.calls(), "no Job is created for a fenced run")
	assert.Equal(t, 1, lookup.reads)
	got := f.row(t)
	assert.Equal(t, model.StatusTerminating, got.Status())
	assert.NotNil(t, got.DeployedAt(), "the observer follows the adopted Job")
	assert.NotNil(t, got.Facts().StopRequestedAt)
	assert.False(t, got.HasOutcome())
	assert.Zero(t, outboxCountByType(t, f.db, event.EventTypeTaskStatusReport), "stopped is reported once the pods are gone")
	assert.Zero(t, f.stops.stopped)
	assert.Equal(t, []string{f.cmd.JobName}, f.term.names, "the adopted Job is deleted")
	assert.Equal(t, []string{string(model.StatusTerminating)}, f.term.statuses, "the Job is deleted after the launch commits")
}

// A reserved row of a fenced run whose attempt has no Job but a pod that has
// not finished writes nothing: the row stays reserved and nothing is
// reported while the pod can still run. The fence, which run-controller's next
// cancel sends again, stops it then.
func TestLaunch_AFencedReservedRowWithAnUnfinishedPodWritesNothing(t *testing.T) {
	f := newFencedLaunch(t)
	started := time.Now().Add(-time.Minute)
	lookup := &liveAPI{jobs: map[string]model.JobObservation{f.cmd.JobName: {Pods: []model.PodObservation{{Name: "pod-1",
		Phase: model.PodRunning, Main: model.ContainerObservation{Name: "dbt-job", StartedAt: &started}}}}}}

	require.NoError(t, f.run(lookup))

	assert.Zero(t, f.fk.calls())
	got := f.row(t)
	assert.Equal(t, model.StatusReserved, got.Status())
	assert.Nil(t, got.Facts().StopRequestedAt)
	assert.Zero(t, outboxCountByType(t, f.db, event.EventTypeTaskStatusReport))
	assert.Zero(t, f.stops.stopped)
	assert.Empty(t, f.term.names)
}

// A reserved row of a fenced run whose Job cannot be read is not stopped:
// the launch fails and its transaction rolls back, leaving the row reserved
// with nothing reported, since a Job of it may exist.
func TestLaunch_AFencedReservedRowWhoseJobCannotBeReadStaysReserved(t *testing.T) {
	f := newFencedLaunch(t)

	err := f.run(&liveAPI{err: errAPITimeout})

	require.ErrorIs(t, err, errAPITimeout)
	assert.Zero(t, f.fk.calls())
	assert.Equal(t, model.StatusReserved, f.row(t).Status())
	assert.Zero(t, outboxCountByType(t, f.db, event.EventTypeTaskStatusReport))
	assert.Zero(t, f.stops.stopped)
}
