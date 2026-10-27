//go:build integration

package handlers_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/k8s/watch"
	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/deploy"
	"github.com/carolsimone/continuo/execution-controller/domain/events"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/execution-controller/service/deployer"
	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	"github.com/carolsimone/continuo/execution-controller/service/ports"
	executortest "github.com/carolsimone/continuo/execution-controller/test"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stopCounter is a ports.StopMetrics that counts what it is told.
type stopCounter struct {
	mu      sync.Mutex
	stopped int
	reaped  []string
}

func (c *stopCounter) AttemptStopped() { c.mu.Lock(); c.stopped++; c.mu.Unlock() }
func (c *stopCounter) JobReaped(reason string) {
	c.mu.Lock()
	c.reaped = append(c.reaped, reason)
	c.mu.Unlock()
}

// deletions is a ports.JobTerminator that records the Jobs it is asked to
// delete, failing each with err.
type deletions struct {
	mu    sync.Mutex
	names []string
	err   error
}

func (d *deletions) DeleteJob(_ context.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.names = append(d.names, name)
	return d.err
}

// jobsOf is a JobCache, and a JobLookup, holding the Jobs it maps by name.
type jobsOf map[string]model.JobObservation

func (jobsOf) Synced() bool                               { return true }
func (j jobsOf) Observe(name string) model.JobObservation { return j[name] }
func (j jobsOf) JobNames() []string {
	names := make([]string, 0, len(j))
	for name := range j {
		names = append(names, name)
	}
	return names
}
func (j jobsOf) ObserveLive(_ context.Context, name string) (model.JobObservation, error) {
	return j[name], nil
}
func (j jobsOf) ObserveRun(context.Context, string) (model.RunObservation, error) {
	return model.NewRunObservation(j), nil
}

// runReads is a RunJobLookup answering with the Jobs it maps by name and
// counting its reads.
type runReads struct {
	jobs  jobsOf
	reads int
}

func (r *runReads) ObserveRun(context.Context, string) (model.RunObservation, error) {
	r.reads++
	return model.NewRunObservation(r.jobs), nil
}

// fenceFlow drives the fence of one run against the executor test database.
type fenceFlow struct {
	t       *testing.T
	db      *sqlx.DB
	logger  *slog.Logger
	repo    repository.DeploymentRepository
	runID   uuid.UUID
	now     time.Time
	metrics *stopCounter
	term    *deletions
}

func newFenceFlow(t *testing.T) *fenceFlow {
	t.Helper()
	db, cleanup := executortest.SetupPostgres(t)
	t.Cleanup(cleanup)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &fenceFlow{t: t, db: db, logger: logger, repo: postgres.NewDeploymentsRepository(db, logger),
		runID: uuid.New(), now: time.Now().UTC().Truncate(time.Microsecond), metrics: &stopCounter{}, term: &deletions{}}
}

// command is attempt attempt of a new task of the run.
func (f *fenceFlow) command(attempt int) command.DeployTask {
	taskID := uuid.NewString()
	return command.DeployTask{TaskID: taskID, RunID: f.runID.String(), ScheduleName: "daily",
		ServiceName: "dbt", SchemaName: "public", TableName: "t" + taskID[:8],
		JobName: "dbt-public-t" + taskID[:8] + "-" + f.runID.String()[:8], NodeType: "dbt-model",
		ImageTag: "sha-abc", Operation: "run", Attempt: attempt, TimeoutSeconds: 3600}
}

// attempt stores attempt 0 of a new task of the run, moved to status.
func (f *fenceFlow) attempt(status model.Status) *model.Deployment {
	f.t.Helper()
	ctx := context.Background()
	d := model.NewDeployment(f.command(0), nil, f.now.Add(-time.Minute))
	d.RecordReport(d.ReportOf(pkgmodel.AttemptStatusQueued, ""))
	added, err := f.repo.AddAttempt(ctx, d)
	require.NoError(f.t, err)
	require.True(f.t, added)
	steps := map[model.Status][]func() error{
		model.StatusPending:  nil,
		model.StatusReserved: {d.Reserve},
		model.StatusStarting: {d.Reserve, func() error { return d.MarkStarted(f.now.Add(-30 * time.Second)) }},
		model.StatusRunning: {d.Reserve, func() error { return d.MarkStarted(f.now.Add(-30 * time.Second)) },
			func() error { _, err := d.MarkRunning(); return err }},
		model.StatusTerminating: {d.Reserve, func() error { return d.MarkStarted(f.now.Add(-30 * time.Second)) },
			func() error {
				return d.BeginTermination(pkgmodel.FailureReasonStartTimeout, "no container of the pod had started")
			}},
	}
	for _, step := range steps[status] {
		require.NoError(f.t, step())
	}
	require.NoError(f.t, f.repo.Save(ctx, d))
	return d
}

func (f *fenceFlow) handler(runJobs ports.RunJobLookup) *handlers.RunCancelledHandler {
	return handlers.NewRunCancelledHandler(runJobs, f.term, f.metrics, f.logger)
}

func (f *fenceFlow) event(attempts ...events.AttemptRef) events.RunCancelled {
	return events.RunCancelled{RunID: f.runID, CancelledBy: "alice", Reason: "wrong window", Attempts: attempts}
}

// cancel applies evt in one transaction and commits it.
func (f *fenceFlow) cancel(h *handlers.RunCancelledHandler, evt events.RunCancelled) []string {
	f.t.Helper()
	ctx := context.Background()
	u := postgres.NewPostgresUnitOfWork(f.db, f.logger)
	require.NoError(f.t, u.Begin(ctx))
	names, err := h.Handle(ctx, u, evt, uuid.Nil)
	if err != nil {
		_ = u.Rollback()
		require.NoError(f.t, err)
	}
	require.NoError(f.t, u.Commit())
	return names
}

func (f *fenceFlow) reload(taskID string, attempt int) *model.Deployment {
	f.t.Helper()
	d, found, err := f.repo.LoadByTaskAttempt(context.Background(), taskID, attempt)
	require.NoError(f.t, err)
	require.True(f.t, found, "attempt %d of task %s is stored", attempt, taskID)
	return d
}

// reports are the task.status.updated:v2 payloads written for taskID, oldest first.
func (f *fenceFlow) reports(taskID string) []pkgevents.TaskStatusReport {
	f.t.Helper()
	var payloads [][]byte
	require.NoError(f.t, f.db.Select(&payloads, `SELECT payload FROM execution_outbox
		WHERE aggregate_id = $1 AND stream_name = $2 ORDER BY created_at, id`, taskID, streams.TaskStatusUpdatedV2))
	out := make([]pkgevents.TaskStatusReport, 0, len(payloads))
	for _, p := range payloads {
		var r pkgevents.TaskStatusReport
		require.NoError(f.t, json.Unmarshal(p, &r))
		out = append(out, r)
	}
	return out
}

func (f *fenceFlow) fenced() bool {
	f.t.Helper()
	var n int
	require.NoError(f.t, f.db.Get(&n, `SELECT count(*) FROM cancelled_runs WHERE run_id = $1`, f.runID))
	return n == 1
}

// A row that has no Job yet, pending or reserved, and whose attempt has no Job
// in the cache or the API, ends done with the outcome stopped, and its stopped
// report commits with the fence.
func TestRunCancelled_EndsPendingRowsAndReportsThemStopped(t *testing.T) {
	f := newFenceFlow(t)
	pending, reserved := f.attempt(model.StatusPending), f.attempt(model.StatusReserved)
	live := &runReads{}

	names := f.cancel(f.handler(live), f.event())

	assert.Empty(t, names, "no Job to delete")
	assert.Equal(t, 1, live.reads, "the run's Jobs and pods are read once, for every row, before the rows end stopped")
	assert.True(t, f.fenced())
	for _, d := range []*model.Deployment{pending, reserved} {
		got := f.reload(d.Command().TaskID, 0)
		assert.Equal(t, model.StatusDone, got.Status())
		assert.Equal(t, model.OutcomeStopped, got.Outcome())
		assert.NotNil(t, got.Facts().StopRequestedAt)
		reps := f.reports(d.Command().TaskID)
		require.Len(t, reps, 1)
		assert.Equal(t, pkgmodel.AttemptStatusStopped, reps[0].Status)
		assert.Empty(t, reps[0].FailureReason)
	}
	assert.Equal(t, 2, f.metrics.stopped)
	assert.Empty(t, f.term.names, "the handler deletes nothing itself")
}

// A row whose Job exists, starting or running, moves to terminating with the
// stop recorded, and its Job is returned for deletion after the commit; it is
// reported only once its pods are gone. A terminating row keeps the reason it
// was ended for and gains the stop; its Job is already being deleted.
func TestRunCancelled_MovesStartedRowsToTerminatingAndReturnsTheirJobs(t *testing.T) {
	f := newFenceFlow(t)
	starting, running, terminating := f.attempt(model.StatusStarting), f.attempt(model.StatusRunning), f.attempt(model.StatusTerminating)
	mine := f.runID
	f.runID = uuid.New()
	untouched := f.attempt(model.StatusRunning)
	f.runID = mine

	names := f.cancel(f.handler(noJobs{}), f.event())

	assert.ElementsMatch(t, []string{starting.JobName(), running.JobName()}, names)
	for _, d := range []*model.Deployment{starting, running, terminating} {
		got := f.reload(d.Command().TaskID, 0)
		assert.Equal(t, model.StatusTerminating, got.Status())
		assert.NotNil(t, got.Facts().StopRequestedAt)
		assert.Empty(t, f.reports(d.Command().TaskID), "reported once the pods are gone")
	}
	assert.Equal(t, pkgmodel.FailureReasonStartTimeout, f.reload(terminating.Command().TaskID, 0).Facts().Reason,
		"the first reason stands")
	assert.Equal(t, model.StatusRunning, f.reload(untouched.Command().TaskID, 0).Status(), "another run's row is untouched")
	assert.Zero(t, f.metrics.stopped)
}

// blockingDeployer creates a Job only once release closes, announcing on
// entered that the launch holds the row and is calling Kubernetes.
type blockingDeployer struct {
	entered chan struct{}
	release chan struct{}
	calls   int
}

func (b *blockingDeployer) Deploy(context.Context, deploy.JobSpec) error {
	b.calls++
	close(b.entered)
	<-b.release
	return nil
}
func (b *blockingDeployer) DeployValidation(context.Context, deploy.ValidationJobSpec) error {
	return nil
}
func (b *blockingDeployer) DeploySeedBuild(context.Context, deploy.ValidationJobSpec) error {
	return nil
}
func (b *blockingDeployer) DeployCompile(context.Context, deploy.ValidationJobSpec) error { return nil }

func (f *fenceFlow) dispatcher(dep deploy.Deployer) *deployer.Dispatcher {
	return deployer.NewDispatcher(f.db, dep, noJobs{},
		func(exec outbox.Executor) repository.DeploymentRepository {
			return postgres.NewDeploymentsRepository(exec, f.logger)
		},
		func(exec outbox.Executor) repository.ValidationAggregateRepository {
			return postgres.NewValidationAggregateRepository(exec)
		},
		func(exec outbox.Executor) repository.AdmissionRepository {
			return postgres.NewAdmissionRepository(exec, f.logger)
		},
		func(exec outbox.Executor) repository.CancelledRunsRepository {
			return postgres.NewCancelledRunsRepository(exec)
		},
		50, f.logger, deployer.DispatcherConfig{})
}

// waitForLockWaiter waits until a backend of the test database waits on a
// row lock.
func (f *fenceFlow) waitForLockWaiter() {
	f.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		require.NoError(f.t, f.db.Get(&n, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'
			  AND wait_event IN ('transactionid', 'tuple')`))
		if n > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.t.Fatal("no transaction waited on a row lock")
}

// A cancel that meets a launch in progress waits for the launch's row lock:
// the launch, which read no fence, creates the Job and commits the row
// starting; the fence then moves it to terminating and returns its Job, so the
// Job is deleted after the commit.
func TestRunCancelled_WaitsForALaunchThatHoldsTheRow(t *testing.T) {
	f := newFenceFlow(t)
	d := f.attempt(model.StatusPending)
	dep := &blockingDeployer{entered: make(chan struct{}), release: make(chan struct{})}
	launched := make(chan error, 1)
	go func() { launched <- f.dispatcher(dep).ProcessBatch(context.Background()) }()
	<-dep.entered
	// A failure below must not leave the launch holding its row: the
	// database cleanup that runs after this one would wait on it.
	var release sync.Once
	t.Cleanup(func() {
		release.Do(func() { close(dep.release) })
		if err := <-launched; err != nil {
			t.Errorf("the launch failed: %v", err)
		}
	})

	type result struct {
		names []string
		err   error
	}
	fenced := make(chan result, 1)
	go func() {
		ctx := context.Background()
		u := postgres.NewPostgresUnitOfWork(f.db, f.logger)
		if err := u.Begin(ctx); err != nil {
			fenced <- result{err: err}
			return
		}
		names, err := f.handler(noJobs{}).Handle(ctx, u, f.event(), uuid.Nil)
		if err != nil {
			_ = u.Rollback()
			fenced <- result{err: err}
			return
		}
		fenced <- result{names: names, err: u.Commit()}
	}()
	f.waitForLockWaiter()
	select {
	case r := <-fenced:
		t.Fatalf("the fence did not wait for the launch: %v %v", r.names, r.err)
	case <-time.After(200 * time.Millisecond):
	}

	release.Do(func() { close(dep.release) })
	r := <-fenced
	require.NoError(t, r.err)

	assert.Equal(t, 1, dep.calls, "the launch created its Job")
	assert.Equal(t, []string{d.JobName()}, r.names, "the Job the launch created is deleted")
	got := f.reload(d.Command().TaskID, 0)
	assert.Equal(t, model.StatusTerminating, got.Status())
	assert.NotNil(t, got.DeployedAt())
	assert.NotNil(t, got.Facts().StopRequestedAt)
	assert.Empty(t, f.reports(d.Command().TaskID), "nothing is reported until its pods are gone")
}

// A fence that locks a reserved row before its launch ends it stopped; the
// launch, which locks with SKIP LOCKED and reads only reserved rows, finds
// nothing to launch while the fence holds the row and after it commits.
func TestRunCancelled_EndsAReservedRowItLocksFirst(t *testing.T) {
	f := newFenceFlow(t)
	ctx := context.Background()
	d := f.attempt(model.StatusReserved)
	u := postgres.NewPostgresUnitOfWork(f.db, f.logger)
	require.NoError(t, u.Begin(ctx))
	defer func() { _ = u.Rollback() }()
	names, err := f.handler(noJobs{}).Handle(ctx, u, f.event(), uuid.Nil)
	require.NoError(t, err)
	assert.Empty(t, names)

	tx, err := f.db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	_, err = postgres.NewAdmissionRepository(tx, f.logger).LoadReserved(ctx, d.ID())
	assert.ErrorIs(t, err, sql.ErrNoRows, "the launch skips the row the fence holds")
	require.NoError(t, tx.Rollback())

	require.NoError(t, u.Commit())
	_, err = postgres.NewAdmissionRepository(f.db, f.logger).LoadReserved(ctx, d.ID())
	assert.ErrorIs(t, err, sql.ErrNoRows, "the row is no longer reserved")
	dep := &blockingDeployer{entered: make(chan struct{}), release: make(chan struct{})}
	require.NoError(t, f.dispatcher(dep).ProcessBatch(ctx))
	assert.Zero(t, dep.calls, "no Job is created")
	got := f.reload(d.Command().TaskID, 0)
	assert.Equal(t, model.StatusDone, got.Status())
	assert.Equal(t, model.OutcomeStopped, got.Outcome())
	reps := f.reports(d.Command().TaskID)
	require.Len(t, reps, 1)
	assert.Equal(t, pkgmodel.AttemptStatusStopped, reps[0].Status)
}

func ref(cmd command.DeployTask) events.AttemptRef {
	return events.AttemptRef{TaskID: cmd.TaskID, Attempt: cmd.Attempt, JobName: cmd.JobName}
}

// An attempt the cancel names that execution-controller never received, with
// no Job in the cache or the API, is stored done with the outcome stopped and
// reported stopped, so a dispatch of it that arrives later is answered with
// that report.
func TestRunCancelled_StoresANamedAttemptItNeverReceivedAsStopped(t *testing.T) {
	f := newFenceFlow(t)
	cmd := f.command(1)
	live := &runReads{}

	names := f.cancel(f.handler(live), f.event(ref(cmd)))

	assert.Empty(t, names)
	assert.Equal(t, 1, live.reads, "the API is read before the attempt is stored stopped")
	got := f.reload(cmd.TaskID, 1)
	assert.Equal(t, model.StatusDone, got.Status())
	assert.Equal(t, model.OutcomeStopped, got.Outcome())
	assert.Nil(t, got.DeployedAt())
	reps := f.reports(cmd.TaskID)
	require.Len(t, reps, 1)
	assert.Equal(t, pkgmodel.AttemptStatusStopped, reps[0].Status)
	assert.Equal(t, int32(1), reps[0].Attempt)
	assert.Equal(t, cmd.JobName, reps[0].JobName)
	assert.Equal(t, 1, f.metrics.stopped)
}

// An attempt the cancel names that has no row but whose Job exists (here only
// a live read shows it) is adopted as terminating with the stop recorded, and
// its Job is returned for deletion; the observer reports it once its pods are
// gone.
func TestRunCancelled_AdoptsAndStopsANamedAttemptsJobWithNoRow(t *testing.T) {
	f := newFenceFlow(t)
	cmd := f.command(0)
	live := jobsOf{cmd.JobName: {JobPresent: true, JobTaskID: cmd.TaskID, JobAttempt: "0"}}

	names := f.cancel(f.handler(live), f.event(ref(cmd)))

	assert.Equal(t, []string{cmd.JobName}, names)
	got := f.reload(cmd.TaskID, 0)
	assert.Equal(t, model.StatusTerminating, got.Status())
	assert.NotNil(t, got.DeployedAt(), "the observer follows the adopted Job")
	assert.NotNil(t, got.Facts().StopRequestedAt)
	assert.Empty(t, f.reports(cmd.TaskID))
	adopted, err := f.repo.LoadByJobName(context.Background(), cmd.JobName)
	require.NoError(t, err)
	assert.Equal(t, got.ID(), adopted.ID())
}

// An attempt the cancel names that has ended is answered with its last
// report, whatever it was: state is waiting for it, and the report may have
// been lost.
func TestRunCancelled_ResendsTheReportOfANamedAttemptThatEnded(t *testing.T) {
	f := newFenceFlow(t)
	ctx := context.Background()
	d := f.attempt(model.StatusRunning)
	finished := f.now.Add(-time.Second)
	exit := int32(0)
	d.Observe(model.AttemptFacts{StartedAt: &finished, FinishedAt: &finished, ExitCode: &exit})
	require.NoError(t, d.Finish("ok", f.now))
	d.RecordReport(d.ReportOf(pkgmodel.AttemptStatusSucceeded, ""))
	require.NoError(t, f.repo.Save(ctx, d))

	names := f.cancel(f.handler(noJobs{}), f.event(ref(d.Command())))

	assert.Empty(t, names)
	reps := f.reports(d.Command().TaskID)
	require.Len(t, reps, 1)
	assert.Equal(t, pkgmodel.AttemptStatusSucceeded, reps[0].Status)
	got := f.reload(d.Command().TaskID, 0)
	assert.Equal(t, "ok", got.Outcome(), "the attempt keeps how it ended")
	assert.Zero(t, f.metrics.stopped)
}

// A cancel delivered again, or re-sent, changes nothing already stopped: the
// fence is one row, an ended row is not reported again unless named, and a
// terminating row's Job is not returned again (the observer re-deletes a Job
// whose deletion did not take).
func TestRunCancelled_IsIdempotent(t *testing.T) {
	f := newFenceFlow(t)
	pending, running := f.attempt(model.StatusPending), f.attempt(model.StatusRunning)
	h := f.handler(noJobs{})

	first := f.cancel(h, f.event())
	second := f.cancel(h, f.event())

	assert.Equal(t, []string{running.JobName()}, first)
	assert.Empty(t, second)
	assert.True(t, f.fenced())
	assert.Len(t, f.reports(pending.Command().TaskID), 1, "the stopped report is written once")
	assert.Empty(t, f.reports(running.Command().TaskID))
	assert.Equal(t, model.StatusTerminating, f.reload(running.Command().TaskID, 0).Status())
	assert.Equal(t, 1, f.metrics.stopped)
}

// dispatchOf is state's dispatch of attempt 0 of a new task of the run.
func (f *fenceFlow) dispatchOf() events.TaskDispatch {
	cmd := f.command(0)
	return events.TaskDispatch{RunID: f.runID, TaskID: uuid.MustParse(cmd.TaskID), Attempt: 0,
		KnownStatus: pkgmodel.AttemptStatusDispatched, ScheduleName: cmd.ScheduleName, ServiceName: cmd.ServiceName,
		SchemaName: cmd.SchemaName, TableName: cmd.TableName, NodeType: cmd.NodeType, Operation: cmd.Operation,
		ImageTag: cmd.ImageTag, JobName: cmd.JobName, TimeoutSeconds: cmd.TimeoutSeconds}
}

// waitForAdvisoryWaiter reports whether a backend of the test database comes
// to wait on an advisory lock within a few seconds.
func (f *fenceFlow) waitForAdvisoryWaiter() bool {
	f.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		require.NoError(f.t, f.db.Get(&n, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND wait_event = 'advisory'`))
		if n > 0 {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// inTx runs fn in its own transaction on another connection and sends what
// it returned once the transaction committed (or fn failed).
func (f *fenceFlow) inTx(fn func(u *postgres.PostgresUnitOfWork) error) <-chan error {
	done := make(chan error, 1)
	go func() {
		ctx := context.Background()
		u := postgres.NewPostgresUnitOfWork(f.db, f.logger)
		if err := u.Begin(ctx); err != nil {
			done <- err
			return
		}
		if err := fn(u); err != nil {
			_ = u.Rollback()
			done <- err
			return
		}
		done <- u.Commit()
	}()
	return done
}

// waitOrNot returns what done sends within 200 ms, and whether it sent.
func waitOrNot(done <-chan error) (error, bool) {
	select {
	case err := <-done:
		return err, true
	case <-time.After(200 * time.Millisecond):
		return nil, false
	}
}

// A cancel that meets a dispatch of the run whose transaction has read no
// fence and stored the attempt pending, uncommitted, waits for it: the fence
// then sees the committed row and ends it stopped, so no pending row of a
// fenced run is left for state's next cancel to find.
func TestRunCancelled_WaitsForADispatchThatMayStoreAnAttemptOfTheRun(t *testing.T) {
	f := newFenceFlow(t)
	ctx := context.Background()
	evt := f.dispatchOf()
	u := postgres.NewPostgresUnitOfWork(f.db, f.logger)
	require.NoError(t, u.Begin(ctx))
	t.Cleanup(func() { _ = u.Rollback() })
	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, noJobs{}, f.metrics, f.logger).Handle(ctx, u, evt, uuid.Nil))

	fenced := f.inTx(func(fu *postgres.PostgresUnitOfWork) error {
		_, err := f.handler(noJobs{}).Handle(ctx, fu, f.event(), uuid.Nil)
		return err
	})
	assert.True(t, f.waitForAdvisoryWaiter(), "the fence waits for the dispatch's run lock")
	fenceErr, early := waitOrNot(fenced)
	assert.False(t, early, "the fence did not wait for the dispatch")

	require.NoError(t, u.Commit())
	if !early {
		fenceErr = <-fenced
	}
	require.NoError(t, fenceErr)

	got := f.reload(evt.TaskID.String(), 0)
	assert.Equal(t, model.StatusDone, got.Status(), "the fence ended the row the dispatch stored")
	assert.Equal(t, model.OutcomeStopped, got.Outcome())
	reps := f.reports(evt.TaskID.String())
	require.Len(t, reps, 2)
	assert.Equal(t, pkgmodel.AttemptStatusQueued, reps[0].Status)
	assert.Equal(t, pkgmodel.AttemptStatusStopped, reps[1].Status)
}

// A dispatch of a run whose fence is being written, uncommitted, waits for
// the fence and then meets it: the attempt is stored stopped and no Job is
// requested.
func TestTaskDispatch_WaitsForAFenceInProgressAndMeetsIt(t *testing.T) {
	f := newFenceFlow(t)
	ctx := context.Background()
	u := postgres.NewPostgresUnitOfWork(f.db, f.logger)
	require.NoError(t, u.Begin(ctx))
	t.Cleanup(func() { _ = u.Rollback() })
	_, err := f.handler(noJobs{}).Handle(ctx, u, f.event(), uuid.Nil)
	require.NoError(t, err)

	evt := f.dispatchOf()
	dispatched := f.inTx(func(du *postgres.PostgresUnitOfWork) error {
		return handlers.NewTaskDispatchHandler(noJobs{}, noJobs{}, f.metrics, f.logger).Handle(ctx, du, evt, uuid.Nil)
	})
	assert.True(t, f.waitForAdvisoryWaiter(), "the dispatch waits for the fence's run lock")
	dispatchErr, early := waitOrNot(dispatched)
	assert.False(t, early, "the dispatch did not wait for the fence")

	require.NoError(t, u.Commit())
	if !early {
		dispatchErr = <-dispatched
	}
	require.NoError(t, dispatchErr)

	got := f.reload(evt.TaskID.String(), 0)
	assert.Equal(t, model.StatusDone, got.Status(), "the dispatch met the fence")
	assert.Equal(t, model.OutcomeStopped, got.Outcome())
	assert.Nil(t, got.DeployedAt())
	reps := f.reports(evt.TaskID.String())
	require.Len(t, reps, 1)
	assert.Equal(t, pkgmodel.AttemptStatusStopped, reps[0].Status)
	assert.Equal(t, 1, f.metrics.stopped)
}

// An attempt the cancel names that has neither a row nor a Job, but whose pod
// a live read shows still running (its Job was deleted with orphan
// propagation), is not reported stopped while the pod can still write:
// nothing is stored or reported for it, and state's next cancel names it
// again. The run is fenced all the same.
func TestRunCancelled_LeavesANamedAttemptWithAnOrphanPodUnanswered(t *testing.T) {
	f := newFenceFlow(t)
	cmd := f.command(0)
	started := f.now.Add(-time.Minute)
	live := jobsOf{cmd.JobName: {Pods: []model.PodObservation{{Name: "pod-1", Phase: model.PodRunning,
		Main: model.ContainerObservation{Name: "dbt-job", StartedAt: &started}}}}}

	names := f.cancel(f.handler(live), f.event(ref(cmd)))

	assert.Empty(t, names)
	assert.True(t, f.fenced())
	_, found, err := f.repo.LoadByTaskAttempt(context.Background(), cmd.TaskID, 0)
	require.NoError(t, err)
	assert.False(t, found, "nothing is stored for the attempt")
	assert.Empty(t, f.reports(cmd.TaskID), "nothing is reported for the attempt")
	assert.Zero(t, f.metrics.stopped)
}

// An attempt is counted stopped once the transaction that reports it stopped
// commits: a fence that rolls back counts nothing, and its redelivery counts
// the attempt once.
func TestRunCancelled_CountsAStopOnlyOnceItCommits(t *testing.T) {
	f := newFenceFlow(t)
	ctx := context.Background()
	f.attempt(model.StatusPending)
	h := f.handler(noJobs{})
	u := postgres.NewPostgresUnitOfWork(f.db, f.logger)
	require.NoError(t, u.Begin(ctx))
	_, err := h.Handle(ctx, u, f.event(), uuid.Nil)
	require.NoError(t, err)
	require.NoError(t, u.Rollback())
	assert.Zero(t, f.metrics.stopped, "a rolled-back stop is not counted")

	f.cancel(h, f.event())

	assert.Equal(t, 1, f.metrics.stopped)
}

// liveJobOfCmd is what the API shows of a live Job of cmd's attempt, its pod
// running.
func (f *fenceFlow) liveJobOf(cmd command.DeployTask) model.JobObservation {
	started := f.now.Add(-time.Minute)
	return model.JobObservation{JobPresent: true, JobTaskID: cmd.TaskID, JobAttempt: strconv.Itoa(cmd.Attempt),
		JobRunID: cmd.RunID, Pods: []model.PodObservation{{Name: "pod-1", Phase: model.PodRunning,
			Main: model.ContainerObservation{Name: "dbt-job", StartedAt: &started}}}}
}

// A reserved row whose Job exists although the row never recorded it (its
// launch created the Job and was lost before its commit) adopts the Job: it
// moves to terminating with the stop recorded, its Job is returned for
// deletion after the commit, and nothing is reported until its pods are
// gone. Only a live read shows the Job here.
func TestRunCancelled_AdoptsTheJobOfAReservedRowThatNeverRecordedIt(t *testing.T) {
	f := newFenceFlow(t)
	d := f.attempt(model.StatusReserved)
	live := jobsOf{d.JobName(): f.liveJobOf(d.Command())}

	names := f.cancel(f.handler(live), f.event())

	assert.Equal(t, []string{d.JobName()}, names, "the Job is deleted once the fence commits")
	got := f.reload(d.Command().TaskID, 0)
	assert.Equal(t, model.StatusTerminating, got.Status())
	assert.NotNil(t, got.DeployedAt(), "the observer follows the adopted Job")
	assert.NotNil(t, got.Facts().StopRequestedAt)
	assert.False(t, got.HasOutcome())
	assert.Empty(t, f.reports(d.Command().TaskID), "stopped is reported only once the pods are gone")
	assert.Zero(t, f.metrics.stopped)
	adopted, err := f.repo.LoadByJobName(context.Background(), d.JobName())
	require.NoError(t, err)
	assert.Equal(t, got.ID(), adopted.ID(), "the observer finds the row by the Job's name")
}

// A pending row whose Job exists (a reserved row the reconciler returned to
// pending after its launch was lost) adopts the Job the same way.
func TestRunCancelled_AdoptsTheJobOfAPendingRowThatNeverRecordedIt(t *testing.T) {
	f := newFenceFlow(t)
	d := f.attempt(model.StatusPending)
	live := &runReads{jobs: jobsOf{d.JobName(): f.liveJobOf(d.Command())}}

	names := f.cancel(f.handler(live), f.event())

	assert.Equal(t, []string{d.JobName()}, names)
	assert.Equal(t, 1, live.reads)
	got := f.reload(d.Command().TaskID, 0)
	assert.Equal(t, model.StatusTerminating, got.Status())
	assert.NotNil(t, got.DeployedAt())
	assert.NotNil(t, got.Facts().StopRequestedAt)
	assert.Empty(t, f.reports(d.Command().TaskID))
	assert.Zero(t, f.metrics.stopped)
}

// A pending row whose attempt has no Job but a pod that has not finished (its
// Job was deleted with orphan propagation) is left as it is, with an error
// logged: no stop is reported while the pod can still write, and state's
// next cancel names the attempt again. The run is fenced all the same.
func TestRunCancelled_LeavesAPendingRowWithAnUnfinishedPodAndNoJob(t *testing.T) {
	f := newFenceFlow(t)
	d := f.attempt(model.StatusPending)
	orphan := f.liveJobOf(d.Command())
	orphan.JobPresent = false
	live := jobsOf{d.JobName(): orphan}
	var logs bytes.Buffer
	h := handlers.NewRunCancelledHandler(live, f.term, f.metrics, slog.New(slog.NewTextHandler(&logs, nil)))

	names := f.cancel(h, f.event())

	assert.Empty(t, names)
	assert.True(t, f.fenced())
	got := f.reload(d.Command().TaskID, 0)
	assert.Equal(t, model.StatusPending, got.Status(), "nothing is written for the row")
	assert.Nil(t, got.Facts().StopRequestedAt)
	assert.Empty(t, f.reports(d.Command().TaskID), "nothing is reported")
	assert.Zero(t, f.metrics.stopped)
	assert.Contains(t, logs.String(), "level=ERROR")
	assert.Contains(t, logs.String(), "a pod of its attempt has not finished")
	assert.Contains(t, logs.String(), d.JobName())
}

// A cancel delivered again over rows that waited for their launch answers
// each once: an adopted row's Job is not returned again and nothing is
// reported for it until its pods are gone; a row left for its unfinished pod
// ends stopped once the pod is gone; a row that ended stopped is not reported
// again.
func TestRunCancelled_AReSentCancelOverRowsWaitingForTheirLaunch(t *testing.T) {
	f := newFenceFlow(t)
	withJob, withPod, withNothing := f.attempt(model.StatusReserved), f.attempt(model.StatusPending), f.attempt(model.StatusPending)
	orphan := f.liveJobOf(withPod.Command())
	orphan.JobPresent = false
	live := jobsOf{withJob.JobName(): f.liveJobOf(withJob.Command()), withPod.JobName(): orphan}

	first := f.cancel(f.handler(live), f.event())
	assert.Equal(t, []string{withJob.JobName()}, first)
	assert.Equal(t, model.StatusPending, f.reload(withPod.Command().TaskID, 0).Status())

	delete(live, withPod.JobName())
	second := f.cancel(f.handler(live), f.event(ref(withJob.Command()), ref(withPod.Command())))

	assert.Empty(t, second, "the adopted row's Job is not returned again")
	assert.Equal(t, model.StatusTerminating, f.reload(withJob.Command().TaskID, 0).Status())
	assert.Empty(t, f.reports(withJob.Command().TaskID))
	got := f.reload(withPod.Command().TaskID, 0)
	assert.Equal(t, model.StatusDone, got.Status(), "the pod is gone, so the row ends stopped")
	assert.Equal(t, model.OutcomeStopped, got.Outcome())
	require.Len(t, f.reports(withPod.Command().TaskID), 1)
	require.Len(t, f.reports(withNothing.Command().TaskID), 1, "the stopped report is written once")
	assert.Equal(t, 2, f.metrics.stopped)
}

// newRunCancelledHandlerOver builds the fence handler over the watch w.
func newRunCancelledHandlerOver(w *watch.Watch, f *fenceFlow, logger *slog.Logger) *handlers.RunCancelledHandler {
	return handlers.NewRunCancelledHandler(w, f.term, f.metrics, logger)
}
