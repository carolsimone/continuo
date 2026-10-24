//go:build integration

package observer

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/k8s"
	"github.com/carolsimone/continuo/execution-controller/adapters/k8s/watch"
	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/execution-controller/service/outcomes"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	"github.com/carolsimone/continuo/execution-controller/service/validation"
	executortest "github.com/carolsimone/continuo/execution-controller/test"
	"github.com/carolsimone/continuo/pkg/domain"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// harness wires an Observer to the executor test database and fake
// Kubernetes ports, its clock fixed at now.
type harness struct {
	db    *sqlx.DB
	repo  repository.DeploymentRepository
	o     *Observer
	cache *fakeCache
	look  *fakeLookup
	term  *fakeTerminator
	logs  *fakeLogs
	up    *fakeUploader
	q     *fakeQueue
	m     *fakeMetrics
	stop  *fakeStopMetrics
	now   time.Time
}

func newHarness(t testing.TB) *harness {
	t.Helper()
	db, cleanup := executortest.SetupPostgres(t)
	t.Cleanup(cleanup)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cache := &fakeCache{}
	h := &harness{db: db, repo: postgres.NewDeploymentsRepository(db, logger), cache: cache,
		look: &fakeLookup{cache: cache, live: map[string]model.JobObservation{}}, term: &fakeTerminator{}, logs: &fakeLogs{},
		up: &fakeUploader{}, q: &fakeQueue{}, m: &fakeMetrics{}, stop: &fakeStopMetrics{},
		// Postgres keeps timestamps to the microsecond; a clock finer than that
		// would make a duration read back from a stored time miss by nanoseconds.
		now: time.Now().UTC().Truncate(time.Microsecond)}
	h.o = New(Config{StartTimeout: 15 * time.Minute, ErrorMessageMaxLen: 4096, LogTailLines: 50}, Deps{
		NewUoW:   func() uow.UnitOfWork { return postgres.NewPostgresUnitOfWork(db, logger) },
		Observed: postgres.NewObservedDeployments(db, logger), Cache: h.cache, Lookup: h.look, Terminator: h.term,
		Logs: h.logs, Uploader: h.up, Queue: h.q, Outcomes: outcomes.NewRecorder(logger), Metrics: h.m,
		Cancelled: postgres.NewCancelledRunsRepository(db), StopMetrics: h.stop, Logger: logger,
	})
	h.o.now = func() time.Time { return h.now }
	return h
}

// started stores a production deployment of attempt attempt of a new task
// whose Job was created at deployedAt.
func (h *harness) started(t testing.TB, attempt int, deployedAt time.Time) *model.Deployment {
	t.Helper()
	runID := uuid.New().String()
	cmd := command.DeployTask{TaskID: uuid.New().String(), ScheduleID: runID, ScheduleName: "daily",
		ServiceName: "service-1", SchemaName: "e2e_schema", TableName: "ftable_e",
		NodeType: "dbt-model", ImageTag: "v42", Operation: "run", Attempt: attempt}
	name, err := domain.ComputeJobName(cmd.ServiceName, cmd.SchemaName, cmd.TableName, runID, attempt)
	require.NoError(t, err)
	cmd.JobName = name
	dep := model.NewDeployment(cmd, nil, deployedAt)
	require.NoError(t, h.repo.Add(context.Background(), dep))
	require.NoError(t, dep.Reserve())
	require.NoError(t, dep.MarkStarted(deployedAt))
	require.NoError(t, h.repo.Save(context.Background(), dep))
	return dep
}

func (h *harness) reload(t *testing.T, jobName string) *model.Deployment {
	t.Helper()
	d, err := h.repo.LoadByJobName(context.Background(), jobName)
	require.NoError(t, err)
	return d
}

// outbox returns the payloads of the outbox rows of eventType, oldest first.
func (h *harness) outbox(t *testing.T, eventType string) []map[string]any {
	t.Helper()
	var raws [][]byte
	require.NoError(t, h.db.Select(&raws,
		`SELECT payload FROM execution_outbox WHERE event_type = $1 ORDER BY created_at, id`, eventType))
	out := make([]map[string]any, 0, len(raws))
	for _, raw := range raws {
		var m map[string]any
		require.NoError(t, json.Unmarshal(raw, &m))
		out = append(out, m)
	}
	return out
}

// reports returns the task.status.updated:v2 reports written so far, oldest
// first.
func (h *harness) reports(t *testing.T) []map[string]any {
	t.Helper()
	return h.outbox(t, event.EventTypeTaskStatusReport)
}

// attempts counts the deployments of taskID: execution-controller never
// queues an attempt of its own.
func (h *harness) attempts(t *testing.T, taskID string) int {
	t.Helper()
	var n int
	require.NoError(t, h.db.Get(&n, `SELECT count(*) FROM deployments WHERE task_id = $1`, taskID))
	return n
}

func ts(t time.Time) *time.Time { return &t }

func runningPod(started time.Time) model.PodObservation {
	return model.PodObservation{Name: "pod-1", Phase: model.PodRunning,
		Main: model.ContainerObservation{Name: "dbt-job", StartedAt: &started}}
}

func endedPod(phase model.PodPhase, started, finished time.Time, exit int32, reason string) model.PodObservation {
	return model.PodObservation{Name: "pod-1", Phase: phase, Main: model.ContainerObservation{Name: "dbt-job",
		StartedAt: &started, Terminated: true, ExitCode: exit, TerminatedReason: reason, FinishedAt: &finished}}
}

func TestProcess_TheMainContainerStartingIsReportedRunningOnce(t *testing.T) {
	h := newHarness(t)
	dep := h.started(t, 0, h.now.Add(-5*time.Second))
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, Pods: []model.PodObservation{runningPod(h.now.Add(-time.Second))}})

	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))
	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))

	assert.Equal(t, model.StatusRunning, h.reload(t, dep.JobName()).Status())
	running := h.reports(t)
	require.Len(t, running, 1, "running is reported once per attempt")
	assert.Equal(t, "running", running[0]["status"])
	assert.Equal(t, dep.Command().TaskID, running[0]["task_id"])
	assert.EqualValues(t, 0, running[0]["attempt"])
	assert.NotEmpty(t, running[0]["started_at"], "a report carries every fact seen so far")
}

func TestProcess_ASucceededAttemptReportsItsResultAndLog(t *testing.T) {
	h := newHarness(t)
	dep := h.started(t, 0, h.now.Add(-time.Minute))
	start, finish := h.now.Add(-50*time.Second), h.now.Add(-20*time.Second)
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobSucceeded: true,
		Pods: []model.PodObservation{endedPod(model.PodSucceeded, start, finish, 0, "Completed")}})
	h.logs.full = "1 of 1 OK created sql table model e2e_schema.ftable_e"

	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))

	got := h.reload(t, dep.JobName())
	assert.Equal(t, model.StatusDone, got.Status())
	assert.Equal(t, "ok", got.Outcome())
	logKey := "logs/task-executions/service-1/e2e_schema/ftable_e/" + dep.ID().String() + ".log"
	assert.Equal(t, logKey, got.Facts().LogKey)
	assert.Equal(t, h.logs.full, h.up.uploaded[logKey])

	status := h.reports(t)
	require.Len(t, status, 1)
	assert.Equal(t, "succeeded", status[0]["status"])
	assert.Equal(t, dep.JobName(), status[0]["job_name"])
	assert.Equal(t, logKey, status[0]["log_s3_key"])
	assert.InDelta(t, 30.0, status[0]["execution_seconds"], 0.001)
	assert.Nil(t, status[0]["error_message"], "a success carries no error message")
	assert.Nil(t, status[0]["failure_class"])
	require.NotNil(t, got.LastReport(), "the row keeps the report it sent")
	assert.Equal(t, pkgmodel.AttemptStatusSucceeded, got.LastReport().Status)
}

func TestProcess_AFailedAttemptIsReportedWithItsClassAndQueuesNothing(t *testing.T) {
	h := newHarness(t)
	dep := h.started(t, 0, h.now.Add(-time.Minute))
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobFailed: true, JobFailureReason: "BackoffLimitExceeded",
		Pods: []model.PodObservation{endedPod(model.PodFailed, h.now.Add(-40*time.Second), h.now.Add(-10*time.Second), 137, "OOMKilled")}})
	h.logs.full = "Killed"

	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))

	got := h.reload(t, dep.JobName())
	assert.Equal(t, "failed", got.Outcome())
	assert.Equal(t, pkgmodel.FailureReasonOOMKilled, got.Facts().Reason)
	assert.Equal(t, pkgmodel.FailureClassResource, got.Facts().Class())
	status := h.reports(t)
	require.Len(t, status, 1)
	assert.Equal(t, "failed", status[0]["status"])
	assert.EqualValues(t, 0, status[0]["attempt"], "the failure is reported at the attempt that ran")
	assert.Equal(t, "resource", status[0]["failure_class"])
	assert.Equal(t, "oom_killed", status[0]["failure_reason"])
	assert.EqualValues(t, 137, status[0]["exit_code"])
	assert.Equal(t, "Killed", status[0]["error_message"])
	assert.Equal(t, 1, h.attempts(t, dep.Command().TaskID), "state decides whether the task runs again")
}

func TestProcess_ALaterAttemptIsReportedAtItsOwnNumber(t *testing.T) {
	h := newHarness(t)
	dep := h.started(t, 2, h.now.Add(-time.Minute))
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobFailed: true,
		Pods: []model.PodObservation{endedPod(model.PodFailed, h.now.Add(-40*time.Second), h.now.Add(-10*time.Second), 1, "Error")}})

	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))

	assert.Equal(t, pkgmodel.FailureReasonExitCode, h.reload(t, dep.JobName()).Facts().Reason)
	status := h.reports(t)
	require.Len(t, status, 1)
	assert.EqualValues(t, 2, status[0]["attempt"])
	assert.Equal(t, "task", status[0]["failure_class"])
	assert.Equal(t, dep.JobName(), status[0]["job_name"])
}

// A dispatch refused while it carries the name of a running attempt's Job
// leaves that attempt observed: it is settled from its Job and reported.
func TestProcess_ARefusedDispatchOfTheJobsNameLeavesItsOwnerObserved(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	dep := h.started(t, 0, h.now.Add(-time.Minute))
	cmd := dep.Command()
	cmd.TaskID = uuid.NewString()
	cmd.NodeType = "dbt-macro"
	refused := model.NewRefusedDeployment(cmd, nil, pkgmodel.FailureReasonInvalidNodeType, "node type dbt-macro is not declared", h.now)
	refused.RecordReport(refused.ReportOf(pkgmodel.AttemptStatusFailed, "node type dbt-macro is not declared"))
	added, err := h.repo.AddAttempt(ctx, refused)
	require.NoError(t, err)
	require.True(t, added)
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobSucceeded: true,
		Pods: []model.PodObservation{endedPod(model.PodSucceeded, h.now.Add(-50*time.Second), h.now.Add(-20*time.Second), 0, "Completed")}})

	require.NoError(t, h.o.Process(ctx, dep.JobName()))

	var status string
	require.NoError(t, h.db.Get(&status, `SELECT status FROM deployments WHERE id = $1`, dep.ID()))
	assert.Equal(t, "done", status, "the attempt is settled and its slot released")
	reports := h.reports(t)
	require.Len(t, reports, 1)
	assert.Equal(t, "succeeded", reports[0]["status"])
	assert.Equal(t, dep.Command().TaskID, reports[0]["task_id"])
}

func TestProcess_AStartErrorEndsTheAttemptAndReportsItOnceItsPodsAreGone(t *testing.T) {
	h := newHarness(t)
	dep := h.started(t, 0, h.now.Add(-30*time.Second))
	pulling := model.PodObservation{Name: "pod-1", Phase: model.PodPending, Main: model.ContainerObservation{
		Name: "dbt-job", WaitingReason: "ErrImagePull", WaitingMessage: "pull access denied for service-1"}}
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, Pods: []model.PodObservation{pulling}})

	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))

	got := h.reload(t, dep.JobName())
	assert.Equal(t, model.StatusTerminating, got.Status(), "the slot is held while the pod goes")
	assert.Equal(t, pkgmodel.FailureReasonImagePull, got.Facts().Reason)
	assert.Equal(t, []string{dep.JobName()}, h.term.deleted)
	assert.Empty(t, h.reports(t), "nothing is reported while the pod exists")

	deletingPod := pulling
	deletingPod.DeletingSince = ts(h.now)
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobDeleting: true, Pods: []model.PodObservation{deletingPod}})
	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))
	assert.Empty(t, h.reports(t), "a pod being deleted has not ended the attempt")
	assert.Equal(t, 0, h.logs.reads, "a pod that never ran has no log to read")

	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobDeleting: true})
	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))

	assert.Equal(t, model.StatusDone, h.reload(t, dep.JobName()).Status())
	status := h.reports(t)
	require.Len(t, status, 1)
	assert.Equal(t, "failed", status[0]["status"])
	assert.Equal(t, "start", status[0]["failure_class"])
	assert.Equal(t, "image_pull", status[0]["failure_reason"])
	assert.Equal(t, "ErrImagePull: pull access denied for service-1", status[0]["error_message"])
	assert.Equal(t, 1, h.attempts(t, dep.Command().TaskID))
	assert.Len(t, h.term.deleted, 1, "a Job already deleting is not deleted again")
}

func TestProcess_TheStartTimeoutEndsAnAttemptThatNeverStarted(t *testing.T) {
	h := newHarness(t)
	unschedulable := model.JobObservation{JobPresent: true, Pods: []model.PodObservation{{Name: "pod-1", Phase: model.PodPending}}}

	young := h.started(t, 0, h.now.Add(-10*time.Minute))
	h.cache.set(young.JobName(), unschedulable)
	require.NoError(t, h.o.Process(context.Background(), young.JobName()))
	assert.Equal(t, model.StatusStarting, h.reload(t, young.JobName()).Status())
	assert.Equal(t, 5*time.Minute, h.q.after[young.JobName()], "checked again when the timeout expires")

	old := h.started(t, 0, h.now.Add(-16*time.Minute))
	h.cache.set(old.JobName(), unschedulable)
	require.NoError(t, h.o.Process(context.Background(), old.JobName()))
	got := h.reload(t, old.JobName())
	assert.Equal(t, model.StatusTerminating, got.Status())
	assert.Equal(t, pkgmodel.FailureReasonStartTimeout, got.Facts().Reason)
	assert.Equal(t, "no container of the pod had started 15m0s after the Job was created", got.Facts().Message)
	assert.Contains(t, h.term.deleted, old.JobName())

	compiling := h.started(t, 0, h.now.Add(-20*time.Minute))
	h.cache.set(compiling.JobName(), model.JobObservation{JobPresent: true, Pods: []model.PodObservation{{
		Name: "pod-c", Phase: model.PodPending,
		Main:  model.ContainerObservation{Name: "upload", WaitingReason: "PodInitializing"},
		Inits: []model.ContainerObservation{{Name: "compile", StartedAt: ts(h.now.Add(-19 * time.Minute))}}}}})
	require.NoError(t, h.o.Process(context.Background(), compiling.JobName()))
	assert.Equal(t, model.StatusStarting, h.reload(t, compiling.JobName()).Status(),
		"an init container running past the start timeout is work, not a pod that cannot start")
	assert.NotContains(t, h.term.deleted, compiling.JobName())
}

func TestProcess_AJobAbsentFromTheCacheIsMissingOnlyAfterTheGraceAndALiveRead(t *testing.T) {
	h := newHarness(t)
	dep := h.started(t, 0, h.now.Add(-time.Minute))

	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))
	assert.Equal(t, model.StatusStarting, h.reload(t, dep.JobName()).Status(), "a Job just created may not be cached yet")
	assert.Greater(t, h.q.after[dep.JobName()], time.Duration(0))

	_, err := h.db.Exec(`UPDATE deployments SET state_changed_at = NOW() - interval '3 minutes' WHERE id = $1`, dep.ID())
	require.NoError(t, err)
	h.look.live[dep.JobName()] = model.JobObservation{JobPresent: true}
	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))
	assert.Equal(t, model.StatusStarting, h.reload(t, dep.JobName()).Status(), "the API still has the Job: the cache trails it")

	delete(h.look.live, dep.JobName())
	h.cache.set(dep.JobName(), model.JobObservation{Pods: []model.PodObservation{runningPod(h.now.Add(-time.Minute))}})
	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))
	left := h.reports(t)
	require.Len(t, left, 1, "a pod of the missing Job is left: only its start is reported")
	assert.Equal(t, "running", left[0]["status"])

	assert.Equal(t, model.StatusRunning, h.reload(t, dep.JobName()).Status(), "its pod's facts are recorded")
	_, err = h.db.Exec(`UPDATE deployments SET state_changed_at = NOW() - interval '3 minutes' WHERE id = $1`, dep.ID())
	require.NoError(t, err)
	h.cache.set(dep.JobName(), model.JobObservation{})
	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))
	got := h.reload(t, dep.JobName())
	assert.Equal(t, "failed", got.Outcome())
	assert.Equal(t, pkgmodel.FailureReasonJobMissing, got.Facts().Reason, "the Job is gone and no pod of it is left")
	status := h.reports(t)
	require.Len(t, status, 2)
	assert.Equal(t, "running", status[0]["status"])
	assert.Equal(t, "failed", status[1]["status"])
	assert.Equal(t, "infra", status[1]["failure_class"])
	assert.Equal(t, "job_missing", status[1]["failure_reason"])
}

func TestProcess_APodBeingDeletedHasItsLogReadAtOnceAndRaisesTheAlertAfter15Minutes(t *testing.T) {
	h := newHarness(t)
	dep := h.started(t, 0, h.now.Add(-time.Hour))
	pod := runningPod(h.now.Add(-50 * time.Minute))
	pod.DeletingSince = ts(h.now)
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, Pods: []model.PodObservation{pod}})
	h.logs.full = "partial output"

	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))
	got := h.reload(t, dep.JobName())
	assert.Equal(t, model.StatusRunning, got.Status(), "a deletion timestamp alone ends nothing")
	assert.NotEmpty(t, got.Facts().LogKey, "the log is uploaded while the pod can still be read")
	assert.Equal(t, 15*time.Minute, h.q.after[dep.JobName()])
	assert.False(t, h.m.waiting[dep.JobName()])

	h.now = h.now.Add(16 * time.Minute)
	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))
	assert.True(t, h.m.waiting[dep.JobName()], "the alert is raised after 15 minutes")
	assert.Equal(t, 1, h.logs.reads, "the log is read once")

	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobFailed: true})
	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))
	assert.False(t, h.m.waiting[dep.JobName()], "the alert clears once the attempt ends")
	assert.Equal(t, pkgmodel.FailureReasonNodeLost, h.reload(t, dep.JobName()).Facts().Reason)
}

// An attempt of a cancelled run is reported like any other: running once its
// main container starts, and, once it was asked to stop and its pods are gone
// without it succeeding, stopped, with the log it left and no failure. The
// row ends done with the outcome stopped.
func TestSettle_ReportsAStoppedAttemptOfACancelledRun(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	dep := h.started(t, 0, h.now.Add(-time.Minute))
	_, err := h.db.Exec(`INSERT INTO cancelled_runs (run_id) VALUES ($1)`, dep.Command().ScheduleID)
	require.NoError(t, err)
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, Pods: []model.PodObservation{runningPod(h.now.Add(-30 * time.Second))}})
	require.NoError(t, h.o.Process(ctx, dep.JobName()))
	stopping := h.reload(t, dep.JobName())
	effect, err := stopping.Stop(h.now)
	require.NoError(t, err)
	require.Equal(t, model.StopTerminate, effect)
	require.NoError(t, h.repo.Save(ctx, stopping))
	h.logs.full = "Running with dbt\nreceived SIGTERM"
	deleting := runningPod(h.now.Add(-30 * time.Second))
	deleting.DeletingSince = ts(h.now)
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobDeleting: true, Pods: []model.PodObservation{deleting}})
	require.NoError(t, h.o.Process(ctx, dep.JobName()))
	require.Len(t, h.reports(t), 1, "nothing more is reported while the pod is still being deleted")

	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobDeleting: true})
	require.NoError(t, h.o.Process(ctx, dep.JobName()))

	got := h.reload(t, dep.JobName())
	assert.Equal(t, model.StatusDone, got.Status())
	assert.Equal(t, model.OutcomeStopped, got.Outcome())
	reports := h.reports(t)
	require.Len(t, reports, 2)
	assert.Equal(t, "running", reports[0]["status"])
	assert.Equal(t, "stopped", reports[1]["status"])
	assert.NotEmpty(t, reports[1]["log_s3_key"], "the log the attempt left is kept")
	assert.NotContains(t, reports[1], "failure_reason")
	assert.NotContains(t, reports[1], "failure_class")
	assert.Empty(t, got.Facts().Reason, "a stop is no failure")
	assert.Equal(t, 1, h.stop.stopped, "the stopped attempt is counted")
}

// An attempt whose container failed before its run was cancelled, but which
// the observer had not settled yet when the stop was recorded, is reported
// failed with its own class and reason, not stopped: state then ends the
// stopping task failed. The row ends done with the outcome failed.
func TestSettle_AFailureReachedBeforeTheStopIsReportedAsItself(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	dep := h.started(t, 0, h.now.Add(-time.Minute))
	_, err := dep.MarkRunning()
	require.NoError(t, err)
	failed := endedPod(model.PodFailed, h.now.Add(-50*time.Second), h.now.Add(-30*time.Second).Truncate(time.Second), 1, "Error")
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, Pods: []model.PodObservation{failed}})
	effect, err := dep.Stop(h.now)
	require.NoError(t, err)
	require.Equal(t, model.StopTerminate, effect)
	require.NoError(t, h.repo.Save(ctx, dep))
	h.logs.full = "Database Error in model ftable_e\n  relation \"missing\" does not exist"

	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobFailed: true, Pods: []model.PodObservation{failed}})
	require.NoError(t, h.o.Process(ctx, dep.JobName()))

	got := h.reload(t, dep.JobName())
	assert.Equal(t, model.StatusDone, got.Status())
	assert.Equal(t, "failed", got.Outcome())
	assert.Equal(t, pkgmodel.FailureReasonExitCode, got.Facts().Reason)
	assert.NotNil(t, got.Facts().StopRequestedAt, "the stop stays recorded beside the failure")
	reports := h.reports(t)
	require.Len(t, reports, 1)
	assert.Equal(t, "failed", reports[0]["status"])
	assert.Equal(t, "task", reports[0]["failure_class"])
	assert.Equal(t, "exit_code", reports[0]["failure_reason"])
	assert.EqualValues(t, 1, reports[0]["exit_code"])
	assert.Zero(t, h.stop.stopped, "a failure is not counted as a stop")
}

func TestProcess_AValidationNodeRecordsItsOutcome(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	cmd := command.ValidationDeployTask{ReleaseID: "rel-" + uuid.NewString()[:8], NodeID: "e2e_schema.ftable_e",
		ServiceName: "service-1", SchemaName: "e2e_schema", TableName: "ftable_e", NodeType: "dbt-model",
		ImageTag: "v42", JobName: "validate-ftable-e", CandidateSchema: "_cand_x"}
	dep := model.NewValidationDeployment(cmd, nil, h.now, false)
	require.NoError(t, h.repo.Add(ctx, dep))
	require.NoError(t, dep.Reserve())
	require.NoError(t, dep.MarkStarted(h.now.Add(-time.Minute)))
	require.NoError(t, h.repo.Save(ctx, dep))
	h.cache.set("validate-ftable-e", model.JobObservation{JobPresent: true, JobSucceeded: true,
		Pods: []model.PodObservation{endedPod(model.PodSucceeded, h.now.Add(-50*time.Second), h.now.Add(-5*time.Second), 0, "Completed")}})
	h.logs.full = "validated"

	require.NoError(t, h.o.Process(ctx, "validate-ftable-e"))

	got := h.reload(t, "validate-ftable-e")
	assert.Equal(t, "ok", got.Outcome())
	assert.Equal(t, "logs/task-executions/service-1/e2e_schema/ftable_e/"+dep.ID().String()+".log", got.DBTLogURI())
	assert.NotNil(t, got.Facts().FinishedAt, "the facts are kept with the outcome")
	assert.Len(t, h.outbox(t, validation.EventTypeValidationNodeResult), 1)
	assert.Len(t, h.outbox(t, validation.EventTypeValidationCompleted), 1, "its only node settled, so the leg completes")
	assert.Empty(t, h.reports(t), "a release leg reports no task status")
}

func TestProcess_AReleaseLegWhosePodNeverStartsIsRecordedFailed(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	cmd := command.ValidationDeployTask{ReleaseID: "rel-" + uuid.NewString()[:8], NodeID: "e2e_schema.ftable_e",
		ServiceName: "service-1", SchemaName: "e2e_schema", TableName: "ftable_e", NodeType: "dbt-model",
		ImageTag: "v42", JobName: "validate-ftable-e-stuck", CandidateSchema: "_cand_x"}
	dep := model.NewValidationDeployment(cmd, nil, h.now.Add(-16*time.Minute), false)
	require.NoError(t, h.repo.Add(ctx, dep))
	require.NoError(t, dep.Reserve())
	require.NoError(t, dep.MarkStarted(h.now.Add(-16*time.Minute)))
	require.NoError(t, h.repo.Save(ctx, dep))
	h.cache.set(cmd.JobName, model.JobObservation{JobPresent: true,
		Pods: []model.PodObservation{{Name: "pod-v", Phase: model.PodPending}}})

	require.NoError(t, h.o.Process(ctx, cmd.JobName))
	assert.Equal(t, model.StatusTerminating, h.reload(t, cmd.JobName).Status())
	assert.Contains(t, h.term.deleted, cmd.JobName)
	assert.Empty(t, h.outbox(t, validation.EventTypeValidationNodeResult), "nothing is recorded while its pod remains")

	h.cache.set(cmd.JobName, model.JobObservation{JobDeleting: true, JobPresent: true})
	require.NoError(t, h.o.Process(ctx, cmd.JobName))

	got := h.reload(t, cmd.JobName)
	assert.Equal(t, "failed", got.Outcome())
	assert.Equal(t, pkgmodel.FailureReasonStartTimeout, got.Facts().Reason)
	assert.Len(t, h.outbox(t, validation.EventTypeValidationNodeResult), 1)
	assert.Len(t, h.outbox(t, validation.EventTypeValidationCompleted), 1,
		"a leg whose pod never started completes, so its release is decided")
}

// A terminating attempt is settled only once a live read shows it has ended:
// the pod watch can trail the Job watch, so a Job being deleted with no pod in
// the cache may still have a running pod.
func TestProcess_ATerminatingAttemptIsNotSettledWhileTheAPIStillShowsItsPod(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	dep := h.started(t, 0, h.now.Add(-time.Minute))
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, Pods: []model.PodObservation{{
		Name: "pod-1", Phase: model.PodPending,
		Main: model.ContainerObservation{Name: "dbt-job", WaitingReason: "ErrImagePull", WaitingMessage: "not found"}}}})
	require.NoError(t, h.o.Process(ctx, dep.JobName()))
	require.Equal(t, model.StatusTerminating, h.reload(t, dep.JobName()).Status())

	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobDeleting: true})
	h.look.live[dep.JobName()] = model.JobObservation{JobPresent: true, JobDeleting: true,
		Pods: []model.PodObservation{runningPod(h.now.Add(-30 * time.Second))}}
	delete(h.q.after, dep.JobName())
	require.NoError(t, h.o.Process(ctx, dep.JobName()))
	assert.Equal(t, model.StatusTerminating, h.reload(t, dep.JobName()).Status(), "its pod still runs")
	assert.Empty(t, h.reports(t), "nothing is reported")
	assert.Greater(t, h.q.after[dep.JobName()], time.Duration(0), "it is checked again")

	h.look.live[dep.JobName()] = model.JobObservation{}
	require.NoError(t, h.o.Process(ctx, dep.JobName()))
	got := h.reload(t, dep.JobName())
	assert.Equal(t, "failed", got.Outcome())
	assert.Equal(t, pkgmodel.FailureReasonImagePull, got.Facts().Reason)
}

// A failed Job whose pod is still stopping (Kubernetes marks a Job failed
// while its pods terminate) is settled once the live read shows the pod
// finished, from the facts that read shows when the cache has not caught up.
func TestProcess_AFailedJobIsSettledOnlyOnceTheAPIShowsItsPodFinished(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	dep := h.started(t, 0, h.now.Add(-time.Minute))
	stopping := runningPod(h.now.Add(-50 * time.Second))
	deleting := h.now.Add(-time.Second)
	stopping.DeletingSince = &deleting
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobFailed: true})
	h.look.live[dep.JobName()] = model.JobObservation{JobPresent: true, JobFailed: true, Pods: []model.PodObservation{stopping}}
	require.NoError(t, h.o.Process(ctx, dep.JobName()))
	assert.False(t, h.reload(t, dep.JobName()).HasOutcome(), "its pod is still stopping")
	status := h.reports(t)
	require.Len(t, status, 1)
	assert.Equal(t, "running", status[0]["status"], "the live read shows its main container started; no result yet")

	h.look.live[dep.JobName()] = model.JobObservation{JobPresent: true, JobFailed: true,
		Pods: []model.PodObservation{endedPod(model.PodFailed, h.now.Add(-50*time.Second), h.now.Add(-time.Second), 1, "Error")}}
	require.NoError(t, h.o.Process(ctx, dep.JobName()))
	got := h.reload(t, dep.JobName())
	assert.Equal(t, "failed", got.Outcome())
	assert.Equal(t, pkgmodel.FailureReasonExitCode, got.Facts().Reason, "the live read's exit code decides")
}

// A Job deleted without its pods (cascade=orphan) whose pods have all finished
// is settled from their facts once the grace and a live read confirm the Job
// is gone: it is not reported missing.
func TestProcess_AMissingJobWhosePodsAllFinishedIsSettledFromThem(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	succeeded := h.started(t, 0, h.now.Add(-time.Hour))
	failed := h.started(t, 0, h.now.Add(-time.Hour))
	for _, d := range []*model.Deployment{succeeded, failed} {
		_, err := h.db.Exec(`UPDATE deployments SET state_changed_at = NOW() - interval '3 minutes' WHERE id = $1`, d.ID())
		require.NoError(t, err)
	}
	h.cache.set(succeeded.JobName(), model.JobObservation{Pods: []model.PodObservation{
		endedPod(model.PodSucceeded, h.now.Add(-50*time.Minute), h.now.Add(-49*time.Minute), 0, "Completed")}})
	h.cache.set(failed.JobName(), model.JobObservation{Pods: []model.PodObservation{
		endedPod(model.PodFailed, h.now.Add(-50*time.Minute), h.now.Add(-49*time.Minute), 1, "Error")}})

	for _, d := range []*model.Deployment{succeeded, failed} {
		require.NoError(t, h.o.Process(ctx, d.JobName()))
	}

	assert.Equal(t, "ok", h.reload(t, succeeded.JobName()).Outcome())
	got := h.reload(t, failed.JobName())
	assert.Equal(t, "failed", got.Outcome())
	assert.Equal(t, pkgmodel.FailureReasonExitCode, got.Facts().Reason, "settled from its pod, not reported missing")
	assert.Len(t, h.reports(t), 2)
}

func TestProcess_AMissingJobWhoseFinishedPodShowsNoExitCodeIsJobMissing(t *testing.T) {
	h := newHarness(t)
	dep := h.started(t, 0, h.now.Add(-time.Hour))
	_, err := h.db.Exec(`UPDATE deployments SET state_changed_at = NOW() - interval '3 minutes' WHERE id = $1`, dep.ID())
	require.NoError(t, err)
	rejected := model.PodObservation{Name: "pod-1", Phase: model.PodFailed, Reason: "OutOfcpu",
		Main: model.ContainerObservation{Name: "dbt-job"}}
	h.cache.set(dep.JobName(), model.JobObservation{Pods: []model.PodObservation{rejected}})

	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))

	got := h.reload(t, dep.JobName())
	assert.Equal(t, "failed", got.Outcome(), "its only pod has finished, so the attempt is settled rather than waited for")
	assert.Equal(t, pkgmodel.FailureReasonJobMissing, got.Facts().Reason)
	assert.Equal(t, "the Job is gone from Kubernetes and nothing seen of the attempt shows how it ended", got.Facts().Message)
	status := h.reports(t)
	require.Len(t, status, 1)
	assert.Equal(t, "job_missing", status[0]["failure_reason"])
}

// A missing Job whose finished pod only the cache still shows (the API has
// removed it) is settled from that pod's facts, not reported missing.
func TestProcess_AMissingJobIsSettledFromAPodOnlyTheCacheStillShows(t *testing.T) {
	h := newHarness(t)
	dep := h.started(t, 0, h.now.Add(-time.Hour))
	_, err := h.db.Exec(`UPDATE deployments SET state_changed_at = NOW() - interval '3 minutes' WHERE id = $1`, dep.ID())
	require.NoError(t, err)
	h.cache.set(dep.JobName(), model.JobObservation{Pods: []model.PodObservation{
		endedPod(model.PodSucceeded, h.now.Add(-50*time.Minute), h.now.Add(-49*time.Minute), 0, "Completed")}})
	h.look.live[dep.JobName()] = model.JobObservation{}

	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))

	got := h.reload(t, dep.JobName())
	assert.Equal(t, "ok", got.Outcome(), "the cache shows the pod exited 0")
	assert.Empty(t, got.Facts().Reason)
	status := h.reports(t)
	require.Len(t, status, 1)
	assert.Equal(t, "succeeded", status[0]["status"])
}

// Whether a missing Job's attempt is job_missing is decided on the locked row,
// so a fact another replica recorded after this one read the row counts.
func TestProcess_AMissingJobIsSettledFromFactsRecordedAfterTheRowWasRead(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	dep := h.started(t, 0, h.now.Add(-time.Hour))
	_, err := h.db.Exec(`UPDATE deployments SET state_changed_at = NOW() - interval '3 minutes' WHERE id = $1`, dep.ID())
	require.NoError(t, err)
	h.look.live[dep.JobName()] = model.JobObservation{}
	h.look.onRead = func() {
		other := h.reload(t, dep.JobName())
		exit := int32(1)
		other.Observe(model.AttemptFacts{ExitCode: &exit, FailedContainer: "dbt-job", Message: "Error"})
		require.NoError(t, h.repo.Save(ctx, other))
	}

	require.NoError(t, h.o.Process(ctx, dep.JobName()))

	got := h.reload(t, dep.JobName())
	assert.Equal(t, "failed", got.Outcome())
	assert.Equal(t, pkgmodel.FailureReasonExitCode, got.Facts().Reason, "the recorded exit code decides")
	assert.Equal(t, "Error", got.Facts().Message)
}

func TestProcess_AnEndedAttemptIsSettledOnce(t *testing.T) {
	h := newHarness(t)
	dep := h.started(t, 0, h.now.Add(-time.Minute))
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobSucceeded: true,
		Pods: []model.PodObservation{endedPod(model.PodSucceeded, h.now.Add(-time.Minute), h.now, 0, "Completed")}})

	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))
	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))

	assert.Len(t, h.reports(t), 1)
}

func TestResync_QueuesTheJobOfEveryStartedDeployment(t *testing.T) {
	h := newHarness(t)
	a := h.started(t, 0, h.now)
	b := h.started(t, 1, h.now)
	require.NoError(t, h.o.Resync(context.Background()))
	assert.ElementsMatch(t, []string{a.JobName(), b.JobName()}, h.q.added)
}

// commitProbe is a unit of work that runs check, on another connection,
// just before it commits.
type commitProbe struct {
	uow.UnitOfWork
	check func()
}

func (c *commitProbe) Commit() error {
	c.check()
	return c.UnitOfWork.Commit()
}

// A production failure's report and the row's outcome commit in one
// transaction.
func TestSettle_AFailureAndItsReportCommitTogether(t *testing.T) {
	h := newHarness(t)
	dep := h.started(t, 0, h.now.Add(-time.Minute))
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobFailed: true,
		Pods: []model.PodObservation{endedPod(model.PodFailed, h.now.Add(-40*time.Second), h.now.Add(-10*time.Second), 1, "Error")}})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	probed := false
	h.o.deps.NewUoW = func() uow.UnitOfWork {
		return &commitProbe{UnitOfWork: postgres.NewPostgresUnitOfWork(h.db, logger), check: func() {
			probed = true
			assert.Empty(t, h.reports(t), "nothing is visible before the commit")
			var status string
			require.NoError(t, h.db.Get(&status, `SELECT status FROM deployments WHERE id = $1`, dep.ID()))
			assert.Equal(t, string(model.StatusStarting), status, "the row's outcome is not visible either")
		}}
	}

	require.NoError(t, h.o.Process(context.Background(), dep.JobName()))

	require.True(t, probed)
	assert.Len(t, h.reports(t), 1)
	got := h.reload(t, dep.JobName())
	assert.Equal(t, "failed", got.Outcome())
	require.NotNil(t, got.LastReport())
	assert.Equal(t, pkgmodel.AttemptStatusFailed, got.LastReport().Status)
}

// failingOutbox fails its nth Create.
type failingOutbox struct {
	pkgoutbox.Repository
	n, calls int
}

func (f *failingOutbox) Create(ctx context.Context, e *pkgoutbox.Entry) error {
	f.calls++
	if f.calls == f.n {
		return errBoom
	}
	return f.Repository.Create(ctx, e)
}

// failOnFirstCreate is a unit of work whose outbox fails its first Create.
type failOnFirstCreate struct {
	uow.UnitOfWork
	outbox *failingOutbox
}

func (u *failOnFirstCreate) OutboxRepo() pkgoutbox.Repository {
	if u.outbox == nil {
		u.outbox = &failingOutbox{Repository: u.UnitOfWork.OutboxRepo(), n: 1}
	}
	return u.outbox
}

// A failure partway through settling leaves nothing behind: no report, and
// the row still awaits its result.
func TestSettle_AFailedWriteRollsEverythingBack(t *testing.T) {
	h := newHarness(t)
	dep := h.started(t, 0, h.now.Add(-time.Minute))
	h.cache.set(dep.JobName(), model.JobObservation{JobPresent: true, JobSucceeded: true,
		Pods: []model.PodObservation{endedPod(model.PodSucceeded, h.now.Add(-40*time.Second), h.now.Add(-10*time.Second), 0, "Completed")}})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h.o.deps.NewUoW = func() uow.UnitOfWork {
		return &failOnFirstCreate{UnitOfWork: postgres.NewPostgresUnitOfWork(h.db, logger)}
	}

	require.ErrorIs(t, h.o.Process(context.Background(), dep.JobName()), errBoom)

	assert.Empty(t, h.reports(t))
	assert.True(t, h.reload(t, dep.JobName()).AwaitingResult(), "the attempt is settled again on the next try")
}

// liveJobOf is what the cache shows of a live Job of attempt 0 of a task of
// runID, with its pod running.
func liveJobOf(runID string, now time.Time) model.JobObservation {
	return model.JobObservation{JobPresent: true, JobTaskID: uuid.NewString(), JobAttempt: "0", JobRunID: runID,
		Pods: []model.PodObservation{runningPod(now.Add(-time.Minute))}}
}

// unstartedRow stores a production deployment of jobName for runID that never
// created its Job: reserved, or failed for good when failed is set.
func (h *harness) unstartedRow(t *testing.T, runID, jobName string, failed bool) *model.Deployment {
	t.Helper()
	ctx := context.Background()
	cmd := command.DeployTask{TaskID: uuid.NewString(), ScheduleID: runID, ScheduleName: "daily",
		ServiceName: "service-1", SchemaName: "e2e_schema", TableName: "ftable_e", JobName: jobName,
		NodeType: "dbt-model", ImageTag: "v42", Operation: "run"}
	dep := model.NewDeployment(cmd, nil, h.now.Add(-time.Minute))
	require.NoError(t, h.repo.Add(ctx, dep))
	require.NoError(t, dep.Reserve())
	if failed {
		_, err := dep.RegisterFailure(h.now, true, "the API refused the Job", model.BackoffPolicy{Base: time.Second, Cap: time.Minute})
		require.NoError(t, err)
	}
	require.NoError(t, h.repo.Save(ctx, dep))
	return dep
}

// A live Job of a fenced run that no started row deploys is deleted, with the
// warning and the count: it has no row at all, or its row never deployed it
// (a launch that read no fence created it). A Job already being deleted is
// left to finish.
func TestObserver_DeletesALiveJobOfAFencedRunWithNoStartedRow(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	runID := uuid.NewString()
	_, err := h.db.Exec(`INSERT INTO cancelled_runs (run_id) VALUES ($1)`, runID)
	require.NoError(t, err)
	h.cache.set("job-no-row", liveJobOf(runID, h.now))
	h.unstartedRow(t, runID, "job-reserved-row", false)
	h.cache.set("job-reserved-row", liveJobOf(runID, h.now))
	deleting := liveJobOf(runID, h.now)
	deleting.JobDeleting = true
	h.cache.set("job-deleting", deleting)

	for _, name := range []string{"job-no-row", "job-reserved-row", "job-deleting"} {
		require.NoError(t, h.o.Process(ctx, name))
	}

	assert.Equal(t, []string{"job-no-row", "job-reserved-row"}, h.term.deleted)
	assert.Equal(t, []string{"fenced_run", "fenced_run"}, h.stop.reaped)
	assert.Empty(t, h.reports(t), "the Jobs had no attempt to report")
}

// A Job Kubernetes created although its create call failed, whose row then
// ended failed without deploying it, is deleted when the watch sees it, in a
// run that is not fenced: the attempt was reported failed, and state may be
// running the task's next attempt.
func TestObserver_DeletesAJobWhoseRowFailedBeforeItWasDeployed(t *testing.T) {
	h := newHarness(t)
	runID := uuid.NewString()
	failed := h.unstartedRow(t, runID, "job-created-late", true)
	obs := liveJobOf(runID, h.now)
	obs.JobTaskID = failed.Command().TaskID
	h.cache.set("job-created-late", obs)

	require.NoError(t, h.o.Process(context.Background(), "job-created-late"))

	assert.Equal(t, []string{"job-created-late"}, h.term.deleted)
	assert.Equal(t, []string{"ended_undeployed"}, h.stop.reaped)
	var status string
	require.NoError(t, h.db.Get(&status, `SELECT status FROM deployments WHERE id = $1`, failed.ID()))
	assert.Equal(t, string(model.StatusFailed), status, "the row keeps how it ended")
	assert.Empty(t, h.reports(t))
}

// A live Job of a run that is not fenced, with no row, or with a row that has
// not deployed it yet, is left alone: a dispatch of its attempt adopts it, or
// the launch in progress records it.
func TestObserver_LeavesAnUnfencedJobWithNoRowForAdoption(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.cache.set("job-orphan", liveJobOf(uuid.NewString(), h.now))
	runID := uuid.NewString()
	h.unstartedRow(t, runID, "job-launching", false)
	h.cache.set("job-launching", liveJobOf(runID, h.now))
	h.cache.set("job-of-a-release-leg", model.JobObservation{JobPresent: true})

	for _, name := range []string{"job-orphan", "job-launching", "job-of-a-release-leg"} {
		require.NoError(t, h.o.Process(ctx, name))
	}

	assert.Empty(t, h.term.deleted)
	assert.Empty(t, h.stop.reaped)
}

// watchOver points the observer's cache, live reads, deletions and log reads
// at cs's namespace ns, through the real watch and Job control, once the
// watch has synced.
func (h *harness) watchOver(t *testing.T, cs *fake.Clientset, ns string) *watch.Watch {
	t.Helper()
	w, err := watch.New(cs, ns, h.q)
	require.NoError(t, err)
	watchCtx, stopWatch := context.WithCancel(context.Background())
	t.Cleanup(stopWatch)
	w.Start(watchCtx)
	require.True(t, w.WaitForCacheSync(watchCtx))
	jobs := k8s.NewJobControl(k8s.NewK8sClientFor(cs, slog.New(slog.NewTextHandler(io.Discard, nil)), nil), ns)
	h.o.deps.Cache, h.o.deps.Lookup, h.o.deps.Terminator, h.o.deps.Logs = w, w, jobs, jobs
	return w
}

// The stop of a running attempt is reported only once its pod is gone, read
// through the watch over a Kubernetes API: while the pod is being deleted
// nothing is reported and its log is kept; once it is gone the attempt is
// reported stopped, once, with that log.
func TestObserver_ReportsStoppedOnlyOnceThePodIsGone(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	dep := h.started(t, 0, h.now.Add(-time.Minute))
	_, err := dep.MarkRunning()
	require.NoError(t, err)
	effect, err := dep.Stop(h.now)
	require.NoError(t, err)
	require.Equal(t, model.StopTerminate, effect)
	require.NoError(t, h.repo.Save(ctx, dep))
	require.NotNil(t, h.reload(t, dep.JobName()).Facts().StopRequestedAt)

	const ns = "jobs"
	cmd := dep.Command()
	labels := map[string]string{"app": "dbt-job", "task-id": cmd.TaskID, "attempt": "0", "schedule-id": cmd.ScheduleID}
	deletingSince := metav1.NewTime(h.now.Add(-5 * time.Second))
	cs := fake.NewClientset(
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: dep.JobName(), Namespace: ns, Labels: labels}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: dep.JobName() + "-x7k2p", Namespace: ns, DeletionTimestamp: &deletingSince,
				Finalizers: []string{"batch.kubernetes.io/job-tracking"},
				Labels:     map[string]string{"app": "dbt-job", batchv1.JobNameLabel: dep.JobName(), "job-name": dep.JobName()}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "dbt-job", Image: "service-1:v42"}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "dbt-job",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(h.now.Add(-40 * time.Second))}}}}},
		})
	w := h.watchOver(t, cs, ns)

	require.NoError(t, h.o.Process(ctx, dep.JobName()))

	assert.Empty(t, h.reports(t), "nothing is reported while the pod is being deleted")
	kept := h.reload(t, dep.JobName())
	assert.Equal(t, model.StatusTerminating, kept.Status())
	assert.NotEmpty(t, kept.Facts().LogKey, "the log of the pod awaiting its stop is kept at once")
	_, err = cs.BatchV1().Jobs(ns).Get(ctx, dep.JobName(), metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err), "the Job of the terminating row is deleted")

	require.NoError(t, cs.CoreV1().Pods(ns).Delete(ctx, dep.JobName()+"-x7k2p", metav1.DeleteOptions{}))
	require.Eventually(t, func() bool {
		o := w.Observe(dep.JobName())
		return !o.JobPresent && len(o.Pods) == 0
	}, 5*time.Second, 10*time.Millisecond, "the watch sees the pod gone")
	require.NoError(t, h.o.Process(ctx, dep.JobName()))
	require.NoError(t, h.o.Process(ctx, dep.JobName()))

	reports := h.reports(t)
	require.Len(t, reports, 1, "the stop is reported once")
	assert.Equal(t, "stopped", reports[0]["status"])
	assert.Equal(t, kept.Facts().LogKey, reports[0]["log_s3_key"])
	got := h.reload(t, dep.JobName())
	assert.Equal(t, model.StatusDone, got.Status())
	assert.Equal(t, model.OutcomeStopped, got.Outcome())
	assert.Equal(t, 1, h.stop.stopped, "the stop is counted once")
}

// A Job with no record, of a run that is not fenced when the watch first sees
// it, is left for adoption; once the run is fenced, the next resync queues it
// with every Job the cache holds that can still run a pod, each name once, and
// the Job is deleted. A started deployment's Job the cache also holds is
// queued once; a finished Job and one being deleted are not queued.
func TestResync_QueuesEveryCachedJobSoAFencedRunsRecordlessJobIsDeleted(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const ns = "jobs"
	runID, taskID := uuid.NewString(), uuid.NewString()
	started := h.started(t, 0, h.now.Add(-time.Minute))
	jobOf := func(name, task, run string) *batchv1.Job {
		return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{
			"app": "dbt-job", "task-id": task, "attempt": "0", "schedule-id": run}}}
	}
	completed := jobOf("job-completed", uuid.NewString(), runID)
	completed.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	failed := jobOf("job-failed", uuid.NewString(), runID)
	failed.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	deleting := jobOf("job-deleting", uuid.NewString(), runID)
	deletedAt := metav1.NewTime(h.now)
	deleting.DeletionTimestamp, deleting.Finalizers = &deletedAt, []string{"batch.kubernetes.io/job-tracking"}
	cs := fake.NewClientset(jobOf("job-created-anyway", taskID, runID),
		jobOf(started.JobName(), started.Command().TaskID, started.Command().ScheduleID), completed, failed, deleting)
	h.watchOver(t, cs, ns)
	queued := func() []string {
		h.q.mu.Lock()
		defer h.q.mu.Unlock()
		return slices.Clone(h.q.added)
	}
	require.Eventually(t, func() bool {
		got := queued()
		return slices.Contains(got, "job-created-anyway") && slices.Contains(got, started.JobName()) &&
			slices.Contains(got, "job-completed") && slices.Contains(got, "job-failed") && slices.Contains(got, "job-deleting")
	}, 5*time.Second, 10*time.Millisecond, "the watch queues every Job it lists")
	require.NoError(t, h.o.Process(ctx, "job-created-anyway"))
	require.Empty(t, h.term.deleted, "a Job of a run that is not fenced is left for adoption")

	_, err := h.db.Exec(`INSERT INTO cancelled_runs (run_id) VALUES ($1)`, runID)
	require.NoError(t, err)
	h.q.mu.Lock()
	h.q.added = nil
	h.q.mu.Unlock()
	require.NoError(t, h.o.Resync(ctx))

	assert.ElementsMatch(t, []string{"job-created-anyway", started.JobName()}, queued(),
		"every cached Job that can run a pod and every started deployment, each once")
	require.NoError(t, h.o.Process(ctx, "job-created-anyway"))
	assert.Equal(t, []string{"fenced_run"}, h.stop.reaped)
	_, err = cs.BatchV1().Jobs(ns).Get(ctx, "job-created-anyway", metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err), "the fenced run's Job is deleted")
}

// Two nodes of one run can share a truncated Job name. A live Job is judged
// by the attempt its labels name, not by the newest row of its name: another
// attempt's row refused on the name collision does not get this Job deleted,
// while the Job of an attempt whose own row failed undeployed is. A Job with
// no attempt labels (a release leg's) is judged by its name.
func TestObserver_JudgesAnUndeployedJobByTheAttemptItRuns(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	runID := uuid.NewString()
	collided := h.unstartedRow(t, runID, "job-shared-name", true)
	h.cache.set("job-shared-name", liveJobOf(runID, h.now))

	failed := h.unstartedRow(t, runID, "job-own-failure", true)
	own := liveJobOf(runID, h.now)
	own.JobTaskID = failed.Command().TaskID
	h.cache.set("job-own-failure", own)
	newer := h.unstartedRow(t, runID, "job-own-failure", false)
	_, err := h.db.Exec(`UPDATE deployments SET created_at = created_at + interval '1 second' WHERE id = $1`, newer.ID())
	require.NoError(t, err, "another attempt's row of the name is the newest")

	h.unstartedRow(t, uuid.NewString(), "job-of-a-leg", true)
	h.cache.set("job-of-a-leg", model.JobObservation{JobPresent: true})

	for _, name := range []string{"job-shared-name", "job-own-failure", "job-of-a-leg"} {
		require.NoError(t, h.o.Process(ctx, name))
	}

	assert.Equal(t, []string{"job-own-failure", "job-of-a-leg"}, h.term.deleted,
		"the row of attempt %d of task %s, refused on the shared name, does not get another attempt's Job deleted",
		collided.Command().Attempt, collided.Command().TaskID)
	assert.Equal(t, []string{"ended_undeployed", "ended_undeployed"}, h.stop.reaped)
}
