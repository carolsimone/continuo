package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/events"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/serialization"
	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noJobs is a JobCache, and a JobLookup, that holds no Job.
type noJobs struct{}

func (noJobs) Synced() bool                        { return true }
func (noJobs) Observe(string) model.JobObservation { return model.JobObservation{} }
func (noJobs) JobNames() []string                  { return nil }
func (noJobs) ObserveLive(context.Context, string) (model.JobObservation, error) {
	return model.JobObservation{}, nil
}
func (noJobs) ObserveRun(context.Context, string) (model.RunObservation, error) {
	return model.RunObservation{}, nil
}

// noStops is a ports.StopMetrics that counts nothing.
type noStops struct{}

func (noStops) AttemptStopped()  {}
func (noStops) JobReaped(string) {}

// recordingOutbox keeps every outbox entry written.
type recordingOutbox struct {
	outbox.Repository
	created []*outbox.Entry
}

func (r *recordingOutbox) Create(_ context.Context, e *outbox.Entry) error {
	r.created = append(r.created, e)
	return nil
}

// A new attempt delivered to two replicas at once: the delivery whose insert
// loses the race answers with the report the winner stored. It writes exactly
// that report, no queued report of its own, and stores no deployment of its
// own, so the dispatcher requests no second Job.
func TestTaskDispatch_ALostInsertRaceResendsTheStoredReport(t *testing.T) {
	evt := events.TaskDispatch{RunID: uuid.New(), TaskID: uuid.New(), Attempt: 0,
		KnownStatus: pkgmodel.AttemptStatusDispatched, ScheduleName: "daily", ServiceName: "svc", SchemaName: "sch",
		TableName: "tbl", NodeType: "dbt-model", Operation: "run", ImageTag: "svc:1",
		JobName: "svc-sch-tbl-7f3e1a2b", TimeoutSeconds: 3600}
	cmd := evt.Command()
	accepted := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	started := accepted.Add(time.Minute)
	winner := model.NewDeployment(cmd, nil, accepted)
	winner.Observe(model.AttemptFacts{StartedAt: &started})
	winner.RecordReport(winner.ReportOf(pkgmodel.AttemptStatusRunning, ""))
	key := attemptKey(cmd.TaskID, cmd.Attempt)
	repo := &stubDeploymentsRepo{storedByRace: map[string]*model.Deployment{key: winner}}
	out := &recordingOutbox{}
	u := newFakeUoW(repo, &recordingCancelledRepo{})
	u.Outbox = out
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, noJobs{}, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, out.created, 1, "exactly one report is written")
	want, err := json.Marshal(serialization.ReportFromDomain(cmd, *winner.LastReport()))
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(out.created[0].Payload), "the report the winner stored")
	assert.Equal(t, 1, repo.addAttempts, "the attempt was offered once")
	assert.Equal(t, map[string]*model.Deployment{key: winner}, repo.stored,
		"only the winner's deployment is stored, so no second Job is requested")
	assert.Empty(t, repo.added)
}

// liveJobs is a JobLookup: the Kubernetes API behind a cache that has not
// caught up. It holds job, or answers every read with err, and counts reads.
type liveJobs struct {
	job   model.JobObservation
	err   error
	reads int
}

func (l *liveJobs) ObserveLive(context.Context, string) (model.JobObservation, error) {
	l.reads++
	return l.job, l.err
}

// acknowledgedDispatch is a question from state about attempt 1 of a task it
// knows as running.
func acknowledgedDispatch() events.TaskDispatch {
	return events.TaskDispatch{RunID: uuid.New(), TaskID: uuid.New(), Attempt: 1,
		KnownStatus: pkgmodel.AttemptStatusRunning, ScheduleName: "daily", ServiceName: "svc", SchemaName: "sch",
		TableName: "tbl", NodeType: "dbt-model", Operation: "run", ImageTag: "svc:1",
		JobName: "svc-sch-tbl-7f3e1a2b-r1", TimeoutSeconds: 3600}
}

// decodeReport decodes the task.status.updated:v2 payload of an outbox entry.
func decodeReport(t *testing.T, e *outbox.Entry) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(e.Payload, &m))
	return m
}

// An acknowledged attempt with no record whose Job the cache does not hold is
// read from the API before it is reported lost: the cache can trail the API.
// A live Job of the attempt rebuilds its record instead.
func TestTaskDispatch_AnAcknowledgedAttemptsJobMissingFromTheCacheIsAdoptedFromALiveRead(t *testing.T) {
	evt := acknowledgedDispatch()
	live := &liveJobs{job: model.JobObservation{JobPresent: true, JobTaskID: evt.TaskID.String(), JobAttempt: "1"}}
	repo := &stubDeploymentsRepo{}
	out := &recordingOutbox{}
	u := newFakeUoW(repo, &recordingCancelledRepo{})
	u.Outbox = out
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, live, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

	assert.Equal(t, 1, live.reads)
	stored := repo.stored[attemptKey(evt.TaskID.String(), 1)]
	require.NotNil(t, stored)
	assert.Equal(t, model.StatusStarting, stored.Status(), "the record is rebuilt and follows the Job")
	require.Len(t, out.created, 1)
	assert.Equal(t, "queued", decodeReport(t, out.created[0])["status"])
}

// Only a live read that confirms the Job is gone reports the attempt lost.
func TestTaskDispatch_AnAcknowledgedAttemptIsLostOnlyOnceALiveReadFindsNoJob(t *testing.T) {
	for name, job := range map[string]model.JobObservation{
		"no Job":             {},
		"another task's Job": {JobPresent: true, JobTaskID: uuid.NewString(), JobAttempt: "1"},
	} {
		t.Run(name, func(t *testing.T) {
			evt := acknowledgedDispatch()
			live := &liveJobs{job: job}
			repo := &stubDeploymentsRepo{}
			out := &recordingOutbox{}
			u := newFakeUoW(repo, &recordingCancelledRepo{})
			u.Outbox = out
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

			require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, live, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

			assert.Equal(t, 1, live.reads)
			stored := repo.stored[attemptKey(evt.TaskID.String(), 1)]
			require.NotNil(t, stored)
			assert.Equal(t, model.StatusFailed, stored.Status())
			assert.Equal(t, pkgmodel.FailureReasonAttemptLost, stored.Facts().Reason)
			require.Len(t, out.created, 1)
			report := decodeReport(t, out.created[0])
			assert.Equal(t, "failed", report["status"])
			assert.Equal(t, "infra", report["failure_class"])
			assert.Equal(t, "attempt_lost", report["failure_reason"])
		})
	}
}

// When the Job cannot be read, the dispatch is not answered: the error is
// returned, nothing is stored or reported, and the message is delivered again.
func TestTaskDispatch_AnUnreadableJobLeavesAnAcknowledgedAttemptUnanswered(t *testing.T) {
	evt := acknowledgedDispatch()
	live := &liveJobs{err: errors.New("the server was unable to return a response in the time allotted")}
	repo := &stubDeploymentsRepo{}
	out := &recordingOutbox{}
	u := newFakeUoW(repo, &recordingCancelledRepo{})
	u.Outbox = out
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	err := handlers.NewTaskDispatchHandler(noJobs{}, live, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New())

	require.Error(t, err)
	assert.Equal(t, 1, live.reads)
	assert.Zero(t, repo.addAttempts, "nothing is stored")
	assert.Empty(t, out.created, "nothing is reported")
}

// A new dispatch, and a question whose Job the cache holds, need no live read:
// the dispatcher's creation adopts a Job of the same attempt.
func TestTaskDispatch_OnlyAnAttemptAboutToBeReportedLostIsReadLive(t *testing.T) {
	evt := acknowledgedDispatch()
	evt.KnownStatus = pkgmodel.AttemptStatusDispatched
	live := &liveJobs{}
	repo := &stubDeploymentsRepo{}
	u := newFakeUoW(repo, &recordingCancelledRepo{})
	u.Outbox = &recordingOutbox{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, live, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

	assert.Zero(t, live.reads)
	assert.Equal(t, model.StatusPending, repo.stored[attemptKey(evt.TaskID.String(), 1)].Status())
}

// An acknowledged attempt with neither a record nor a Job, whose pod a live
// read still shows running (its Job was deleted with orphan propagation), is
// not reported lost: nothing is stored or reported, and state asks again.
func TestTaskDispatch_AnAcknowledgedAttemptWithAnOrphanPodIsNotReportedLost(t *testing.T) {
	evt := acknowledgedDispatch()
	started := time.Now()
	live := &liveJobs{job: model.JobObservation{Pods: []model.PodObservation{{Name: "pod-1", Phase: model.PodRunning,
		Main: model.ContainerObservation{Name: "dbt-job", StartedAt: &started}}}}}
	repo := &stubDeploymentsRepo{}
	out := &recordingOutbox{}
	u := newFakeUoW(repo, &recordingCancelledRepo{})
	u.Outbox = out
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, live, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

	assert.Equal(t, 1, live.reads)
	assert.Zero(t, repo.addAttempts, "nothing is stored")
	assert.Empty(t, out.created, "nothing is reported")
}

// A dispatch of a fenced run with neither a record nor a Job of the attempt,
// but a pod of it a live read shows running, is not answered stopped while
// the pod can still write: nothing is stored or reported, and state's next
// cancel names the attempt again.
func TestTaskDispatch_AFencedRunsAttemptWithAnOrphanPodIsLeftUnanswered(t *testing.T) {
	evt := acknowledgedDispatch()
	started := time.Now()
	live := &liveJobs{job: model.JobObservation{Pods: []model.PodObservation{{Name: "pod-1", Phase: model.PodRunning,
		Main: model.ContainerObservation{Name: "dbt-job", StartedAt: &started}}}}}
	repo := &stubDeploymentsRepo{}
	out := &recordingOutbox{}
	cancelled := &recordingCancelledRepo{fenced: true}
	u := newFakeUoW(repo, cancelled)
	u.Outbox = out
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, live, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

	assert.Equal(t, 1, live.reads)
	assert.Zero(t, repo.addAttempts, "nothing is stored")
	assert.Empty(t, out.created, "nothing is reported")
	assert.Equal(t, []string{"dispatch"}, cancelled.holds, "the dispatch held the run before it read the fence")
}
