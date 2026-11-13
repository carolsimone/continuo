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
	"github.com/carolsimone/continuo/execution-controller/service/ports"
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

// cachedJob is a JobCache that holds one Job, job, under name.
type cachedJob struct {
	name string
	job  model.JobObservation
}

func (cachedJob) Synced() bool { return true }
func (c cachedJob) Observe(name string) model.JobObservation {
	if name != c.name {
		return model.JobObservation{}
	}
	return c.job
}
func (c cachedJob) JobNames() []string { return []string{c.name} }

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
	evt := events.TaskDispatch{TenantID: "default", RunID: uuid.New(), TaskID: uuid.New(), Attempt: 0,
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

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, noJobs{}, fakeResolver{}, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, out.created, 1, "exactly one report is written")
	want, err := json.Marshal(serialization.ReportFromDomain(cmd, *winner.LastReport()))
	require.NoError(t, err)
	_, report, err := outbox.UnwrapTenant(out.created[0].Payload)
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(report), "the report the winner stored")
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

// acknowledgedDispatch is a question from run-controller about attempt 1 of a task it
// knows as running.
func acknowledgedDispatch() events.TaskDispatch {
	return events.TaskDispatch{TenantID: "default", RunID: uuid.New(), TaskID: uuid.New(), Attempt: 1,
		KnownStatus: pkgmodel.AttemptStatusRunning, ScheduleName: "daily", ServiceName: "svc", SchemaName: "sch",
		TableName: "tbl", NodeType: "dbt-model", Operation: "run", ImageTag: "svc:1",
		JobName: "svc-sch-tbl-7f3e1a2b-r1", TimeoutSeconds: 3600}
}

// decodeReport decodes the task.status.updated:v2 payload of an outbox entry.
func decodeReport(t *testing.T, e *outbox.Entry) map[string]any {
	t.Helper()
	_, payload, err := outbox.UnwrapTenant(e.Payload)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(payload, &m))
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

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, live, fakeResolver{}, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

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

			require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, live, fakeResolver{}, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

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

	err := handlers.NewTaskDispatchHandler(noJobs{}, live, fakeResolver{}, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New())

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

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, live, fakeResolver{}, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

	assert.Zero(t, live.reads)
	assert.Equal(t, model.StatusPending, repo.stored[attemptKey(evt.TaskID.String(), 1)].Status())
}

// An acknowledged attempt with neither a record nor a Job, whose pod a live
// read still shows running (its Job was deleted with orphan propagation), is
// not reported lost: nothing is stored or reported, and run-controller asks again.
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

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, live, fakeResolver{}, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

	assert.Equal(t, 1, live.reads)
	assert.Zero(t, repo.addAttempts, "nothing is stored")
	assert.Empty(t, out.created, "nothing is reported")
}

// A dispatch of a fenced run with neither a record nor a Job of the attempt,
// but a pod of it a live read shows running, is not answered stopped while
// the pod can still write: nothing is stored or reported, and run-controller's next
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

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, live, fakeResolver{}, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

	assert.Equal(t, 1, live.reads)
	assert.Zero(t, repo.addAttempts, "nothing is stored")
	assert.Empty(t, out.created, "nothing is reported")
	assert.Equal(t, []string{"dispatch"}, cancelled.holds, "the dispatch held the run before it read the fence")
}

// The tenant of a dispatch is kept on the attempt's record, and the report
// the dispatch is answered with is stored under it.
func TestTaskDispatch_StoresTheDispatchsTenant(t *testing.T) {
	evt := acknowledgedDispatch()
	evt.KnownStatus = pkgmodel.AttemptStatusDispatched
	evt.TenantID = "acme"
	repo := &stubDeploymentsRepo{}
	out := &recordingOutbox{}
	u := newFakeUoW(repo, &recordingCancelledRepo{})
	u.Outbox = out
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, noJobs{}, fakeResolver{}, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

	stored := repo.stored[attemptKey(evt.TaskID.String(), 1)]
	require.NotNil(t, stored)
	assert.Equal(t, "acme", stored.Command().TenantID)
	assert.Equal(t, "acme", stored.TenantID())
	require.Len(t, out.created, 1)
	tenantID, _, err := outbox.UnwrapTenant(out.created[0].Payload)
	require.NoError(t, err)
	assert.Equal(t, "acme", tenantID, "the report is stored under the dispatch's tenant")
}

// fullRefreshDispatch is a new dispatch of a full refresh of a node of
// nodeType in the finance service.
func fullRefreshDispatch(nodeType pkgmodel.NodeType) events.TaskDispatch {
	evt := acknowledgedDispatch()
	evt.KnownStatus = pkgmodel.AttemptStatusDispatched
	evt.ServiceName, evt.NodeType, evt.Operation = "finance", string(nodeType), string(pkgmodel.OperationFullRefresh)
	return evt
}

// A full refresh whose dbt-commands block defines no such command can never
// run: the attempt is recorded failed with the reason that names the missing
// command, before any Job is made.
func TestTaskDispatchHandler_RefusesAFullRefreshWhoseBlockLacksTheCommand(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	undefined := fakeResolver{blocks: map[string]bool{"finance/dbt-seed": false, "finance/dbt-model": false}}
	for nodeType, want := range map[pkgmodel.NodeType]struct {
		reason  pkgmodel.FailureReason
		message string
	}{
		pkgmodel.NodeTypeDbtSeed:  {pkgmodel.FailureReasonSeedFullRefreshUndefined, "dbt-commands services.finance defines no seed_full_refresh command"},
		pkgmodel.NodeTypeDbtModel: {pkgmodel.FailureReasonFullRefreshUndefined, "dbt-commands services.finance defines no full_refresh command"},
	} {
		t.Run(string(nodeType), func(t *testing.T) {
			evt := fullRefreshDispatch(nodeType)
			repo := &stubDeploymentsRepo{}
			out := &recordingOutbox{}
			u := newFakeUoW(repo, &recordingCancelledRepo{})
			u.Outbox = out

			require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, noJobs{}, undefined, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

			d := repo.stored[attemptKey(evt.TaskID.String(), evt.Attempt)]
			require.NotNil(t, d)
			assert.Equal(t, model.StatusFailed, d.Status(), "no Job is requested")
			assert.Equal(t, want.reason, d.Facts().Reason)
			require.Len(t, out.created, 1)
			report := decodeReport(t, out.created[0])
			assert.Equal(t, "failed", report["status"])
			assert.Equal(t, "invalid", report["failure_class"])
			assert.Equal(t, string(want.reason), report["failure_reason"])
			assert.Equal(t, want.message, report["error_message"])
		})
	}
}

// A full refresh whose block defines the command, and a run whose block
// lacks a full refresh command, are queued for their Job.
func TestTaskDispatchHandler_QueuesAFullRefreshWhoseBlockDefinesTheCommand(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	for name, c := range map[string]struct {
		evt      events.TaskDispatch
		resolver fakeResolver
	}{
		"a defined seed full refresh": {fullRefreshDispatch(pkgmodel.NodeTypeDbtSeed), fakeResolver{}},
		"a seed run": {func() events.TaskDispatch {
			evt := fullRefreshDispatch(pkgmodel.NodeTypeDbtSeed)
			evt.Operation = string(pkgmodel.OperationRun)
			return evt
		}(), fakeResolver{blocks: map[string]bool{"finance/dbt-seed": false}}},
	} {
		t.Run(name, func(t *testing.T) {
			repo := &stubDeploymentsRepo{}
			out := &recordingOutbox{}
			u := newFakeUoW(repo, &recordingCancelledRepo{})
			u.Outbox = out

			require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, noJobs{}, c.resolver, noStops{}, logger).Handle(context.Background(), u, c.evt, uuid.New()))

			require.Len(t, out.created, 1)
			assert.Equal(t, "queued", decodeReport(t, out.created[0])["status"])
		})
	}
}

// A full refresh whose block no longer defines the command, with a Job of the
// attempt that survives its lost record (made while the block defined the
// command), adopts the Job rather than refusing the attempt: refusing it would
// end the task while its pod may still write the table. The cache can trail
// the API, so a Job only a live read shows is adopted too.
func TestTaskDispatchHandler_AFullRefreshTheBlockNoLongerDefinesAdoptsItsSurvivingJob(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	undefined := fakeResolver{blocks: map[string]bool{"finance/dbt-model": false}}
	for _, known := range []pkgmodel.AttemptStatus{pkgmodel.AttemptStatusDispatched, pkgmodel.AttemptStatusRunning} {
		for _, where := range []string{"in the cache", "only in a live read"} {
			t.Run(string(known)+" "+where, func(t *testing.T) {
				evt := fullRefreshDispatch(pkgmodel.NodeTypeDbtModel)
				evt.KnownStatus = known
				job := model.JobObservation{JobPresent: true, JobTaskID: evt.TaskID.String(), JobAttempt: "1"}
				var cache ports.JobCache = noJobs{}
				live := &liveJobs{}
				if where == "in the cache" {
					cache = cachedJob{name: evt.JobName, job: job}
				} else {
					live.job = job
				}
				repo := &stubDeploymentsRepo{}
				out := &recordingOutbox{}
				u := newFakeUoW(repo, &recordingCancelledRepo{})
				u.Outbox = out

				require.NoError(t, handlers.NewTaskDispatchHandler(cache, live, undefined, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

				stored := repo.stored[attemptKey(evt.TaskID.String(), evt.Attempt)]
				require.NotNil(t, stored)
				assert.Equal(t, model.StatusStarting, stored.Status(), "the record is rebuilt and follows the Job")
				require.Len(t, out.created, 1)
				assert.Equal(t, "queued", decodeReport(t, out.created[0])["status"])
			})
		}
	}
}

// A full refresh whose block no longer defines the command, with no Job of
// the attempt but a pod of it a live read still shows running, is left
// unanswered while the pod can write: nothing is stored or reported.
func TestTaskDispatchHandler_AFullRefreshTheBlockNoLongerDefinesAwaitsAnOrphanPod(t *testing.T) {
	evt := fullRefreshDispatch(pkgmodel.NodeTypeDbtModel)
	started := time.Now()
	live := &liveJobs{job: model.JobObservation{Pods: []model.PodObservation{{Name: "pod-1", Phase: model.PodRunning,
		Main: model.ContainerObservation{Name: "dbt-job", StartedAt: &started}}}}}
	repo := &stubDeploymentsRepo{}
	out := &recordingOutbox{}
	u := newFakeUoW(repo, &recordingCancelledRepo{})
	u.Outbox = out
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	undefined := fakeResolver{blocks: map[string]bool{"finance/dbt-model": false}}

	require.NoError(t, handlers.NewTaskDispatchHandler(noJobs{}, live, undefined, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New()))

	assert.Equal(t, 1, live.reads)
	assert.Zero(t, repo.addAttempts, "nothing is stored")
	assert.Empty(t, out.created, "nothing is reported")
}

// A full refresh whose block lacks the command is refused only once a live
// read finds neither a Job nor an unfinished pod of the attempt; a read that
// fails leaves the dispatch unanswered, and its message is delivered again.
func TestTaskDispatchHandler_AFullRefreshIsRefusedOnlyOnceALiveReadFindsNothingOfIt(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	undefined := fakeResolver{blocks: map[string]bool{"finance/dbt-model": false}}
	for name, c := range map[string]struct {
		live    *liveJobs
		wantErr bool
	}{
		"nothing of it runs": {&liveJobs{}, false},
		"the read fails":     {&liveJobs{err: errors.New("the server was unable to return a response in the time allotted")}, true},
		"another task's Job": {&liveJobs{job: model.JobObservation{JobPresent: true, JobTaskID: uuid.NewString(), JobAttempt: "1"}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			evt := fullRefreshDispatch(pkgmodel.NodeTypeDbtModel)
			repo := &stubDeploymentsRepo{}
			out := &recordingOutbox{}
			u := newFakeUoW(repo, &recordingCancelledRepo{})
			u.Outbox = out

			err := handlers.NewTaskDispatchHandler(noJobs{}, c.live, undefined, noStops{}, logger).Handle(context.Background(), u, evt, uuid.New())

			assert.Equal(t, 1, c.live.reads, "a live read confirms nothing of the attempt runs")
			if c.wantErr {
				require.Error(t, err)
				assert.Zero(t, repo.addAttempts, "nothing is stored")
				assert.Empty(t, out.created, "nothing is reported")
				return
			}
			require.NoError(t, err)
			d := repo.stored[attemptKey(evt.TaskID.String(), evt.Attempt)]
			require.NotNil(t, d)
			assert.Equal(t, model.StatusFailed, d.Status())
			assert.Equal(t, pkgmodel.FailureReasonFullRefreshUndefined, d.Facts().Reason)
			require.Len(t, out.created, 1)
			assert.Equal(t, "full_refresh_undefined", decodeReport(t, out.created[0])["failure_reason"])
		})
	}
}
