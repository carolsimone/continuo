package reaper_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/service/reaper"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reapAt is the instant every test reaps at: past the lease deadline the
// fixtures claim under.
var reapAt = time.Unix(200, 0)

const backoff = 30 * time.Second

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fixedClock reports one instant, so a transition's timestamps are exact.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// deletedPod is one pod deletion the reaper asked the runtime for.
type deletedPod struct{ name, uid string }

type recordingTerminator struct {
	mu      sync.Mutex
	deleted []deletedPod
	err     error
}

func (t *recordingTerminator) DeletePod(_ context.Context, podName, podUID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return t.err
	}
	t.deleted = append(t.deleted, deletedPod{name: podName, uid: podUID})
	return nil
}

func (t *recordingTerminator) calls() []deletedPod {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]deletedPod(nil), t.deleted...)
}

// stubDeployments serves one expired row and records what is saved.
type stubDeployments struct {
	repository.DeploymentRepository
	expired *model.Deployment
	saved   []*model.Deployment
}

// GetExpiredLeaseForUpdate mirrors the adapter: it serves rows still leased or
// running whose deadline has passed, and nothing once one has settled.
func (r *stubDeployments) GetExpiredLeaseForUpdate(_ context.Context, now time.Time) (*model.Deployment, error) {
	dep := r.expired
	if dep == nil {
		return nil, nil
	}
	status := dep.Status()
	if status != model.StatusLeased && status != model.StatusRunning {
		return nil, nil
	}
	if lease := dep.ActiveLease(); lease == nil || !lease.ExpiresAt.Before(now) {
		return nil, nil
	}
	return dep, nil
}

func (r *stubDeployments) Save(_ context.Context, d *model.Deployment) error {
	r.saved = append(r.saved, d)
	return nil
}

type stubOutbox struct {
	pkgoutbox.Repository
	entries []*pkgoutbox.Entry
}

func (r *stubOutbox) Create(_ context.Context, e *pkgoutbox.Entry) error {
	r.entries = append(r.entries, e)
	return nil
}

func (r *stubOutbox) eventTypes() []string {
	out := make([]string, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e.EventType)
	}
	return out
}

func (r *stubOutbox) entry(t *testing.T, eventType string) *pkgoutbox.Entry {
	t.Helper()
	for _, e := range r.entries {
		if e.EventType == eventType {
			return e
		}
	}
	t.Fatalf("no %q announcement was written; got %v", eventType, r.eventTypes())
	return nil
}

// leasedTask builds a worker task claimed by a pod at time 100 whose lease ran
// out at 150, on a task that has retryCount attempts behind it out of maxRetries.
func leasedTask(t *testing.T, retryCount, maxRetries int) (*model.Deployment, uuid.UUID) {
	t.Helper()
	dep := model.NewWorkerDeployment(command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: "dbt-model", ImageTag: "sha-abc",
		TaskRetryCount: retryCount, TaskMaxRetries: maxRetries,
	}, uuid.Nil, "pool-abc", time.Unix(90, 0))
	leaseID := uuid.New()
	require.NoError(t, dep.Claim(leaseID, "digest", "worker-1", "dbt-worker-abc-1", "uid-1",
		time.Unix(100, 0), time.Unix(150, 0), []string{"dbt", "run"}, model.ExecutionPathNative))
	return dep, leaseID
}

// running marks a claimed task's dbt process as started, the state a worker
// spends its run in.
func running(t *testing.T, dep *model.Deployment, leaseID uuid.UUID) {
	t.Helper()
	_, err := dep.AcknowledgeStart(leaseID, "digest", time.Unix(110, 0))
	require.NoError(t, err)
}

// harness wires a Reaper to stubs and hands back what they recorded.
type harness struct {
	reaper *reaper.Reaper
	repo   *stubDeployments
	outbox *stubOutbox
	pods   *recordingTerminator
}

func newHarness(dep *model.Deployment, pods *recordingTerminator) *harness {
	repo := &stubDeployments{expired: dep}
	out := &stubOutbox{}
	u := &uow.FakeUnitOfWork{Deployments: repo, Outbox: out}
	return &harness{
		reaper: reaper.NewReaper(reaper.Config{
			UnitOfWork:   func() uow.UnitOfWork { return u },
			Pods:         pods,
			Clock:        fixedClock{t: reapAt},
			RetryBackoff: backoff,
			Logger:       testLogger(),
		}),
		repo:   repo,
		outbox: out,
		pods:   pods,
	}
}

// TestReapOne_ParksARetryableTaskAndStopsItsPod pins what an expired lease with
// attempts left costs and recovers: the pod that stopped reporting is stopped
// for real, the task is parked for another attempt, and the execution slot it
// held goes back to the executor's budget.
func TestReapOne_ParksARetryableTaskAndStopsItsPod(t *testing.T) {
	dep, leaseID := leasedTask(t, 0, 3)
	running(t, dep, leaseID)
	h := newHarness(dep, &recordingTerminator{})

	found, err := h.reaper.ReapOne(context.Background())
	require.NoError(t, err)
	assert.True(t, found, "the expired lease was there to reap")

	assert.Equal(t, []deletedPod{{name: "dbt-worker-abc-1", uid: "uid-1"}}, h.pods.calls(),
		"the pod is deleted by the UID its lease named")
	assert.Equal(t, model.StatusRetryPending, dep.Status())
	assert.Equal(t, reapAt.Add(backoff), dep.NextAttemptAt(), "requeued after its backoff")
	require.NotNil(t, dep.SlotReleasedAt(), "the expired lease must not keep its execution slot")
	assert.Equal(t, reapAt, *dep.SlotReleasedAt())
	assert.Len(t, h.repo.saved, 1, "the transition is persisted")
	assert.Equal(t, []string{"task_status_updated", "task_execution_recorded", "task_retry"},
		h.outbox.eventTypes(),
		"the attempt is announced failed and recorded, and the next one is asked for")
}

// TestReapOne_FailsATaskOutOfAttemptsAndStopsItsPod pins the other end of the
// budget. A task whose final attempt was the one that went silent has nothing
// left to try, so it settles FAILED — node.updated:v1 is what lets the run
// advance past a node no worker will ever report on.
func TestReapOne_FailsATaskOutOfAttemptsAndStopsItsPod(t *testing.T) {
	dep, leaseID := leasedTask(t, 2, 3)
	running(t, dep, leaseID)
	h := newHarness(dep, &recordingTerminator{})

	found, err := h.reaper.ReapOne(context.Background())
	require.NoError(t, err)
	assert.True(t, found)

	assert.Equal(t, []deletedPod{{name: "dbt-worker-abc-1", uid: "uid-1"}}, h.pods.calls())
	assert.Equal(t, model.StatusFailed, dep.Status())
	require.NotNil(t, dep.SlotReleasedAt(), "a permanently failed task returns its slot too")
	assert.Equal(t, []string{"task_status_updated", "task_execution_recorded", "node_updated"},
		h.outbox.eventTypes(), "the node settles so the run advances; no retry is asked for")

	var node struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(h.outbox.entry(t, "node_updated").Payload, &node))
	assert.Equal(t, "FAILED", node.Status)
}

// TestReapOne_FencesTheWorkerOnEitherPath pins that the reaped worker can no
// longer drive its task. It is the whole point of reaping: the task is about to
// be given to somebody else, and a late report from the pod that lost it must be
// answered "your lease is gone" — which is terminal for the worker — rather than
// applied, or answered with a fault it would retry against forever.
func TestReapOne_FencesTheWorkerOnEitherPath(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		retryCount, maxRetries int
	}{
		{"attempts left", 0, 3},
		{"out of attempts", 2, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dep, leaseID := leasedTask(t, tc.retryCount, tc.maxRetries)
			running(t, dep, leaseID)
			h := newHarness(dep, &recordingTerminator{})

			_, err := h.reaper.ReapOne(context.Background())
			require.NoError(t, err)

			assert.Nil(t, dep.ActiveLease(), "the reaped lease is gone")
			assert.ErrorIs(t, dep.Complete(leaseID, "digest",
				model.WorkerResult{Succeeded: true}, time.Unix(300, 0)), model.ErrStaleLease,
				"a late success from the reaped worker is fenced, not applied")
			assert.ErrorIs(t, dep.Heartbeat(leaseID, "digest", time.Unix(300, 0), time.Unix(400, 0)),
				model.ErrStaleLease, "the reaped worker cannot revive its lease")
		})
	}
}

// TestReapOne_AnnouncesTheExecutionTheReapedLeaseRan pins that the reaper reads
// the execution's identity before the transition that drops the lease: the
// announcement has to name the lease that ran the attempt and when its dbt
// process started, and after the transition there is no lease left to read it
// from.
func TestReapOne_AnnouncesTheExecutionTheReapedLeaseRan(t *testing.T) {
	dep, leaseID := leasedTask(t, 0, 3)
	running(t, dep, leaseID)
	h := newHarness(dep, &recordingTerminator{})

	_, err := h.reaper.ReapOne(context.Background())
	require.NoError(t, err)

	var rec pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(h.outbox.entry(t, "task_execution_recorded").Payload, &rec))
	assert.Equal(t, leaseID.String(), rec.ExecutionID, "the attempt is named by the lease that ran it")
	assert.Equal(t, "1970-01-01T00:01:50Z", rec.StartedAt,
		"the run started when the reaped lease said it did, not when the reaper noticed")
	assert.Equal(t, reapAt.UTC().Format(time.RFC3339), rec.CompletedAt,
		"the attempt is recorded as finished when the executor gave up on it")
	assert.Equal(t, "worker heartbeat expired", rec.ErrorMessage)
}

// TestReapOne_AnnouncesTheRetryOnTheContractedStream pins that the next attempt
// is asked for on the stream the executor's own retry consumer reads, so a
// reaped task is actually re-offered rather than parked forever.
func TestReapOne_AnnouncesTheRetryOnTheContractedStream(t *testing.T) {
	dep, leaseID := leasedTask(t, 0, 3)
	running(t, dep, leaseID)
	h := newHarness(dep, &recordingTerminator{})

	_, err := h.reaper.ReapOne(context.Background())
	require.NoError(t, err)

	assert.Equal(t, streams.RetryTaskV1, h.outbox.entry(t, "task_retry").StreamName)
}

// TestReapOne_KeepsTheLeaseWhenThePodCannotBeStopped pins the rollback. A
// Kubernetes API the reaper cannot reach means it does not know the pod stopped,
// and a task whose slot it released anyway would let other work start beside a
// dbt process still running. It reports the failure instead, changes nothing,
// and the next tick tries again — the lease it could not fence stays the
// authoritative one, so no other worker can claim the task in the meantime.
func TestReapOne_KeepsTheLeaseWhenThePodCannotBeStopped(t *testing.T) {
	dep, leaseID := leasedTask(t, 0, 3)
	running(t, dep, leaseID)
	h := newHarness(dep, &recordingTerminator{err: errors.New("kubernetes API unreachable")})

	_, err := h.reaper.ReapOne(context.Background())

	require.Error(t, err, "a pod that may still be running must not be reaped silently")
	assert.Equal(t, model.StatusRunning, dep.Status(), "nothing settled")
	assert.Nil(t, dep.SlotReleasedAt(), "the slot is not returned under a live pod")
	require.NotNil(t, dep.ActiveLease(), "the lease stays authoritative")
	assert.Equal(t, leaseID, dep.ActiveLease().ID)
	assert.Empty(t, h.repo.saved)
	assert.Empty(t, h.outbox.entries)
}

// TestReapOne_ReportsNothingToReap pins that an executor whose workers are all
// reporting does nothing: the reaper polls constantly, and a tick that finds no
// expired lease must not touch a pod or a row.
func TestReapOne_ReportsNothingToReap(t *testing.T) {
	h := newHarness(nil, &recordingTerminator{})

	found, err := h.reaper.ReapOne(context.Background())

	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, h.pods.calls())
	assert.Empty(t, h.repo.saved)
}

// TestReapOne_StopsThePodOfAWorkerThatNeverStarted pins that a lease that
// expired before its worker reported a start is reaped like any other: the pod
// claimed the task and may be running dbt without having said so, so it is
// stopped and the attempt is recorded with no start time.
func TestReapOne_StopsThePodOfAWorkerThatNeverStarted(t *testing.T) {
	dep, _ := leasedTask(t, 0, 3)
	h := newHarness(dep, &recordingTerminator{})

	found, err := h.reaper.ReapOne(context.Background())
	require.NoError(t, err)
	assert.True(t, found)

	assert.Equal(t, []deletedPod{{name: "dbt-worker-abc-1", uid: "uid-1"}}, h.pods.calls())
	assert.Equal(t, model.StatusRetryPending, dep.Status())

	var rec pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(h.outbox.entry(t, "task_execution_recorded").Payload, &rec))
	assert.Empty(t, rec.StartedAt, "a worker that never reported a start timed nothing")
}

// unleasedDeployments serves whatever row it is given, with no regard for
// whether a lease is on it.
type unleasedDeployments struct {
	repository.DeploymentRepository
	dep   *model.Deployment
	saved []*model.Deployment
}

func (r *unleasedDeployments) GetExpiredLeaseForUpdate(_ context.Context, _ time.Time) (*model.Deployment, error) {
	return r.dep, nil
}

func (r *unleasedDeployments) Save(_ context.Context, d *model.Deployment) error {
	r.saved = append(r.saved, d)
	return nil
}

// TestReapOne_ReportsARowServedWithNoLease pins that a row the lookup should
// never serve — one whose status says a worker holds it while its lease columns
// say otherwise — is reported, not dereferenced. The reaper polls from its own
// goroutine, so reading a lease that is not there would panic and take the whole
// executor down, stopping every dispatch in the service over one inconsistent
// row.
func TestReapOne_ReportsARowServedWithNoLease(t *testing.T) {
	dep, leaseID := leasedTask(t, 0, 3)
	require.NoError(t, dep.ExpireLease(leaseID))
	repo := &unleasedDeployments{dep: dep}
	u := &uow.FakeUnitOfWork{Deployments: repo, Outbox: &stubOutbox{}}
	r := reaper.NewReaper(reaper.Config{
		UnitOfWork:   func() uow.UnitOfWork { return u },
		Pods:         &recordingTerminator{},
		Clock:        fixedClock{t: reapAt},
		RetryBackoff: backoff,
		Logger:       testLogger(),
	})

	_, err := r.ReapOne(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no lease")
	assert.Empty(t, repo.saved)
}
