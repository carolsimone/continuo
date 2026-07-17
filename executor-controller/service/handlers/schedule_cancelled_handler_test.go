// executor-controller/service/handlers/schedule_cancelled_handler_test.go
package handlers_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingCancelledRepo struct {
	mu       sync.Mutex
	inserted []uuid.UUID
	calls    []string
}

func (r *recordingCancelledRepo) LockSchedule(_ context.Context, _ uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "lock")
	return nil
}

func (r *recordingCancelledRepo) Insert(_ context.Context, id uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "insert")
	r.inserted = append(r.inserted, id)
	return nil
}
func (r *recordingCancelledRepo) Exists(_ context.Context, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (r *recordingCancelledRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

func cancelTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// deletedPod is one pod deletion the handler asked the runtime for.
type deletedPod struct {
	name string
	uid  string
}

// recordingTerminator records the pod deletions asked of it, and can be made to
// fail so the handler's rollback can be observed.
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

// workerDeployment builds a pending production worker deployment: routed to a
// pool, waiting to be claimed, holding no slot and no lease.
func workerDeployment(scheduleID uuid.UUID, now time.Time) *model.Deployment {
	return model.NewWorkerDeployment(command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: scheduleID.String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: "dbt-model", ImageTag: "sha-abc",
	}, uuid.Nil, "pool-abc", now)
}

// leasedWorkerDeployment builds a production worker deployment a pod has claimed
// and is running — the state in which a cancellation takes the task away from a
// live dbt process.
func leasedWorkerDeployment(t *testing.T, scheduleID uuid.UUID, podName, podUID string, now time.Time) *model.Deployment {
	t.Helper()
	dep := workerDeployment(scheduleID, now)
	leaseID := uuid.New()
	require.NoError(t, dep.Claim(leaseID, "digest", "worker-1", podName, podUID,
		now, now.Add(time.Minute), []string{"dbt", "run"}, model.ExecutionPathNative))
	require.NotNil(t, dep.Reservation().ReservedAt)
	return dep
}

// deployedJobsDeployment builds a production Jobs deployment that has reserved
// its execution slot and had its Kubernetes Job created — the state an in-flight
// task sits in while Kubernetes runs it.
func deployedJobsDeployment(t *testing.T, scheduleID uuid.UUID, now time.Time) *model.Deployment {
	t.Helper()
	dep := model.NewDeployment(command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: scheduleID.String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: "dbt-model", ImageTag: "sha-abc",
	}, nil, now)
	require.NoError(t, dep.ReserveForDispatch(now))
	require.NoError(t, dep.MarkDeployed(now))
	require.NotNil(t, dep.Reservation().ReservedAt)
	require.Nil(t, dep.SlotReleasedAt())
	return dep
}

func TestScheduleCancelledHandler_InsertsCancelledRow(t *testing.T) {
	cancelled := &recordingCancelledRepo{}
	u := newFakeUoW(&stubDeploymentsRepo{}, cancelled)

	id := uuid.New()
	h := handlers.NewScheduleCancelledHandler(cancelTestLogger(), &recordingTerminator{})
	err := h.Handle(context.Background(), u, events.ScheduleCancelled{ScheduleID: id}, uuid.New())
	require.NoError(t, err)
	require.Len(t, cancelled.inserted, 1)
	assert.Equal(t, id, cancelled.inserted[0])
}

// TestScheduleCancelledHandler_LocksTheScheduleBeforeRecordingIt pins the order
// the handler settles a cancellation in. The lock is what a concurrent enqueue's
// guard contends on, so taking it after the insert — or not at all — would let an
// enqueue that has already passed its guard commit a deployment this handler's
// scan cannot see, stranding that deployment's execution slot.
func TestScheduleCancelledHandler_LocksTheScheduleBeforeRecordingIt(t *testing.T) {
	cancelled := &recordingCancelledRepo{}
	u := newFakeUoW(&stubDeploymentsRepo{}, cancelled)

	h := handlers.NewScheduleCancelledHandler(cancelTestLogger(), &recordingTerminator{})
	require.NoError(t, h.Handle(
		context.Background(), u, events.ScheduleCancelled{ScheduleID: uuid.New()}, uuid.New()))

	assert.Equal(t, []string{"lock", "insert"}, cancelled.calls)
}

// TestScheduleCancelledHandler_CancelsInFlightDeploymentsAndReleasesSlots pins
// the capacity leak a cancelled schedule would otherwise cause. A deployment
// holds its execution slot until a transition releases it, and k8s-controller
// absorbs a cancelled schedule's Job result without reporting the Job terminal —
// so if this handler leaves the row in 'deployed', its slot is never returned
// and the executor's budget shrinks permanently.
func TestScheduleCancelledHandler_CancelsInFlightDeploymentsAndReleasesSlots(t *testing.T) {
	scheduleID := uuid.New()
	now := time.Now()

	inFlight := []*model.Deployment{
		deployedJobsDeployment(t, scheduleID, now),
		deployedJobsDeployment(t, scheduleID, now),
	}
	repo := &stubDeploymentsRepo{
		bySchedule: map[uuid.UUID][]*model.Deployment{scheduleID: inFlight},
	}
	u := newFakeUoW(repo, &recordingCancelledRepo{})

	h := handlers.NewScheduleCancelledHandler(cancelTestLogger(), &recordingTerminator{})
	require.NoError(t, h.Handle(context.Background(), u,
		events.ScheduleCancelled{ScheduleID: scheduleID}, uuid.New()))

	require.Len(t, repo.saved, 2, "every in-flight deployment must be persisted cancelled")
	for _, dep := range inFlight {
		assert.Equal(t, model.StatusCancelled, dep.Status())
		assert.NotNil(t, dep.SlotReleasedAt(), "cancelling must return the execution slot")
	}
}

// TestScheduleCancelledHandler_RedeliveryCancelsNothing pins that a second
// delivery of the same cancellation touches no row: the schedule's deployments
// are already terminal, so the schedule-scoped lookup returns none and Cancel —
// which rejects a terminal source status — is never reached.
func TestScheduleCancelledHandler_RedeliveryCancelsNothing(t *testing.T) {
	scheduleID := uuid.New()
	now := time.Now()

	dep := deployedJobsDeployment(t, scheduleID, now)
	repo := &stubDeploymentsRepo{
		bySchedule: map[uuid.UUID][]*model.Deployment{scheduleID: {dep}},
	}
	u := newFakeUoW(repo, &recordingCancelledRepo{})
	h := handlers.NewScheduleCancelledHandler(cancelTestLogger(), &recordingTerminator{})
	evt := events.ScheduleCancelled{ScheduleID: scheduleID}

	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	assert.Len(t, repo.saved, 1, "the redelivery must not re-save an already cancelled row")
	assert.Equal(t, model.StatusCancelled, dep.Status())
}

// TestScheduleCancelledHandler_DeletesThePodOfEveryActiveLease pins the fence a
// cancellation owes a worker pool. Cancelling a leased task releases its
// execution slot, and the executor hands that slot to other work immediately —
// so a pod left running keeps its dbt process writing to the warehouse
// alongside whatever took its place. The pod is named by UID so a pod the pool
// has already replaced under the same name is not taken out instead.
func TestScheduleCancelledHandler_DeletesThePodOfEveryActiveLease(t *testing.T) {
	scheduleID := uuid.New()
	now := time.Now()

	leased := leasedWorkerDeployment(t, scheduleID, "dbt-worker-abc-1", "uid-1", now)
	running := leasedWorkerDeployment(t, scheduleID, "dbt-worker-abc-2", "uid-2", now)
	repo := &stubDeploymentsRepo{
		bySchedule: map[uuid.UUID][]*model.Deployment{scheduleID: {leased, running}},
	}
	term := &recordingTerminator{}
	u := newFakeUoW(repo, &recordingCancelledRepo{})

	h := handlers.NewScheduleCancelledHandler(cancelTestLogger(), term)
	require.NoError(t, h.Handle(context.Background(), u,
		events.ScheduleCancelled{ScheduleID: scheduleID}, uuid.New()))

	assert.ElementsMatch(t, []deletedPod{
		{name: "dbt-worker-abc-1", uid: "uid-1"},
		{name: "dbt-worker-abc-2", uid: "uid-2"},
	}, term.calls(), "every pod holding a lease of the cancelled schedule is deleted")
	for _, dep := range []*model.Deployment{leased, running} {
		assert.Equal(t, model.StatusCancelled, dep.Status())
		assert.NotNil(t, dep.SlotReleasedAt(), "cancelling must return the execution slot")
	}
	assert.Len(t, repo.saved, 2)
}

// TestScheduleCancelledHandler_DeletesNoPodForWorkForNoWorkerHolds pins that the
// fence is scoped to the leases that exist. A pending worker task has no pod
// running it, and a Kubernetes Job is not a worker pod at all — asking the pool
// runtime to delete anything for either would name a pod that does not exist.
func TestScheduleCancelledHandler_DeletesNoPodForWorkNoWorkerHolds(t *testing.T) {
	scheduleID := uuid.New()
	now := time.Now()

	pending := workerDeployment(scheduleID, now)
	job := deployedJobsDeployment(t, scheduleID, now)
	repo := &stubDeploymentsRepo{
		bySchedule: map[uuid.UUID][]*model.Deployment{scheduleID: {pending, job}},
	}
	term := &recordingTerminator{}
	u := newFakeUoW(repo, &recordingCancelledRepo{})

	h := handlers.NewScheduleCancelledHandler(cancelTestLogger(), term)
	require.NoError(t, h.Handle(context.Background(), u,
		events.ScheduleCancelled{ScheduleID: scheduleID}, uuid.New()))

	assert.Empty(t, term.calls(), "no lease, no pod to fence")
	assert.Equal(t, model.StatusCancelled, pending.Status())
	assert.Equal(t, model.StatusCancelled, job.Status())
	assert.Len(t, repo.saved, 2, "both rows still settle")
}

// TestScheduleCancelledHandler_FailsTheCancellationWhenAPodCannotBeDeleted pins
// that a pod the runtime would not delete stops the whole cancellation. The
// handler runs inside the transaction its binding commits, so returning the
// error rolls back the tombstone and every cancellation with it: the lease stays
// authoritative, its slot stays held, and the redelivered message tries the
// fence again. Committing the release of a slot whose pod is still running is
// the one outcome that must not happen.
func TestScheduleCancelledHandler_FailsTheCancellationWhenAPodCannotBeDeleted(t *testing.T) {
	scheduleID := uuid.New()
	now := time.Now()

	leased := leasedWorkerDeployment(t, scheduleID, "dbt-worker-abc-1", "uid-1", now)
	repo := &stubDeploymentsRepo{
		bySchedule: map[uuid.UUID][]*model.Deployment{scheduleID: {leased}},
	}
	term := &recordingTerminator{err: errors.New("kubernetes API unreachable")}
	u := newFakeUoW(repo, &recordingCancelledRepo{})

	h := handlers.NewScheduleCancelledHandler(cancelTestLogger(), term)
	err := h.Handle(context.Background(), u,
		events.ScheduleCancelled{ScheduleID: scheduleID}, uuid.New())

	require.Error(t, err, "the cancellation must not settle while its pod is still running")
	assert.Equal(t, model.StatusLeased, leased.Status(), "the lease stays authoritative")
	assert.Nil(t, leased.SlotReleasedAt(), "the slot is not returned under a live pod")
	assert.Empty(t, repo.saved, "nothing is written for a row whose pod survives")
}

// TestScheduleCancelledHandler_FencesThePodBeforeReleasingItsSlot pins the
// order. The slot is released by the Cancel transition and taken by other work
// as soon as the transaction commits, so the pod deletion has to be requested
// first: a delete asked for only after the row is saved would be one the
// transaction could commit without.
func TestScheduleCancelledHandler_FencesThePodBeforeReleasingItsSlot(t *testing.T) {
	scheduleID := uuid.New()
	now := time.Now()

	leased := leasedWorkerDeployment(t, scheduleID, "dbt-worker-abc-1", "uid-1", now)
	repo := &stubDeploymentsRepo{
		bySchedule: map[uuid.UUID][]*model.Deployment{scheduleID: {leased}},
	}
	statusAtDelete := model.Status("")
	term := &podStatusProbe{dep: leased, seen: &statusAtDelete}
	u := newFakeUoW(repo, &recordingCancelledRepo{})

	h := handlers.NewScheduleCancelledHandler(cancelTestLogger(), term)
	require.NoError(t, h.Handle(context.Background(), u,
		events.ScheduleCancelled{ScheduleID: scheduleID}, uuid.New()))

	assert.Equal(t, model.StatusLeased, statusAtDelete,
		"the pod is deleted while the task still holds its lease and its slot")
	assert.Equal(t, model.StatusCancelled, leased.Status())
}

// podStatusProbe records the status a deployment was in at the moment its pod
// was deleted.
type podStatusProbe struct {
	dep  *model.Deployment
	seen *model.Status
}

func (p *podStatusProbe) DeletePod(_ context.Context, _, _ string) error {
	*p.seen = p.dep.Status()
	return nil
}

// TestScheduleCancelledHandler_RedeliveryFencesNoPod pins that a second delivery
// of the same cancellation asks for no deletion. The schedule's rows are already
// terminal, so the lookup returns none and there is no lease left to fence — a
// pool that has since put a new pod on the same node must not be disturbed by a
// duplicate message.
func TestScheduleCancelledHandler_RedeliveryFencesNoPod(t *testing.T) {
	scheduleID := uuid.New()
	now := time.Now()

	leased := leasedWorkerDeployment(t, scheduleID, "dbt-worker-abc-1", "uid-1", now)
	repo := &stubDeploymentsRepo{
		bySchedule: map[uuid.UUID][]*model.Deployment{scheduleID: {leased}},
	}
	term := &recordingTerminator{}
	u := newFakeUoW(repo, &recordingCancelledRepo{})
	h := handlers.NewScheduleCancelledHandler(cancelTestLogger(), term)
	evt := events.ScheduleCancelled{ScheduleID: scheduleID}

	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	assert.Len(t, term.calls(), 1, "the redelivery fences nothing a second time")
	assert.Len(t, repo.saved, 1)
}

// strictTerminator rejects a deletion that names no pod, as the Kubernetes API
// does: a resource name is required, so a delete by an empty name is a request
// error rather than a no-op.
type strictTerminator struct {
	recordingTerminator
}

func (t *strictTerminator) DeletePod(ctx context.Context, podName, podUID string) error {
	if podName == "" || podUID == "" {
		return errors.New(`resource name may not be empty`)
	}
	return t.recordingTerminator.DeletePod(ctx, podName, podUID)
}

// TestScheduleCancelledHandler_CancelsALeaseThatNamedNoPod pins that a lease
// carrying no pod identity does not wedge the cancel consumer. The pod cannot be
// fenced by UID and a delete by an empty name is rejected by the API every time,
// so attempting one would fail the handler, roll the tombstone back, and have the
// redelivered message fail again — the schedule could never settle and the
// consumer would spin on it forever. The row goes terminal instead, which fences
// every report its holder sends.
func TestScheduleCancelledHandler_CancelsALeaseThatNamedNoPod(t *testing.T) {
	scheduleID := uuid.New()
	now := time.Now()

	leased := leasedWorkerDeployment(t, scheduleID, "", "", now)
	repo := &stubDeploymentsRepo{
		bySchedule: map[uuid.UUID][]*model.Deployment{scheduleID: {leased}},
	}
	term := &strictTerminator{}
	u := newFakeUoW(repo, &recordingCancelledRepo{})

	h := handlers.NewScheduleCancelledHandler(cancelTestLogger(), term)
	err := h.Handle(context.Background(), u,
		events.ScheduleCancelled{ScheduleID: scheduleID}, uuid.New())

	require.NoError(t, err, "a lease that named no pod must not wedge the cancellation")
	assert.Empty(t, term.calls(), "no pod identity, nothing to ask the runtime for")
	assert.Equal(t, model.StatusCancelled, leased.Status(), "the row still settles")
	assert.NotNil(t, leased.SlotReleasedAt(), "and returns its execution slot")
	assert.Len(t, repo.saved, 1)
}

// TestScheduleCancelledHandler_KeepsTheLeaseSoItsWorkerIsToldItWasCancelled pins
// that cancelling does not drop the lease. Deleting a pod is a request
// Kubernetes serves with a termination grace period, so a worker can outlive it
// by seconds and heartbeat once more; that heartbeat authorizes, finds the task
// cancelled, and is the notice that tells the worker to abandon its task rather
// than a fault it would retry against.
func TestScheduleCancelledHandler_KeepsTheLeaseSoItsWorkerIsToldItWasCancelled(t *testing.T) {
	scheduleID := uuid.New()
	now := time.Now()

	leased := leasedWorkerDeployment(t, scheduleID, "dbt-worker-abc-1", "uid-1", now)
	leaseID := leased.ActiveLease().ID
	repo := &stubDeploymentsRepo{
		bySchedule: map[uuid.UUID][]*model.Deployment{scheduleID: {leased}},
	}
	u := newFakeUoW(repo, &recordingCancelledRepo{})

	h := handlers.NewScheduleCancelledHandler(cancelTestLogger(), &recordingTerminator{})
	require.NoError(t, h.Handle(context.Background(), u,
		events.ScheduleCancelled{ScheduleID: scheduleID}, uuid.New()))

	require.NotNil(t, leased.ActiveLease(), "the cancelled task keeps the lease it was taken from")
	assert.True(t, leased.ActiveLease().Authorizes(leaseID, "digest"),
		"the surviving worker still authorizes, so its heartbeat is answered 'cancelled'")
}
