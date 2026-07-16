// executor-controller/service/handlers/schedule_cancelled_handler_test.go
package handlers_test

import (
	"context"
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
	h := handlers.NewScheduleCancelledHandler(cancelTestLogger())
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

	h := handlers.NewScheduleCancelledHandler(cancelTestLogger())
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

	h := handlers.NewScheduleCancelledHandler(cancelTestLogger())
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
	h := handlers.NewScheduleCancelledHandler(cancelTestLogger())
	evt := events.ScheduleCancelled{ScheduleID: scheduleID}

	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	assert.Len(t, repo.saved, 1, "the redelivery must not re-save an already cancelled row")
	assert.Equal(t, model.StatusCancelled, dep.Status())
}
