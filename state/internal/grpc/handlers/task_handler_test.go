package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTaskStatusConversion_SkippedRoundTrips guards the regression where a
// cascade-skipped node was reported to clients as UNSPECIFIED: the proto
// TaskStatus enum lacked a SKIPPED member, so domainToProtoTaskStatus fell
// through to the default. Every domain status must survive the round trip.
func TestTaskStatusConversion_EveryStatusRoundTrips(t *testing.T) {
	cases := []struct {
		domain run.TaskStatus
		proto  statev1.TaskStatus
	}{
		{run.TaskStatusPending, statev1.TaskStatus_TASK_STATUS_PENDING},
		{run.TaskStatusRunning, statev1.TaskStatus_TASK_STATUS_RUNNING},
		{run.TaskStatusSucceeded, statev1.TaskStatus_TASK_STATUS_SUCCEEDED},
		{run.TaskStatusFailed, statev1.TaskStatus_TASK_STATUS_FAILED},
		{run.TaskStatusCancelled, statev1.TaskStatus_TASK_STATUS_CANCELLED},
		{run.TaskStatusSkipped, statev1.TaskStatus_TASK_STATUS_SKIPPED},
		{run.TaskStatusDispatched, statev1.TaskStatus_TASK_STATUS_DISPATCHED},
		{run.TaskStatusQueued, statev1.TaskStatus_TASK_STATUS_QUEUED},
		{run.TaskStatusStopping, statev1.TaskStatus_TASK_STATUS_STOPPING},
	}
	for _, c := range cases {
		assert.Equal(t, c.proto, domainToProtoTaskStatus(c.domain),
			"domain %q must map to its proto enum, not UNSPECIFIED", c.domain)
		assert.Equal(t, c.domain, protoToDomainTaskStatus(c.proto),
			"proto %v must map back to its domain status", c.proto)
	}
}

// Every domain task status maps to its own proto status and back; none falls
// to UNSPECIFIED, so a status added to the domain without a proto value fails here.
func TestTaskStatusConversion_NoDomainStatusIsUnspecified(t *testing.T) {
	for _, s := range run.TaskStatuses() {
		p := domainToProtoTaskStatus(s)
		assert.NotEqual(t, statev1.TaskStatus_TASK_STATUS_UNSPECIFIED, p, "task status %q", s)
		assert.Equal(t, s, protoToDomainTaskStatus(p), "task status %q must round-trip", s)
	}
}

// fakeTaskRepo serves fixed tasks to TaskHandler's reads.
type fakeTaskRepo struct {
	postgres.TaskTrackerRepository
	tasks []*postgres.TaskTracker
}

func (f *fakeTaskRepo) GetByID(_ context.Context, id uuid.UUID) (*postgres.TaskTracker, error) {
	for _, t := range f.tasks {
		if t.TaskID == id {
			return t, nil
		}
	}
	return nil, postgres.ErrNotFound
}

func (f *fakeTaskRepo) ListByScheduleID(_ context.Context, _ uuid.UUID, _ *run.TaskStatus, _, _ int) ([]*postgres.TaskTracker, int, error) {
	return f.tasks, len(f.tasks), nil
}

// stampedTask is a task of status s that a cancel stamped with its instant and author.
func stampedTask(s run.TaskStatus) *postgres.TaskTracker {
	at := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	by := "alice"
	return &postgres.TaskTracker{
		TaskID: uuid.New(), ScheduleID: uuid.New(), CreatedAt: at.Add(-time.Hour),
		Status: s, CancelledAt: &at, CancelledBy: &by,
	}
}

// ListTasks and GetTask report a stopping task as STOPPING.
func TestTaskHandler_AStoppingTaskIsStopping(t *testing.T) {
	stopping := stampedTask(run.TaskStatusStopping)
	h := NewTaskHandler(&fakeTaskRepo{tasks: []*postgres.TaskTracker{stopping}}, newTestLogger())

	list, err := h.ListTasks(context.Background(), &statev1.ListTasksRequest{ScheduleId: stopping.ScheduleID.String()})
	require.NoError(t, err)
	require.Len(t, list.Tasks, 1)
	assert.Equal(t, statev1.TaskStatus_TASK_STATUS_STOPPING, list.Tasks[0].Status)

	got, err := h.GetTask(context.Background(), &statev1.GetTaskRequest{TaskId: stopping.TaskID.String()})
	require.NoError(t, err)
	assert.Equal(t, statev1.TaskStatus_TASK_STATUS_STOPPING, got.Task.Status)
}

// A cancelled task shows who cancelled it and when. A task a stop was requested
// for keeps those stamps in storage after it ends succeeded or failed, but it was
// not cancelled, so no read presents them.
func TestDomainToProtoTask_CancelStampsOnlyOnACancelledTask(t *testing.T) {
	for _, s := range run.TaskStatuses() {
		got := domainToProtoTask(stampedTask(s))
		if s == run.TaskStatusCancelled {
			assert.Equal(t, "alice", got.CancelledBy, "status %q", s)
			assert.NotNil(t, got.CancelledAt, "status %q", s)
			continue
		}
		assert.Empty(t, got.CancelledBy, "status %q must not show cancelled_by", s)
		assert.Nil(t, got.CancelledAt, "status %q must not show cancelled_at", s)
	}
}

// The same rule holds through ListTasks and GetTask for a task that ended
// succeeded after its stop was requested.
func TestTaskHandler_ASucceededTaskThatWasAskedToStopShowsNoCancel(t *testing.T) {
	done := stampedTask(run.TaskStatusSucceeded)
	h := NewTaskHandler(&fakeTaskRepo{tasks: []*postgres.TaskTracker{done}}, newTestLogger())

	list, err := h.ListTasks(context.Background(), &statev1.ListTasksRequest{ScheduleId: done.ScheduleID.String()})
	require.NoError(t, err)
	assert.Empty(t, list.Tasks[0].CancelledBy)
	assert.Nil(t, list.Tasks[0].CancelledAt)

	got, err := h.GetTask(context.Background(), &statev1.GetTaskRequest{TaskId: done.TaskID.String()})
	require.NoError(t, err)
	assert.Empty(t, got.Task.CancelledBy)
	assert.Nil(t, got.Task.CancelledAt)
}
