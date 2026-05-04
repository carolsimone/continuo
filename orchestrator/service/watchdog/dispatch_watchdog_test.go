package watchdog_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/service/watchdog"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

type fakeStateClient struct {
	ListAllSchedulesFn func(ctx context.Context, req *statev1.ListAllSchedulesRequest, opts ...grpc.CallOption) (*statev1.ListAllSchedulesResponse, error)
	ListTasksFn        func(ctx context.Context, req *statev1.ListTasksRequest, opts ...grpc.CallOption) (*statev1.ListTasksResponse, error)
	CancelCalls        []*statev1.CancelScheduleRequest
	CancelErr          error
}

func (f *fakeStateClient) ListAllSchedules(ctx context.Context, req *statev1.ListAllSchedulesRequest, opts ...grpc.CallOption) (*statev1.ListAllSchedulesResponse, error) {
	return f.ListAllSchedulesFn(ctx, req, opts...)
}

func (f *fakeStateClient) ListTasks(ctx context.Context, req *statev1.ListTasksRequest, opts ...grpc.CallOption) (*statev1.ListTasksResponse, error) {
	return f.ListTasksFn(ctx, req, opts...)
}

func (f *fakeStateClient) CancelSchedule(ctx context.Context, req *statev1.CancelScheduleRequest, opts ...grpc.CallOption) (*statev1.CancelScheduleResponse, error) {
	f.CancelCalls = append(f.CancelCalls, req)
	return &statev1.CancelScheduleResponse{}, f.CancelErr
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func newWatchdogForTest(state watchdog.StateClient, now time.Time, noProgressFor time.Duration) *watchdog.Watchdog {
	w := watchdog.NewWatchdog(
		watchdog.Config{Enabled: true, Interval: time.Second, NoProgressFor: noProgressFor},
		state,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	watchdog.SetClockForTest(w, fixedClock{t: now})
	return w
}

// ---------------------------------------------------------------------------
// Loop tests
// ---------------------------------------------------------------------------

func TestWatchdog_StuckSchedule_CallsCancelSchedule(t *testing.T) {
	now := time.Now()
	state := &fakeStateClient{
		ListAllSchedulesFn: func(_ context.Context, _ *statev1.ListAllSchedulesRequest, _ ...grpc.CallOption) (*statev1.ListAllSchedulesResponse, error) {
			return &statev1.ListAllSchedulesResponse{
				Schedules: []*statev1.ScheduleSummary{
					{ScheduleName: "stuck-schedule", IsRunning: true, LastRunId: "run-uuid-1"},
				},
			}, nil
		},
		ListTasksFn: func(_ context.Context, _ *statev1.ListTasksRequest, _ ...grpc.CallOption) (*statev1.ListTasksResponse, error) {
			return &statev1.ListTasksResponse{
				Tasks: []*statev1.Task{
					{Status: statev1.TaskStatus_TASK_STATUS_PENDING, CreatedAt: timestamppb.New(now.Add(-45 * time.Minute))},
				},
			}, nil
		},
	}
	w := newWatchdogForTest(state, now, 30*time.Minute)
	require.NoError(t, watchdog.TickForTest(w, context.Background()))
	require.Len(t, state.CancelCalls, 1)
	assert.Equal(t, "stuck-schedule", state.CancelCalls[0].ScheduleName)
	assert.Equal(t, "watchdog", state.CancelCalls[0].CancelledBy)
	assert.Contains(t, state.CancelCalls[0].CancellationReason, "watchdog:")
}

func TestWatchdog_NotRunning_Skipped(t *testing.T) {
	now := time.Now()
	state := &fakeStateClient{
		ListAllSchedulesFn: func(_ context.Context, _ *statev1.ListAllSchedulesRequest, _ ...grpc.CallOption) (*statev1.ListAllSchedulesResponse, error) {
			return &statev1.ListAllSchedulesResponse{
				Schedules: []*statev1.ScheduleSummary{
					{ScheduleName: "idle", IsRunning: false, LastRunId: "run-uuid-2"},
				},
			}, nil
		},
		ListTasksFn: func(_ context.Context, _ *statev1.ListTasksRequest, _ ...grpc.CallOption) (*statev1.ListTasksResponse, error) {
			t.Fatal("ListTasks must not be called for non-running schedules")
			return nil, nil
		},
	}
	w := newWatchdogForTest(state, now, 30*time.Minute)
	require.NoError(t, watchdog.TickForTest(w, context.Background()))
	assert.Empty(t, state.CancelCalls)
}

func TestWatchdog_RunningWithProgress_NotStuck_NoCancel(t *testing.T) {
	now := time.Now()
	state := &fakeStateClient{
		ListAllSchedulesFn: func(_ context.Context, _ *statev1.ListAllSchedulesRequest, _ ...grpc.CallOption) (*statev1.ListAllSchedulesResponse, error) {
			return &statev1.ListAllSchedulesResponse{
				Schedules: []*statev1.ScheduleSummary{
					{ScheduleName: "fresh", IsRunning: true, LastRunId: "run-uuid-3"},
				},
			}, nil
		},
		ListTasksFn: func(_ context.Context, _ *statev1.ListTasksRequest, _ ...grpc.CallOption) (*statev1.ListTasksResponse, error) {
			return &statev1.ListTasksResponse{
				Tasks: []*statev1.Task{
					{Status: statev1.TaskStatus_TASK_STATUS_RUNNING, CreatedAt: timestamppb.New(now.Add(-2 * time.Hour))},
				},
			}, nil
		},
	}
	w := newWatchdogForTest(state, now, 30*time.Minute)
	require.NoError(t, watchdog.TickForTest(w, context.Background()))
	assert.Empty(t, state.CancelCalls, "RUNNING task means not stuck")
}

func TestWatchdog_DisabledConfig_RunReturnsImmediately(t *testing.T) {
	state := &fakeStateClient{}
	w := watchdog.NewWatchdog(
		watchdog.Config{Enabled: false},
		state,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := w.Run(ctx)
	assert.NoError(t, err)
}

func makeTask(status statev1.TaskStatus, createdAt time.Time) *statev1.Task {
	return &statev1.Task{
		Status:    status,
		CreatedAt: timestamppb.New(createdAt),
	}
}

func TestIsScheduleStuck_StaleAndNoRunning_ReturnsTrue(t *testing.T) {
	now := time.Now()
	tasks := []*statev1.Task{
		makeTask(statev1.TaskStatus_TASK_STATUS_PENDING, now.Add(-45*time.Minute)),
		makeTask(statev1.TaskStatus_TASK_STATUS_FAILED, now.Add(-45*time.Minute)),
	}
	assert.True(t, watchdog.IsScheduleStuck(tasks, now, 30*time.Minute),
		"non-RUNNING tasks all >30m old must classify as stuck")
}

func TestIsScheduleStuck_StaleButHasRunning_ReturnsFalse(t *testing.T) {
	now := time.Now()
	tasks := []*statev1.Task{
		makeTask(statev1.TaskStatus_TASK_STATUS_RUNNING, now.Add(-45*time.Minute)),
		makeTask(statev1.TaskStatus_TASK_STATUS_PENDING, now.Add(-45*time.Minute)),
	}
	assert.False(t, watchdog.IsScheduleStuck(tasks, now, 30*time.Minute),
		"RUNNING task means a long-running model, not stuck dispatch")
}

func TestIsScheduleStuck_FreshTransition_ReturnsFalse(t *testing.T) {
	now := time.Now()
	tasks := []*statev1.Task{
		makeTask(statev1.TaskStatus_TASK_STATUS_PENDING, now.Add(-1*time.Minute)),
	}
	assert.False(t, watchdog.IsScheduleStuck(tasks, now, 30*time.Minute),
		"a task created 1m ago is not stuck")
}

func TestIsScheduleStuck_NoTasks_ReturnsFalse(t *testing.T) {
	assert.False(t, watchdog.IsScheduleStuck(nil, time.Now(), 30*time.Minute),
		"no tasks means schedule hasn't started — not stuck")
	assert.False(t, watchdog.IsScheduleStuck([]*statev1.Task{}, time.Now(), 30*time.Minute),
		"empty slice equivalent to nil")
}

func TestIsScheduleStuck_ExactlyAtThreshold_ReturnsTrue(t *testing.T) {
	now := time.Now()
	// Exactly 30m old -> now.Sub == 30m -> >= 30m -> stuck
	tasks := []*statev1.Task{
		makeTask(statev1.TaskStatus_TASK_STATUS_PENDING, now.Add(-30*time.Minute)),
	}
	assert.True(t, watchdog.IsScheduleStuck(tasks, now, 30*time.Minute),
		"boundary: created exactly noProgressFor ago classifies as stuck")
}

func TestIsScheduleStuck_MixedAges_UsesMostRecent(t *testing.T) {
	now := time.Now()
	// Older task is past threshold but newest task is fresh -> not stuck
	tasks := []*statev1.Task{
		makeTask(statev1.TaskStatus_TASK_STATUS_FAILED, now.Add(-2*time.Hour)),
		makeTask(statev1.TaskStatus_TASK_STATUS_PENDING, now.Add(-2*time.Minute)),
	}
	assert.False(t, watchdog.IsScheduleStuck(tasks, now, 30*time.Minute),
		"the most recent task's created_at is the progress signal")
}
