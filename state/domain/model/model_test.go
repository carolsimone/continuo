package model

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTestTask(s TaskStatus) *TaskTracker {
	return &TaskTracker{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		Status:     s,
		MaxRetries: 3,
	}
}

func TestTransition_AllowedPaths(t *testing.T) {
	cases := []struct {
		name   string
		from   TaskStatus
		to     TaskStatus
		caller CallerID
	}{
		{"startup resets failed", TaskStatusFailed, TaskStatusPending, CallerStartupController},
		{"executor starts pending", TaskStatusPending, TaskStatusRunning, CallerExecutorController},
		{"executor retries failed", TaskStatusFailed, TaskStatusRunning, CallerExecutorController},
		{"k8s succeeds running", TaskStatusRunning, TaskStatusSucceeded, CallerK8sController},
		{"k8s fails running", TaskStatusRunning, TaskStatusFailed, CallerK8sController},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := makeTestTask(tc.from)
			err := task.Transition(tc.caller, tc.to)
			require.NoError(t, err)
			assert.Equal(t, tc.to, task.Status)
		})
	}
}

func TestTransition_InvalidTransition(t *testing.T) {
	task := makeTestTask(TaskStatusPending)
	err := task.Transition(CallerK8sController, TaskStatusFailed)
	assert.ErrorIs(t, err, ErrInvalidTransition)
	assert.Equal(t, TaskStatusPending, task.Status, "status must not change on error")
}

func TestTransition_UnauthorizedTransition(t *testing.T) {
	task := makeTestTask(TaskStatusFailed)
	// failed → pending is startup-controller's, not executor-controller's
	err := task.Transition(CallerExecutorController, TaskStatusPending)
	assert.ErrorIs(t, err, ErrUnauthorizedTransition)
	assert.Equal(t, TaskStatusFailed, task.Status, "status must not change on error")
}

func TestTransition_UnknownCaller(t *testing.T) {
	task := makeTestTask(TaskStatusFailed)
	err := task.Transition("rogue-service", TaskStatusPending)
	assert.ErrorIs(t, err, ErrUnauthorizedTransition)
}

func makeTestScheduler(s SchedulerStatus) *SchedulerTracker {
	return &SchedulerTracker{
		ScheduleID: uuid.New(),
		Status:     s,
	}
}

func TestSchedulerTransition_AllowedPaths(t *testing.T) {
	cases := []struct {
		name string
		from SchedulerStatus
		to   SchedulerStatus
	}{
		{"pending to running", SchedulerStatusPending, SchedulerStatusRunning},
		{"running to succeeded", SchedulerStatusRunning, SchedulerStatusSucceeded},
		{"running to failed", SchedulerStatusRunning, SchedulerStatusFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := makeTestScheduler(tc.from)
			err := s.Transition(tc.to)
			require.NoError(t, err)
			assert.Equal(t, tc.to, s.Status)
		})
	}
}

func TestSchedulerTransition_InvalidTransition(t *testing.T) {
	cases := []struct {
		name string
		from SchedulerStatus
		to   SchedulerStatus
	}{
		{"pending to succeeded", SchedulerStatusPending, SchedulerStatusSucceeded},
		{"pending to failed", SchedulerStatusPending, SchedulerStatusFailed},
		{"succeeded to running", SchedulerStatusSucceeded, SchedulerStatusRunning},
		{"failed to running", SchedulerStatusFailed, SchedulerStatusRunning},
		{"running to pending", SchedulerStatusRunning, SchedulerStatusPending},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := makeTestScheduler(tc.from)
			err := s.Transition(tc.to)
			assert.ErrorIs(t, err, ErrInvalidTransition)
			assert.Equal(t, tc.from, s.Status, "status must not change on error")
		})
	}
}

func TestSchedulerTransition_StatusNotMutatedOnError(t *testing.T) {
	s := makeTestScheduler(SchedulerStatusRunning)
	_ = s.Transition(SchedulerStatusPending)
	assert.Equal(t, SchedulerStatusRunning, s.Status)
}

func TestSchedulerTracker_GetManifestVersions(t *testing.T) {
	tracker := &SchedulerTracker{
		ManifestVersionsRaw: json.RawMessage(`{"svc-a":"v3","svc-b":"v5"}`),
	}

	assert.Equal(t, map[string]string{"svc-a": "v3", "svc-b": "v5"}, tracker.GetManifestVersions())
}
