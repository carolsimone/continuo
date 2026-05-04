package watchdog_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/service/watchdog"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
