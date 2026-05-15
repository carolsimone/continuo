package redis

import (
	"testing"

	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTaskStatusUpdated_HappyPath(t *testing.T) {
	taskID, scheduleID := uuid.New(), uuid.New()
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     taskID.String(),
		"schedule_id": scheduleID.String(),
		"status":      "Succeeded",
		"retry_count": "2",
	}}
	evt, err := ParseTaskStatusUpdated(msg)
	require.NoError(t, err)
	assert.Equal(t, taskID, evt.TaskID)
	assert.Equal(t, scheduleID, evt.ScheduleID)
	assert.Equal(t, model.TaskStatusSucceeded, evt.Status)
	assert.Equal(t, int32(2), evt.RetryCount)
}

func TestParseTaskStatusUpdated_MissingRequired(t *testing.T) {
	_, err := ParseTaskStatusUpdated(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"task_id": uuid.New().String()}})
	require.Error(t, err)
}

func TestParseTaskStatusUpdated_BadTaskID(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id": "x", "schedule_id": uuid.New().String(), "status": "running",
	}}
	_, err := ParseTaskStatusUpdated(msg)
	require.Error(t, err)
}

func TestParseTaskStatusUpdated_RetryCountOmittedDefaultsZero(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id": uuid.New().String(), "schedule_id": uuid.New().String(), "status": "running",
	}}
	evt, err := ParseTaskStatusUpdated(msg)
	require.NoError(t, err)
	assert.Equal(t, int32(0), evt.RetryCount)
}
