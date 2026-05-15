package redis

import (
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTaskExecutionRecorded_HappyPath(t *testing.T) {
	execID, taskID := uuid.New(), uuid.New()
	startedAt := time.Now().UTC().Truncate(time.Second)
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"execution_id":      execID.String(),
		"task_id":           taskID.String(),
		"job_name":          "j1",
		"started_at":        startedAt.Format(time.RFC3339),
		"completed_at":      startedAt.Add(time.Minute).Format(time.RFC3339),
		"execution_seconds": "60.5",
		"error_message":     "",
		"log_s3_key":        "k",
	}}
	evt, err := ParseTaskExecutionRecorded(msg)
	require.NoError(t, err)
	assert.Equal(t, execID, evt.ExecutionID)
	assert.Equal(t, taskID, evt.TaskID)
	require.NotNil(t, evt.JobName)
	assert.Equal(t, "j1", *evt.JobName)
	require.NotNil(t, evt.StartedAt)
	require.NotNil(t, evt.ExecutionTimeSeconds)
	assert.InDelta(t, 60.5, *evt.ExecutionTimeSeconds, 0.0001)
	assert.Nil(t, evt.ErrorMessage)
	require.NotNil(t, evt.LogS3Key)
	assert.Equal(t, "k", *evt.LogS3Key)
}

func TestParseTaskExecutionRecorded_MissingRequired(t *testing.T) {
	_, err := ParseTaskExecutionRecorded(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"task_id": uuid.New().String()}})
	require.Error(t, err)
}

func TestParseTaskExecutionRecorded_BadStartedAt(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"execution_id": uuid.New().String(),
		"task_id":      uuid.New().String(),
		"started_at":   "not-a-time",
	}}
	_, err := ParseTaskExecutionRecorded(msg)
	require.Error(t, err)
}
