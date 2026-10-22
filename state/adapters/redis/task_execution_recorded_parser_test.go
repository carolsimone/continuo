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
		"execution_id":       execID.String(),
		"task_id":            taskID.String(),
		"job_name":           "j1",
		"started_at":         startedAt.Format(time.RFC3339),
		"completed_at":       startedAt.Add(time.Minute).Format(time.RFC3339),
		"execution_seconds":  "60.5",
		"error_message":      "",
		"log_s3_key":         "k",
		"parse_cache":        "degraded",
		"parse_cache_reason": "fetch failed",
	}}
	evt, err := ParseTaskExecutionRecorded(msg)
	require.NoError(t, err)
	assert.Equal(t, execID, evt.ExecutionID)
	assert.Equal(t, taskID, evt.TaskID)
	assert.Equal(t, "j1", evt.JobName)
	require.NotNil(t, evt.StartedAt)
	require.NotNil(t, evt.ExecutionTimeSeconds)
	assert.InDelta(t, 60.5, *evt.ExecutionTimeSeconds, 0.0001)
	assert.Nil(t, evt.ErrorMessage)
	require.NotNil(t, evt.LogS3Key)
	assert.Equal(t, "k", *evt.LogS3Key)
	require.NotNil(t, evt.ParseCache)
	assert.Equal(t, "degraded", *evt.ParseCache)
	require.NotNil(t, evt.ParseCacheReason)
	assert.Equal(t, "fetch failed", *evt.ParseCacheReason)
}

func TestParseTaskExecutionRecorded_ParseCacheAbsent(t *testing.T) {
	execID, taskID := uuid.New(), uuid.New()
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"execution_id": execID.String(),
		"task_id":      taskID.String(),
		"job_name":     "j1",
	}}
	evt, err := ParseTaskExecutionRecorded(msg)
	require.NoError(t, err)
	assert.Nil(t, evt.ParseCache)
	assert.Nil(t, evt.ParseCacheReason)
}

// A record without an execution id, a task id or a Job name is rejected, and
// the error names the missing field. The Job name is the only key to the
// attempt the record describes.
func TestParseTaskExecutionRecorded_MissingRequired(t *testing.T) {
	full := func() map[string]interface{} {
		return map[string]interface{}{
			"execution_id": uuid.New().String(),
			"task_id":      uuid.New().String(),
			"job_name":     "j1",
		}
	}
	for _, field := range []string{"execution_id", "task_id", "job_name"} {
		t.Run(field, func(t *testing.T) {
			values := full()
			delete(values, field)
			_, err := ParseTaskExecutionRecorded(goredis.XMessage{ID: "1-0", Values: values})
			require.Error(t, err)
			assert.Contains(t, err.Error(), field)
		})
	}
	t.Run("empty job_name", func(t *testing.T) {
		values := full()
		values["job_name"] = ""
		_, err := ParseTaskExecutionRecorded(goredis.XMessage{ID: "1-0", Values: values})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "job_name")
	})
}

func TestParseTaskExecutionRecorded_BadStartedAt(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"execution_id": uuid.New().String(),
		"task_id":      uuid.New().String(),
		"job_name":     "j1",
		"started_at":   "not-a-time",
	}}
	_, err := ParseTaskExecutionRecorded(msg)
	require.Error(t, err)
}

// TestParseTaskExecutionRecorded_RunResultsURI verifies the structured-result
// key is read off the wire, and stays nil when absent — which is every dbt
// execution, since dbt containers print no result block.
func TestParseTaskExecutionRecorded_RunResultsURI(t *testing.T) {
	const key = "run-results/task-executions/svc/sc/tbl/e.json"

	present := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"execution_id":    uuid.New().String(),
		"task_id":         uuid.New().String(),
		"job_name":        "j1",
		"run_results_uri": key,
	}}
	evt, err := ParseTaskExecutionRecorded(present)
	require.NoError(t, err)
	require.NotNil(t, evt.RunResultsURI)
	assert.Equal(t, key, *evt.RunResultsURI)

	absent := goredis.XMessage{ID: "1-1", Values: map[string]interface{}{
		"execution_id": uuid.New().String(),
		"task_id":      uuid.New().String(),
		"job_name":     "j1",
	}}
	evt, err = ParseTaskExecutionRecorded(absent)
	require.NoError(t, err)
	assert.Nil(t, evt.RunResultsURI, "no wire key means no value, not an empty string")
}
