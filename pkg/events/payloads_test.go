package events_test

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskStatusUpdated_RoundTrip(t *testing.T) {
	in := events.TaskStatusUpdated{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		Status:     "SUCCEEDED",
		RetryCount: 2,
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out events.TaskStatusUpdated
	require.NoError(t, json.Unmarshal(b, &out))
	assert.Equal(t, in, out)
}

func TestCheckK8s_RoundTrip(t *testing.T) {
	in := events.CheckK8s{
		TaskID:       uuid.New().String(),
		ScheduleID:   uuid.New().String(),
		ScheduleName: "daily",
		ServiceName:  "svc",
		SchemaName:   "public",
		TableName:    "orders",
		JobName:      "job-1",
		NodeType:     "dbt-model",
		ImageTag:     "sha-abc",
		Operation:    "test",
		RetryCount:   3,
		MaxRetries:   5,
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	// check.k8s:v1 carries the task-level retry count under retry_count.
	assert.Contains(t, string(b), `"retry_count":3`)
	// Operation recirculates on the self-poll loop so it survives to retry.
	assert.Contains(t, string(b), `"operation":"test"`)
	var out events.CheckK8s
	require.NoError(t, json.Unmarshal(b, &out))
	assert.Equal(t, in, out)
}

func TestTaskExecutionRecordedToMap(t *testing.T) {
	in := events.TaskExecutionRecorded{
		ExecutionID:      "exec-1",
		TaskID:           "task-1",
		JobName:          "job-1",
		StartedAt:        "2026-05-19T10:00:00Z",
		CompletedAt:      "2026-05-19T10:00:05Z",
		ExecutionSeconds: 5.0,
		ErrorMessage:     "",
		LogS3Key:         "logs/exec-1",
	}
	m := in.ToMap()
	require.Equal(t, "exec-1", m["execution_id"])
	require.Equal(t, "task-1", m["task_id"])
	require.Equal(t, "job-1", m["job_name"])
	require.Equal(t, 5.0, m["execution_seconds"])
	require.Equal(t, "2026-05-19T10:00:00Z", m["started_at"])
	require.Equal(t, "2026-05-19T10:00:05Z", m["completed_at"])
	require.NotContains(t, m, "error_message") // omitempty
	require.Equal(t, "logs/exec-1", m["log_s3_key"])
}

func TestTaskExecutionRecordedToMap_OmitsEmptyOptionalFields(t *testing.T) {
	in := events.TaskExecutionRecorded{
		ExecutionID:      "exec-2",
		TaskID:           "task-2",
		JobName:          "job-2",
		ExecutionSeconds: 1.5,
	}
	m := in.ToMap()
	require.NotContains(t, m, "started_at")
	require.NotContains(t, m, "completed_at")
	require.NotContains(t, m, "error_message")
	require.NotContains(t, m, "log_s3_key")
	require.Contains(t, m, "execution_id")
	require.Contains(t, m, "task_id")
	require.Contains(t, m, "job_name")
	require.Contains(t, m, "execution_seconds")
}

// TestTaskExecutionRecordedToMap_ParseCache verifies that ParseCache and
// ParseCacheReason are emitted under their wire keys (parse_cache /
// parse_cache_reason) when set, and omitted (both, independently) when zero-valued —
// state's execution-record parser depends on these exact keys.
func TestTaskExecutionRecordedToMap_ParseCache(t *testing.T) {
	in := events.TaskExecutionRecorded{
		ExecutionID:      "exec-3",
		TaskID:           "task-3",
		JobName:          "job-3",
		ExecutionSeconds: 2.0,
		ParseCache:       "degraded",
		ParseCacheReason: "x",
	}
	m := in.ToMap()
	require.Equal(t, "degraded", m["parse_cache"])
	require.Equal(t, "x", m["parse_cache_reason"])
}

func TestTaskExecutionRecordedToMap_ParseCache_OmitsZeroValues(t *testing.T) {
	in := events.TaskExecutionRecorded{
		ExecutionID:      "exec-4",
		TaskID:           "task-4",
		JobName:          "job-4",
		ExecutionSeconds: 2.0,
	}
	m := in.ToMap()
	require.NotContains(t, m, "parse_cache")
	require.NotContains(t, m, "parse_cache_reason")
}

// TestCheckK8s_OmitsEmptyOperation verifies the check.k8s:v1 self-poll payload
// omits operation for a normal `dbt run`.
func TestCheckK8s_OmitsEmptyOperation(t *testing.T) {
	b, err := json.Marshal(events.CheckK8s{TaskID: uuid.New().String()})
	require.NoError(t, err)
	assert.NotContains(t, string(b), "operation")
}

// TestTaskExecutionRecorded_OmitsEmptyRunResultsURI pins the wire-compatibility
// guarantee: an execution whose container printed no structured result block —
// every dbt job — serializes exactly as it did before the field existed, on
// both the JSON and the Redis flat-map representations.
func TestTaskExecutionRecorded_OmitsEmptyRunResultsURI(t *testing.T) {
	in := events.TaskExecutionRecorded{
		ExecutionID:      uuid.New().String(),
		TaskID:           uuid.New().String(),
		JobName:          "job-1",
		ExecutionSeconds: 1.5,
	}

	b, err := json.Marshal(in)
	require.NoError(t, err)
	assert.NotContains(t, string(b), "run_results_uri")
	assert.NotContains(t, in.ToMap(), "run_results_uri")
}

// TestTaskExecutionRecorded_CarriesRunResultsURI verifies the structured
// result's S3 key reaches both representations under the same name the
// validation events use, so one consumer vocabulary spans validation and
// production executions.
func TestTaskExecutionRecorded_CarriesRunResultsURI(t *testing.T) {
	const key = "run-results/task-executions/svc/sc/tbl/exec-1.json"
	in := events.TaskExecutionRecorded{
		ExecutionID:   uuid.New().String(),
		TaskID:        uuid.New().String(),
		RunResultsURI: key,
	}

	b, err := json.Marshal(in)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"run_results_uri":"`+key+`"`)
	assert.Equal(t, key, in.ToMap()["run_results_uri"])
}
