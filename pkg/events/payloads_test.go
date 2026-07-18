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

func TestRunEntriesDispatched_RoundTrip(t *testing.T) {
	in := events.RunEntriesDispatched{
		ScheduleID:     uuid.New().String(),
		TotalTaskCount: 3,
		AllTasks: []events.DispatchedTask{
			{TaskID: uuid.New().String(), ServiceName: "svc1", SchemaName: "public", TableName: "t1", NodeType: "model", MaxRetries: 3},
		},
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out events.RunEntriesDispatched
	require.NoError(t, json.Unmarshal(b, &out))
	assert.Equal(t, in, out)
}

func TestDispatchedTask_RoundTripWithManifestVersionAndImageTag(t *testing.T) {
	in := events.DispatchedTask{
		TaskID:          uuid.New().String(),
		ServiceName:     "svc-a",
		SchemaName:      "public",
		TableName:       "users",
		NodeType:        "dbt-model",
		MaxRetries:      3,
		ManifestVersion: "v7",
		ImageTag:        "abcd123-1714300000",
	}
	raw, err := json.Marshal(in)
	require.NoError(t, err)

	var out events.DispatchedTask
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.Equal(t, in, out)
	assert.Contains(t, string(raw), `"manifest_version":"v7"`)
	assert.Contains(t, string(raw), `"image_tag":"abcd123-1714300000"`)
}

func TestDispatchedTask_BackwardCompatDefaults(t *testing.T) {
	// Pre-PR2 producers omit Status and InheritedFromTaskID; consumers must
	// see "" / "" and treat as PENDING / no-inherit.
	raw := `{"task_id":"t1","service_name":"svc","schema_name":"s","table_name":"x","node_type":"dbt-model","max_retries":2,"manifest_version":"v1","image_tag":"img:1"}`
	var dt events.DispatchedTask
	err := json.Unmarshal([]byte(raw), &dt)
	require.NoError(t, err)
	assert.Equal(t, "", dt.Status, "expected empty Status")
	assert.Equal(t, "", dt.InheritedFromTaskID, "expected empty InheritedFromTaskID")
}

func TestDispatchedTask_RoundtripWithNewFields(t *testing.T) {
	dt := events.DispatchedTask{
		TaskID:              "t1",
		ServiceName:         "svc",
		SchemaName:          "s",
		TableName:           "x",
		NodeType:            "dbt-model",
		MaxRetries:          2,
		ManifestVersion:     "v1",
		ImageTag:            "img:1",
		Status:              "succeeded",
		InheritedFromTaskID: "00000000-0000-0000-0000-000000000001",
	}
	b, err := json.Marshal(dt)
	require.NoError(t, err)
	var got events.DispatchedTask
	err = json.Unmarshal(b, &got)
	require.NoError(t, err)
	assert.Equal(t, "succeeded", got.Status)
	assert.Equal(t, "00000000-0000-0000-0000-000000000001", got.InheritedFromTaskID)
}

func TestNodeDeployed_RoundTrip(t *testing.T) {
	in := events.NodeDeployed{
		TaskID:         uuid.New().String(),
		ScheduleID:     uuid.New().String(),
		ScheduleName:   "daily",
		ServiceName:    "svc",
		SchemaName:     "public",
		TableName:      "orders",
		JobName:        "job-1",
		NodeType:       "dbt-model",
		ImageTag:       "sha-abc",
		Operation:      "test",
		TaskRetryCount: 2,
		MaxRetries:     5,
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	// node.deployed:v1 carries the task-level retry count under task_retry_count.
	assert.Contains(t, string(b), `"task_retry_count":2`)
	// Operation rides the durable payload so a retry keeps the dbt verb.
	assert.Contains(t, string(b), `"operation":"test"`)
	var out events.NodeDeployed
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

// TestNodeDeployed_OmitsEmptyOperation verifies a normal `dbt run` (empty
// Operation) stays wire-identical: no operation key is emitted.
func TestNodeDeployed_OmitsEmptyOperation(t *testing.T) {
	b, err := json.Marshal(events.NodeDeployed{TaskID: uuid.New().String()})
	require.NoError(t, err)
	assert.NotContains(t, string(b), "operation")
}

// TestCheckK8s_OmitsEmptyOperation verifies the check.k8s:v1 self-poll payload
// omits operation for a normal `dbt run`.
func TestCheckK8s_OmitsEmptyOperation(t *testing.T) {
	b, err := json.Marshal(events.CheckK8s{TaskID: uuid.New().String()})
	require.NoError(t, err)
	assert.NotContains(t, string(b), "operation")
}
