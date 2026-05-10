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
