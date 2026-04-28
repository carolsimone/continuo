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
