package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeUpdated_ToMap(t *testing.T) {
	e := NodeUpdated{
		TaskID:       "t1",
		ScheduleID:   "s1",
		ScheduleName: "daily",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "orders",
		Status:       "FAILED",
	}
	m := e.ToMap()
	assert.Equal(t, "t1", m["task_id"])
	assert.Equal(t, "s1", m["schedule_id"])
	assert.Equal(t, "daily", m["schedule_name"])
	assert.Equal(t, "dbt", m["service_name"])
	assert.Equal(t, "public", m["schema_name"])
	assert.Equal(t, "orders", m["table_name"])
	assert.Equal(t, "FAILED", m["status"])
}

func TestEventTypesAreDistinctAndComplete(t *testing.T) {
	all := []string{
		EventTypeTaskStatusUpdated, EventTypeTaskExecutionRecorded, EventTypeNodeDeployed,
		EventTypeNodeUpdated, EventTypeTaskRetry, EventTypeTaskFailed, EventTypeCheckDelayed,
		EventTypeValidationNodeCompleted, EventTypeSeedBuildNodeCompleted, EventTypeCompileNodeCompleted,
	}
	seen := map[string]bool{}
	for _, v := range all {
		if v == "" || seen[v] {
			t.Fatalf("event type %q is empty or duplicated", v)
		}
		seen[v] = true
	}
	if EventTypeNodeUpdated != "node_updated" {
		t.Fatalf("node.updated:v1 rows must carry event_type node_updated, got %q", EventTypeNodeUpdated)
	}
}
