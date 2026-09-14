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

// TestTaskRetry_ToMap_OperationPresent guards the false-green retry bug: when
// the failed Job carried a non-run operation (e.g. "test"), the retry.task:v1
// wire map must carry it so the rebuilt Job stays `dbt test`, not `dbt run`.
func TestTaskRetry_ToMap_OperationPresent(t *testing.T) {
	e := TaskRetry{
		TaskID:     "t1",
		ScheduleID: "s1",
		JobName:    "job-1",
		RetryCount: 2,
		MaxRetries: 3,
		NodeType:   "dbt-model",
		Operation:  "test",
	}
	m := e.ToMap()
	if got, _ := m["operation"].(string); got != "test" {
		t.Errorf("ToMap()[\"operation\"]: expected %q, got %v", "test", m["operation"])
	}
}

// TestTaskRetry_ToMap_OperationEmpty guards the normal `dbt run` retry path:
// an empty Operation must not corrupt the wire map (the retry-task parser
// defaults an absent/empty operation field to run).
func TestTaskRetry_ToMap_OperationEmpty(t *testing.T) {
	e := TaskRetry{
		TaskID:     "t1",
		ScheduleID: "s1",
		JobName:    "job-1",
		RetryCount: 1,
		MaxRetries: 3,
		NodeType:   "dbt-model",
	}
	m := e.ToMap()
	if got, _ := m["operation"].(string); got != "" {
		t.Errorf("ToMap()[\"operation\"]: expected empty for run retries, got %q", got)
	}
}
