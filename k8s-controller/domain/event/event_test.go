package event

import "testing"

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
// an empty Operation must not corrupt the wire map (the executor parser
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
