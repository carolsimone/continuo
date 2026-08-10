package events

// TaskStatusUpdated — stream: task.status.updated:v1
// Published by:
//   - executor-controller (RUNNING on deploy, FAILED on deploy failure)
//   - k8s-controller (SUCCEEDED/FAILED from observed job status)
//   - orchestrator (SKIPPED when a node is cascade-skipped after an upstream failure)
//
// Consumed by: state. ToMap is the single serializer for this stream — every
// producer emits through it so the wire shape stays identical across services.
type TaskStatusUpdated struct {
	TaskID     string `json:"task_id"`
	ScheduleID string `json:"schedule_id"`
	Status     string `json:"status"` // RUNNING | SUCCEEDED | FAILED | SKIPPED
	RetryCount int32  `json:"retry_count"`
}

// ToMap converts TaskStatusUpdated to a flat map for Redis stream publishing.
func (e TaskStatusUpdated) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"task_id":     e.TaskID,
		"schedule_id": e.ScheduleID,
		"status":      e.Status,
		"retry_count": e.RetryCount,
	}
}

// TaskExecutionRecorded — stream: task.execution.recorded:v1
// Published by: k8s-controller
// Consumed by: state
type TaskExecutionRecorded struct {
	ExecutionID      string  `json:"execution_id"`
	TaskID           string  `json:"task_id"`
	JobName          string  `json:"job_name"`
	StartedAt        string  `json:"started_at,omitempty"`
	CompletedAt      string  `json:"completed_at,omitempty"`
	ExecutionSeconds float64 `json:"execution_seconds"`
	ErrorMessage     string  `json:"error_message,omitempty"`
	LogS3Key         string  `json:"log_s3_key,omitempty"`
	// RunResultsS3Key is the S3 object key of the structured result block the
	// pod printed on stdout, when it printed one. Python-model containers
	// always emit one; dbt containers never do, so the field is absent from
	// their payloads.
	RunResultsS3Key string `json:"run_results_uri,omitempty"`
	// ParseCache reports whether the Job's team container ran with the
	// hydrated partial-parse cache: "hydrated" | "degraded" | "unknown";
	// empty (omitted) for Jobs without a hydrate initContainer.
	ParseCache       string `json:"parse_cache,omitempty"`
	ParseCacheReason string `json:"parse_cache_reason,omitempty"`
}

// ToMap converts TaskExecutionRecorded to a flat map for Redis stream publishing.
// Optional fields with empty values are omitted from the map to keep the wire
// payload compact and match TaskExecutionRecorded's `omitempty` JSON behavior.
func (e TaskExecutionRecorded) ToMap() map[string]interface{} {
	m := map[string]interface{}{
		"execution_id":      e.ExecutionID,
		"task_id":           e.TaskID,
		"job_name":          e.JobName,
		"execution_seconds": e.ExecutionSeconds,
	}
	if e.StartedAt != "" {
		m["started_at"] = e.StartedAt
	}
	if e.CompletedAt != "" {
		m["completed_at"] = e.CompletedAt
	}
	if e.ErrorMessage != "" {
		m["error_message"] = e.ErrorMessage
	}
	if e.LogS3Key != "" {
		m["log_s3_key"] = e.LogS3Key
	}
	if e.RunResultsS3Key != "" {
		m["run_results_uri"] = e.RunResultsS3Key
	}
	if e.ParseCache != "" {
		m["parse_cache"] = e.ParseCache
	}
	if e.ParseCacheReason != "" {
		m["parse_cache_reason"] = e.ParseCacheReason
	}
	return m
}
