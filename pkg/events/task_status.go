package events

// TaskStatusUpdated — stream: task.status.updated:v1
// Published by: executor-controller (RUNNING), k8s-controller (SUCCEEDED/FAILED)
// Consumed by: state
type TaskStatusUpdated struct {
	TaskID     string `json:"task_id"`
	ScheduleID string `json:"schedule_id"`
	Status     string `json:"status"`      // RUNNING | SUCCEEDED | FAILED
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
	return m
}
