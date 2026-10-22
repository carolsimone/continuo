package events

import "time"

// TaskStatusReportSchemaVersion is the schema_version of
// task.status.updated:v2 entries.
const TaskStatusReportSchemaVersion = 1

// TaskStatusReport is the payload of a task.status.updated:v2 entry: what
// execution-controller knows of one attempt. A report is cumulative: it
// carries every fact known of the attempt so far, so a final report holds the
// queue and start times even when the reports before it were lost. Status is
// queued (execution-controller accepted the attempt), running (its main
// container started), succeeded or failed; a failed report names its
// FailureReason and the FailureClass that reason is filed under. Times are
// execution-controller's clock and only displayed.
type TaskStatusReport struct {
	RunID            string     `json:"run_id"`
	TaskID           string     `json:"task_id"`
	Attempt          int32      `json:"attempt"`
	JobName          string     `json:"job_name"`
	Status           string     `json:"status"`
	QueuedAt         *time.Time `json:"queued_at,omitempty"`
	StartedAt        *time.Time `json:"started_at,omitempty"`
	FinishedAt       *time.Time `json:"finished_at,omitempty"`
	ExecutionSeconds *float64   `json:"execution_seconds,omitempty"`
	ExitCode         *int32     `json:"exit_code,omitempty"`
	FailureClass     string     `json:"failure_class,omitempty"`
	FailureReason    string     `json:"failure_reason,omitempty"`
	ErrorMessage     string     `json:"error_message,omitempty"`
	LogS3Key         string     `json:"log_s3_key,omitempty"`
	RunResultsURI    string     `json:"run_results_uri,omitempty"`
	ParseCache       string     `json:"parse_cache,omitempty"`
	ParseCacheReason string     `json:"parse_cache_reason,omitempty"`
}

// TaskStatusReportFields renders p as the Redis fields of one
// task.status.updated:v2 entry, its times in UTC. eventID is the id of the
// outbox row that carries the entry.
func TaskStatusReportFields(eventID, tenantID, producer string, occurredAt time.Time, p TaskStatusReport) (map[string]any, error) {
	p.QueuedAt, p.StartedAt, p.FinishedAt = utc(p.QueuedAt), utc(p.StartedAt), utc(p.FinishedAt)
	env := Envelope{
		EventID:       eventID,
		TenantID:      tenantID,
		OccurredAt:    occurredAt,
		Producer:      producer,
		SchemaVersion: TaskStatusReportSchemaVersion,
	}
	return env.Fields(p)
}

// utc returns t in UTC, or nil.
func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
