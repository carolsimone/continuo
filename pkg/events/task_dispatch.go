package events

import "time"

// TaskDispatchSchemaVersion is the schema_version of task.dispatch:v1 entries.
const TaskDispatchSchemaVersion = 1

// The statuses of an attempt on the two task streams. A dispatch's
// known_status is dispatched, queued or running; a report's status is
// queued, running, succeeded or failed.
const (
	AttemptDispatched = "dispatched"
	AttemptQueued     = "queued"
	AttemptRunning    = "running"
	AttemptSucceeded  = "succeeded"
	AttemptFailed     = "failed"
)

// TaskDispatch is the payload of a task.dispatch:v1 entry: one attempt of one
// task of a run, as state decided it. KnownStatus is state's view of the
// attempt when it wrote the entry: dispatched on the first send and on every
// send before execution-controller acknowledged it, queued or running on a
// question about an acknowledged attempt. TimeoutSeconds is how long the
// attempt's container may run once its Job has started its pod. No retry
// budget travels: state decides every retry.
type TaskDispatch struct {
	RunID          string `json:"run_id"`
	TaskID         string `json:"task_id"`
	Attempt        int32  `json:"attempt"`
	KnownStatus    string `json:"known_status"`
	ScheduleName   string `json:"schedule_name"`
	NodeKey        string `json:"node_key"`
	ServiceName    string `json:"service_name"`
	SchemaName     string `json:"schema_name"`
	TableName      string `json:"table_name"`
	NodeType       string `json:"node_type"`
	Operation      string `json:"operation"`
	ImageTag       string `json:"image_tag"`
	SecretRef      string `json:"secret_ref"`
	JobName        string `json:"job_name"`
	TimeoutSeconds int64  `json:"timeout_seconds"`
}

// TaskDispatchFields renders p as the Redis fields of one task.dispatch:v1
// entry. eventID is the id of the outbox row that carries the entry.
func TaskDispatchFields(eventID, tenantID, producer string, occurredAt time.Time, p TaskDispatch) (map[string]any, error) {
	env := Envelope{
		EventID:       eventID,
		TenantID:      tenantID,
		OccurredAt:    occurredAt,
		Producer:      producer,
		SchemaVersion: TaskDispatchSchemaVersion,
	}
	return env.Fields(p)
}
