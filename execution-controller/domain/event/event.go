package event

// Event is a marker interface for all events
type Event interface {
	isEvent()
}

// Outbox event types. Each names the typed payload stored in an execution_outbox
// row; the publisher routes on it.
const (
	EventTypeTaskStatusUpdated     = "task_status_updated"
	EventTypeTaskExecutionRecorded = "task_execution_recorded"
	EventTypeNodeUpdated           = "node_updated"
	// EventTypeCheckDelayed rows are not XADDed: the publisher writes them to
	// the delay queue, and the promoter moves them onto check.k8s:v1 when due.
	EventTypeCheckDelayed = "check_delayed"
)

// NodeUpdated is the payload of an execution_outbox row whose event_type is
// "node_updated". The dispatcher writes it (status FAILED) when a deploy
// exhausts its retry budget, so orchestrator's HandleNodeCompleted advances
// the schedule. Stream: node.updated:v1.
type NodeUpdated struct {
	TaskID       string
	ScheduleID   string
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	Status       string
}

func (NodeUpdated) isEvent() {}

// ToMap converts NodeUpdated to a flat map for Redis stream publishing.
func (e NodeUpdated) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"task_id":       e.TaskID,
		"schedule_id":   e.ScheduleID,
		"schedule_name": e.ScheduleName,
		"service_name":  e.ServiceName,
		"schema_name":   e.SchemaName,
		"table_name":    e.TableName,
		"status":        e.Status,
	}
}

// JobCheckRequest is the payload of a check_delayed outbox row. The Publisher
// writes it into the delay queue (HSET payload + ZADD check_after as the
// score); check_after is the ZSET score / due time, not a consumer gate.
type JobCheckRequest struct {
	TaskID       string
	ScheduleID   string
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	JobName      string
	CheckAfter   int64 // Unix timestamp for delayed processing
	NodeType     string
	ImageTag     string
	// Operation is the dbt verb the Job runs (e.g. "test"); empty for a normal
	// production `dbt run`. It travels in the durable payload (delay-queue
	// ticket → promoted stream message) so a check that lands after the Job is
	// TTL-reaped still carries the verb for retry.
	Operation  string
	RetryCount int // current task retry count
	MaxRetries int // maximum task retries allowed
	// RunningAnnounced is true once RUNNING has been announced for this attempt.
	RunningAnnounced bool
}

func (JobCheckRequest) isEvent() {}
