package event

// EventTypeValidationNodeCompleted is the canonical outbox event_type string for
// the validation.node.completed:v1 per-node event. Defined in the domain package
// so both the emit site (service/handlers) and the publisher adapter
// (adapters/publisher) share one source of truth. The adapter imports inward
// (adapter→domain), which is the allowed direction.
const EventTypeValidationNodeCompleted = "validation_node_completed"

// EventTypeSeedBuildNodeCompleted is the canonical outbox event_type string for
// the seed.build.node.completed:v1 per-node event.
const EventTypeSeedBuildNodeCompleted = "seed_build_node_completed"

// EventTypeCompileNodeCompleted is the canonical outbox event_type string for
// the compile.node.completed:v1 per-node event.
const EventTypeCompileNodeCompleted = "compile_node_completed"

// Outbox event_type routing keys for k8s_outbox rows. Each value is the
// event_type stored on the row and matched by the publisher's toValues switch
// (EventTypeCheckDelayed is routed by Publish to the delay queue instead of a
// stream). Defined here next to the payload structs so the emit site
// (service/handlers) and the publisher adapter share one source of truth. Values
// are the wire-stored event_type strings and must not change.
const (
	EventTypeTaskStatusUpdated     = "task_status_updated"
	EventTypeTaskExecutionRecorded = "task_execution_recorded"
	EventTypeTaskRetry             = "task_retry"
	EventTypeTaskFailed            = "task_failed"
	EventTypeNodeStatusUpdated     = "node_status_updated"
	EventTypeCheckDelayed          = "check_delayed"
)

// Event is a marker interface for all events
type Event interface {
	isEvent()
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

// TaskFailed represents an event that a task has permanently failed
type TaskFailed struct {
	TaskID       string
	ScheduleID   string
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	JobName      string
	ErrorMessage string
	RetryCount   int
}

func (TaskFailed) isEvent() {}

// ToMap converts TaskFailed event to a map for Redis publishing
func (e TaskFailed) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"task_id":       e.TaskID,
		"schedule_id":   e.ScheduleID,
		"schedule_name": e.ScheduleName,
		"service_name":  e.ServiceName,
		"schema_name":   e.SchemaName,
		"table_name":    e.TableName,
		"job_name":      e.JobName,
		"error_message": e.ErrorMessage,
		"retry_count":   e.RetryCount,
	}
}

// TaskRetry represents an event that a task should be retried
type TaskRetry struct {
	TaskID       string
	ScheduleID   string
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	JobName      string
	ImageTag     string
	RetryCount   int
	MaxRetries   int
	NodeType     string
	// Operation is the dbt verb the retried Job should run (e.g. "test").
	// Empty for normal production `dbt run` retries — their wire format is
	// unchanged. Sourced from the durable CheckJobStatus.Operation (which rides
	// node.deployed:v1 / check.k8s:v1), never from the failed Job's labels: a
	// TTL-reaped Job has no labels, so a retried `dbt test` Job stays `dbt test`
	// instead of rebuilding as `dbt run`.
	Operation string
}

func (TaskRetry) isEvent() {}

// ToMap converts TaskRetry event to a map for Redis publishing.
// Uses task_retry_count (not retry_count) to match executor-controller's consumer key.
func (e TaskRetry) ToMap() map[string]interface{} {
	m := map[string]interface{}{
		"task_id":          e.TaskID,
		"schedule_id":      e.ScheduleID,
		"schedule_name":    e.ScheduleName,
		"service_name":     e.ServiceName,
		"schema_name":      e.SchemaName,
		"table_name":       e.TableName,
		"job_name":         e.JobName,
		"image_tag":        e.ImageTag,
		"task_retry_count": e.RetryCount,
		"max_retries":      e.MaxRetries,
		"node_type":        e.NodeType,
	}
	// Only stamp operation when non-empty so normal `dbt run` retries stay
	// wire-identical to before this field existed.
	if e.Operation != "" {
		m["operation"] = e.Operation
	}
	return m
}

// NodeStatusUpdated represents an event that a node's status has changed
type NodeStatusUpdated struct {
	TaskID       string
	ScheduleID   string
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	Status       string
}

func (NodeStatusUpdated) isEvent() {}

// ToMap converts NodeStatusUpdated event to a map for Redis publishing
func (e NodeStatusUpdated) ToMap() map[string]interface{} {
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
