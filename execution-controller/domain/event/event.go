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
	EventTypeNodeDeployed          = "node_deployed"
	EventTypeNodeUpdated           = "node_updated"
	EventTypeTaskRetry             = "task_retry"
	EventTypeTaskFailed            = "task_failed"
	// EventTypeCheckDelayed rows are not XADDed: the publisher writes them to
	// the delay queue, and the promoter moves them onto check.k8s:v1 when due.
	EventTypeCheckDelayed = "check_delayed"
	// Per-node terminal results of the three candidate legs.
	EventTypeValidationNodeCompleted = "validation_node_completed"
	EventTypeSeedBuildNodeCompleted  = "seed_build_node_completed"
	EventTypeCompileNodeCompleted    = "compile_node_completed"
)

// JobDeployed is the payload of an executor_outbox row whose event_type is
// "node_deployed". The dispatcher writes it after a deploy succeeds; the
// publisher reads it to build the node.deployed:v1 typed wire event
// (pkg/events.NodeDeployed). Stream: node.deployed:v1.
type JobDeployed struct {
	TaskID       string
	ScheduleID   string
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	JobName      string
	NodeType     string
	ImageTag     string
	// Operation is the dbt verb this Job runs (e.g. "test"); empty for a normal
	// production `dbt run`. It flows onto node.deployed:v1 so k8s-controller
	// carries it through the durable check/retry chain.
	Operation      string
	TaskRetryCount int // task-level retry count (not outbox delivery retries)
	MaxRetries     int // maximum task retries allowed
}

func (JobDeployed) isEvent() {}

// NodeUpdated is the payload of an executor_outbox row whose event_type is
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
