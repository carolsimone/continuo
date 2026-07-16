package event

import pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"

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

// EventTypeExecutorJobTerminal is the canonical outbox event_type string for the
// executor.job.terminal:v1 capacity notification.
const EventTypeExecutorJobTerminal = "executor_job_terminal"

// Event is a marker interface for all events
type Event interface {
	isEvent()
}

// JobCheckRequest is the payload of a check_delayed outbox row. The Publisher
// reads it to build the check.k8s:v1 typed event; check_after gates the
// consumer-side re-delivery delay.
type JobCheckRequest struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	JobName      string `json:"job_name"`
	CheckAfter   int64  `json:"check_after"` // Unix timestamp for delayed processing
	NodeType     string `json:"node_type"`
	ImageTag     string `json:"image_tag"`
	// Operation is the dbt verb the Job runs (e.g. "test"); empty for a normal
	// production `dbt run`. It recirculates on every check.k8s:v1 self-poll so a
	// check that lands after the Job is TTL-reaped still carries the verb for retry.
	Operation string `json:"operation,omitempty"`
	// ExecutorDeploymentID names the executor_deployments row holding this Job's
	// execution slot; it recirculates on every self-poll so the terminal check
	// can name the row to release.
	ExecutorDeploymentID string `json:"executor_deployment_id,omitempty"`
	// Mode is the dispatch mode of this Job (e.g. events.ModePromoteSeed),
	// carried durably so a terminal check routes without reading Job metadata.
	Mode string `json:"mode,omitempty"`
	// RuntimeManifestRef names the prebuilt artifact the Job executes against,
	// recirculated so a retry issued from a check reproduces the same release.
	pkgmodel.RuntimeManifestRef
	RetryCount int `json:"retry_count"` // current task retry count
	MaxRetries int `json:"max_retries"` // maximum task retries allowed
	// RunningAnnounced is true once RUNNING has been announced for this attempt.
	RunningAnnounced bool `json:"running_announced"`
}

func (JobCheckRequest) isEvent() {}

// TaskFailed represents an event that a task has permanently failed
type TaskFailed struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	JobName      string `json:"job_name"`
	ErrorMessage string `json:"error_message"`
	RetryCount   int    `json:"retry_count"`
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
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	JobName      string `json:"job_name"`
	ImageTag     string `json:"image_tag"`
	RetryCount   int    `json:"retry_count"`
	MaxRetries   int    `json:"max_retries"`
	NodeType     string `json:"node_type"`
	// Operation is the dbt verb the retried Job should run (e.g. "test").
	// Empty for normal production `dbt run` retries — their wire format is
	// unchanged. Sourced from the durable CheckJobStatus.Operation (which rides
	// node.deployed:v1 / check.k8s:v1), never from the failed Job's labels: a
	// TTL-reaped Job has no labels, so a retried `dbt test` Job stays `dbt test`
	// instead of rebuilding as `dbt run`.
	Operation string `json:"operation,omitempty"`
	// RuntimeManifestRef pins the retry to the artifact the failed attempt ran
	// against. Sourced from the durable check chain rather than from the Job, so
	// a retry reproduces the original release instead of binding to whatever
	// artifact is current when it runs.
	pkgmodel.RuntimeManifestRef
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
	// The four reference fields are stamped together or not at all: the consumer
	// rejects a partial reference, so a task with no runtime manifest must emit
	// no field rather than four empty ones.
	if e.RuntimeManifestRef != (pkgmodel.RuntimeManifestRef{}) {
		m["runtime_manifest_uri"] = e.RuntimeManifestURI
		m["runtime_manifest_sha256"] = e.RuntimeManifestSHA256
		m["runtime_manifest_dbt_version"] = e.RuntimeManifestDBTVersion
		m["runtime_manifest_parse_context_sha256"] = e.RuntimeManifestParseContextSHA256
	}
	return m
}

// NodeStatusUpdated represents an event that a node's status has changed
type NodeStatusUpdated struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	Status       string `json:"status"`
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
