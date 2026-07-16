package events

import pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"

// ModePromoteSeed is the job mode for prod seed builds triggered on promotion.
// These jobs run as real prod dbt seeds but carry no state-bound schedule/run,
// so k8s-controller suppresses the production lifecycle events for them (no
// task.status.updated / task.execution.recorded). Fire-and-forget semantics.
// This constant is the single shared definition; orchestrator stamps it on the
// query.model:v1 payload, executor passes it as a Job label, and k8s-controller
// reads the label to route the terminal status away from the production path.
const ModePromoteSeed = "promote_seed"

// NodeDeployed — stream: node.deployed:v1
// Published by: executor-controller (after a K8s Job create succeeds)
// Consumed by: k8s-controller (to start watching the Job's status)
//
// Carried as the JSON `payload` field of the Redis message; transport metadata
// (outbox_entry_id) travels as a flat sibling field for consumer-side dedup.
// The task-level retry count is named task_retry_count to distinguish it from
// outbox delivery retries.
type NodeDeployed struct {
	TaskID         string `json:"task_id"`
	ScheduleID     string `json:"schedule_id"`
	ScheduleName   string `json:"schedule_name"`
	ServiceName    string `json:"service_name"`
	SchemaName     string `json:"schema_name"`
	TableName      string `json:"table_name"`
	JobName        string `json:"job_name"`
	NodeType       string `json:"node_type"`
	ImageTag       string `json:"image_tag"`
	// Operation is the dbt verb this Job runs (e.g. "test"). Empty for a normal
	// production `dbt run`, whose wire format is unchanged. It rides the durable
	// check/retry chain so a retry after the Job is TTL-reaped still rebuilds the
	// same verb — it is never re-derived from Job metadata that may be gone.
	Operation string `json:"operation,omitempty"`
	// ExecutorDeploymentID names the executor_deployments row that reserved this
	// Job's execution slot. It rides the whole check chain so the terminal
	// notification can name the row to release without re-reading the Job.
	ExecutorDeploymentID string `json:"executor_deployment_id,omitempty"`
	// Mode is the dispatch mode of this Job (e.g. events.ModePromoteSeed). It
	// travels durably so a terminal check routes on the message rather than on
	// Job metadata, which is gone once the Job is TTL-reaped.
	Mode string `json:"mode,omitempty"`
	// RuntimeManifestRef names the prebuilt artifact this Job executes against.
	// It recirculates so a retry reproduces the original release exactly instead
	// of binding to whatever artifact is current when the retry runs.
	pkgmodel.RuntimeManifestRef
	TaskRetryCount int32 `json:"task_retry_count"`
	MaxRetries     int32 `json:"max_retries"`
}

// CheckK8s — stream: check.k8s:v1
// Published and consumed by: k8s-controller (the delayed status-recheck self-loop)
//
// Carried as the JSON `payload` field of the Redis message. The delay timestamp
// (check_after) and dedup metadata (outbox_entry_id) travel as flat sibling
// fields: check_after gates re-delivery in the binding before the payload is
// decoded, so it is not part of this typed payload.
type CheckK8s struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	JobName      string `json:"job_name"`
	NodeType     string `json:"node_type"`
	ImageTag     string `json:"image_tag"`
	// Operation is the dbt verb this Job runs (e.g. "test"); empty for a normal
	// production `dbt run`. It recirculates on every check.k8s:v1 self-poll so a
	// check that lands after the Job is gone still retains the verb for retry.
	Operation string `json:"operation,omitempty"`
	// ExecutorDeploymentID names the executor_deployments row holding this Job's
	// execution slot; it recirculates on every self-poll so the terminal check
	// can release that slot.
	ExecutorDeploymentID string `json:"executor_deployment_id,omitempty"`
	// Mode is the dispatch mode of this Job (e.g. events.ModePromoteSeed),
	// carried durably so a terminal check routes without reading Job metadata.
	Mode string `json:"mode,omitempty"`
	// RuntimeManifestRef names the prebuilt artifact this Job executes against,
	// recirculated so a retry issued from a check reproduces the same release.
	pkgmodel.RuntimeManifestRef
	RetryCount int32 `json:"retry_count"`
	MaxRetries int32 `json:"max_retries"`
	// RunningAnnounced is true once k8s-controller has announced this attempt as
	// RUNNING on task.status.updated:v1. It rides the check.k8s:v1 self-poll loop
	// so RUNNING is emitted exactly once per attempt; a fresh node.deployed:v1
	// (a new attempt) carries no such field and so resets it to false.
	RunningAnnounced bool `json:"running_announced"`
}
