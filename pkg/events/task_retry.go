package events

import pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"

// TaskRetry — stream: retry.task:v1
// Published by:
//   - k8s-controller (a Job that failed with retry budget left)
//   - executor-controller (a worker task parked after a retryable failure, and a
//     lease the reaper expired with retry budget left)
//
// Consumed by: executor-controller. ToMap is the single serializer for this
// stream — every producer emits through it so the wire shape stays identical
// across services.
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
	// Operation is the dbt verb the retried task should run (e.g. "test"). Empty
	// for a normal production `dbt run` retry. It is sourced from the durable
	// record of the failed attempt rather than from the attempt's Kubernetes Job,
	// so a retry issued after the Job is TTL-reaped still carries the verb.
	Operation string `json:"operation,omitempty"`
	// ExecutorDeploymentID names the executor deployment this retry re-attempts in
	// place. A worker task retries on its own row so its lease history and attempt
	// counter stay in one place. Empty enqueues a fresh deployment, which is how
	// the Kubernetes Job path retries.
	ExecutorDeploymentID string `json:"executor_deployment_id,omitempty"`
	// DBTUniqueID is the one dbt node this task invokes, carried so a retry
	// selects the same node instead of resolving it again.
	DBTUniqueID string `json:"dbt_unique_id,omitempty"`
	// RuntimeManifestRef pins the retry to the artifact the failed attempt ran
	// against, so a retry reproduces the original release instead of binding to
	// whatever artifact is current when it runs.
	pkgmodel.RuntimeManifestRef
}

// ToMap converts TaskRetry to a flat map for Redis stream publishing. The retry
// count is emitted as task_retry_count, the key the executor's consumer reads, to
// distinguish the task-level count from a message-level one.
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
	// wire-identical to a producer that does not set it.
	if e.Operation != "" {
		m["operation"] = e.Operation
	}
	// Only a worker retry names a row to re-attempt; a Job retry omits the field
	// so the executor enqueues a fresh deployment.
	if e.ExecutorDeploymentID != "" {
		m["executor_deployment_id"] = e.ExecutorDeploymentID
	}
	if e.DBTUniqueID != "" {
		m["dbt_unique_id"] = e.DBTUniqueID
	}
	// The four reference fields are stamped together or not at all: the consumer
	// rejects a partial reference, so a task with no runtime manifest must emit no
	// field rather than four empty ones.
	if e.RuntimeManifestRef != (pkgmodel.RuntimeManifestRef{}) {
		m["runtime_manifest_uri"] = e.RuntimeManifestURI
		m["runtime_manifest_sha256"] = e.RuntimeManifestSHA256
		m["runtime_manifest_dbt_version"] = e.RuntimeManifestDBTVersion
		m["runtime_manifest_parse_context_sha256"] = e.RuntimeManifestParseContextSHA256
	}
	return m
}
