package command

import "github.com/google/uuid"

// CheckJobStatus carries the parsed fields needed to check a K8s job and act on
// the result. It is produced by the node.deployed:v1 and check.k8s:v1 parsers.
type CheckJobStatus struct {
	TaskID           uuid.UUID
	ScheduleID       uuid.UUID
	ScheduleName     string
	ServiceName      string
	SchemaName       string
	TableName        string
	JobName          string
	NodeType         string
	ImageTag         string
	// Operation is the dbt verb the Job runs (e.g. "test"); empty for a normal
	// production `dbt run`. Sourced from durable node.deployed:v1 / check.k8s:v1
	// data, never from Job labels — a TTL-reaped Job has no labels, so a retry
	// must rebuild the same verb from this field.
	Operation        string
	RetryCount       int32 // current task retry count
	MaxRetries       int32 // maximum task retries allowed (default from config if absent)
	RunningAnnounced bool  // true once k8s has announced RUNNING for this attempt
}
