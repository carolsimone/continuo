package command

import (
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

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
	Operation string
	// ExecutorDeploymentID names the executor_deployments row holding this Job's
	// execution slot. Empty for a Job dispatched before the field existed; the
	// terminal check then emits no capacity notification for it.
	ExecutorDeploymentID string
	// Mode is the Job's dispatch mode (e.g. events.ModePromoteSeed), carried on
	// the durable check chain so a terminal result routes from the message rather
	// than from Job metadata that a TTL-reaped Job no longer has.
	Mode string
	// RuntimeManifestRef pins the artifact this attempt ran against so a retry
	// reproduces the same release.
	pkgmodel.RuntimeManifestRef
	RetryCount       int32 // current task retry count
	MaxRetries       int32 // maximum task retries allowed (default from config if absent)
	RunningAnnounced bool  // true once k8s has announced RUNNING for this attempt
}
