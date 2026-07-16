// executor-controller/domain/events/retry_task.go
package events

import "github.com/google/uuid"

// RetryTask is the parsed retry.task:v1 stream payload. Strict superset
// of QueryModel's wire fields: retry.task additionally carries
// task_retry_count and max_retries so the executor's outbox row can be
// stamped without a state gRPC lookup.
type RetryTask struct {
	QueryModel
	TaskRetryCount int
	MaxRetries     int
	// ExecutorDeploymentID names the deployment row this retry re-attempts. A
	// worker task retries in place so its lease history and attempt counter stay
	// on one row. uuid.Nil means the retry carries no such pointer and enqueues a
	// fresh deployment, which is how the Kubernetes Job path retries.
	ExecutorDeploymentID uuid.UUID
}
