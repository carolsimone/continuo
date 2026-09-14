// executor-controller/domain/events/retry_task.go
package events

// RetryTask is the parsed retry.task:v1 stream payload. Strict superset
// of QueryModel's wire fields: retry.task additionally carries
// task_retry_count and max_retries so the executor's outbox row can be
// stamped without a state gRPC lookup.
type RetryTask struct {
	QueryModel
	TaskRetryCount int
	MaxRetries     int
}
