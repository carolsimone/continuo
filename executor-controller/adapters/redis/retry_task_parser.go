// executor-controller/adapters/redis/retry_task_parser.go
package redis

import (
	"fmt"
	"strconv"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseRetryTask translates a retry.task:v1 XMessage into a typed
// domain event. Delegates the shared fields to ParseQueryModel and adds
// the retry-specific integer fields. Absent retry fields default to 0;
// non-numeric values are permanent errors.
func ParseRetryTask(msg goredis.XMessage) (events.RetryTask, error) {
	base, err := ParseQueryModel(msg)
	if err != nil {
		return events.RetryTask{}, err
	}
	taskRetryCount, err := optionalInt(msg.Values, "task_retry_count")
	if err != nil {
		return events.RetryTask{}, fmt.Errorf("invalid task_retry_count: %w", err)
	}
	maxRetries, err := optionalInt(msg.Values, "max_retries")
	if err != nil {
		return events.RetryTask{}, fmt.Errorf("invalid max_retries: %w", err)
	}
	// executor_deployment_id points a worker task's retry at the row it
	// re-attempts. Absent or empty → uuid.Nil, which retries by enqueueing a
	// fresh deployment; present-but-malformed → permanent error.
	var deploymentID uuid.UUID
	if s := stringField(msg.Values, "executor_deployment_id"); s != "" {
		deploymentID, err = uuid.Parse(s)
		if err != nil {
			return events.RetryTask{}, fmt.Errorf("invalid executor_deployment_id: %w", err)
		}
	}
	return events.RetryTask{
		QueryModel:           base,
		TaskRetryCount:       taskRetryCount,
		MaxRetries:           maxRetries,
		ExecutorDeploymentID: deploymentID,
	}, nil
}

// optionalInt returns 0 when the field is absent or empty, the parsed
// integer when present and numeric, and an error when present but
// non-numeric (a malformed wire payload).
func optionalInt(values map[string]interface{}, key string) (int, error) {
	s := stringField(values, key)
	if s == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return n, nil
}
