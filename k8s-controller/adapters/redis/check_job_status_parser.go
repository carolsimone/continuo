package redis

import (
	"fmt"
	"strconv"

	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseNodeDeployed parses a node.deployed:v1 message into a CheckJobStatus.
// node.deployed:v1 (from executor-controller) carries the current retry count
// in the "task_retry_count" field.
func ParseNodeDeployed(msg goredis.XMessage, defaultMaxRetries int) (command.CheckJobStatus, error) {
	return parseCheckJobStatus(msg, "task_retry_count", defaultMaxRetries)
}

// ParseCheckK8s parses a check.k8s:v1 message into a CheckJobStatus.
// check.k8s:v1 (self-looped re-check tickets) carries the current retry count
// in the "retry_count" field.
func ParseCheckK8s(msg goredis.XMessage, defaultMaxRetries int) (command.CheckJobStatus, error) {
	return parseCheckJobStatus(msg, "retry_count", defaultMaxRetries)
}

// parseCheckJobStatus extracts the shared flat fields of both streams. The two
// streams differ only in which field holds the current retry count, named by
// retryCountField. max_retries falls back to defaultMaxRetries when absent.
func parseCheckJobStatus(msg goredis.XMessage, retryCountField string, defaultMaxRetries int) (command.CheckJobStatus, error) {
	taskIDStr, _ := msg.Values["task_id"].(string)
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		return command.CheckJobStatus{}, fmt.Errorf("invalid task_id: %w", err)
	}
	scheduleIDStr, _ := msg.Values["schedule_id"].(string)
	scheduleID, err := uuid.Parse(scheduleIDStr)
	if err != nil {
		return command.CheckJobStatus{}, fmt.Errorf("invalid schedule_id: %w", err)
	}

	scheduleName, _ := msg.Values["schedule_name"].(string)
	serviceName, _ := msg.Values["service_name"].(string)
	schema, _ := msg.Values["schema_name"].(string)
	tableName, _ := msg.Values["table_name"].(string)
	jobName, _ := msg.Values["job_name"].(string)
	nodeType, _ := msg.Values["node_type"].(string)
	imageTag, _ := msg.Values["image_tag"].(string)

	retryCount := int32(0)
	if s, _ := msg.Values[retryCountField].(string); s != "" {
		if n, convErr := strconv.ParseInt(s, 10, 32); convErr == nil {
			retryCount = int32(n)
		}
	}

	// A non-positive max_retries is treated as absent; fall back to the default.
	maxRetries := int32(defaultMaxRetries)
	if s, _ := msg.Values["max_retries"].(string); s != "" {
		if n, convErr := strconv.ParseInt(s, 10, 32); convErr == nil && n > 0 {
			maxRetries = int32(n)
		}
	}

	return command.CheckJobStatus{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: scheduleName,
		ServiceName:  serviceName,
		SchemaName:   schema,
		TableName:    tableName,
		JobName:      jobName,
		NodeType:     nodeType,
		ImageTag:     imageTag,
		RetryCount:   retryCount,
		MaxRetries:   maxRetries,
	}, nil
}
