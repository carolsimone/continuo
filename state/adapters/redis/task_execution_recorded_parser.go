package redis

import (
	"fmt"
	"strconv"
	"time"

	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseTaskExecutionRecorded translates a task.execution.recorded:v1 XMessage
// into a typed domain event. Fields are read as flat string values from
// msg.Values. execution_id and task_id are required and must be valid UUIDs.
// Optional fields (job_name, started_at, completed_at, execution_seconds,
// error_message, log_s3_key, parse_cache, parse_cache_reason) are nil when
// their wire value is empty.
// Malformed RFC3339 timestamps produce an error, matching the existing
// handler's discard semantics. execution_seconds silently defaults to nil
// when unparseable or non-positive. Errors are parse-permanent.
func ParseTaskExecutionRecorded(msg goredis.XMessage) (events.TaskExecutionRecorded, error) {
	executionIDStr, _ := msg.Values["execution_id"].(string)
	taskIDStr, _ := msg.Values["task_id"].(string)
	if executionIDStr == "" || taskIDStr == "" {
		return events.TaskExecutionRecorded{},
			fmt.Errorf("missing required fields (execution_id=%q task_id=%q)", executionIDStr, taskIDStr)
	}
	executionID, err := uuid.Parse(executionIDStr)
	if err != nil {
		return events.TaskExecutionRecorded{}, fmt.Errorf("invalid execution_id: %w", err)
	}
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		return events.TaskExecutionRecorded{}, fmt.Errorf("invalid task_id: %w", err)
	}
	out := events.TaskExecutionRecorded{ExecutionID: executionID, TaskID: taskID}

	if v, _ := msg.Values["job_name"].(string); v != "" {
		out.JobName = &v
	}
	if v, _ := msg.Values["started_at"].(string); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return events.TaskExecutionRecorded{}, fmt.Errorf("invalid started_at: %w", err)
		}
		out.StartedAt = &t
	}
	if v, _ := msg.Values["completed_at"].(string); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return events.TaskExecutionRecorded{}, fmt.Errorf("invalid completed_at: %w", err)
		}
		out.CompletedAt = &t
	}
	if v, _ := msg.Values["execution_seconds"].(string); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err == nil && f > 0 {
			out.ExecutionTimeSeconds = &f
		}
	}
	if v, _ := msg.Values["error_message"].(string); v != "" {
		out.ErrorMessage = &v
	}
	if v, _ := msg.Values["log_s3_key"].(string); v != "" {
		out.LogS3Key = &v
	}
	if v, _ := msg.Values["parse_cache"].(string); v != "" {
		out.ParseCache = &v
	}
	if v, _ := msg.Values["parse_cache_reason"].(string); v != "" {
		out.ParseCacheReason = &v
	}
	return out, nil
}
