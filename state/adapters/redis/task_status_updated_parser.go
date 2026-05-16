package redis

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseTaskStatusUpdated translates a task.status.updated:v1 XMessage into a
// typed domain event. Fields are read as flat string values from msg.Values
// (task_id, schedule_id, status, retry_count). Status is lowercased before
// validation. An empty or unparseable retry_count silently defaults to 0,
// matching the existing handler's semantics. Errors are parse-permanent and
// indicate the message should be discarded.
func ParseTaskStatusUpdated(msg goredis.XMessage) (events.TaskStatusUpdated, error) {
	taskIDStr, _ := msg.Values["task_id"].(string)
	scheduleIDStr, _ := msg.Values["schedule_id"].(string)
	statusStr, _ := msg.Values["status"].(string)
	retryCountStr, _ := msg.Values["retry_count"].(string)

	if taskIDStr == "" || scheduleIDStr == "" || statusStr == "" {
		return events.TaskStatusUpdated{},
			fmt.Errorf("missing required fields (task_id=%q schedule_id=%q status=%q)",
				taskIDStr, scheduleIDStr, statusStr)
	}
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		return events.TaskStatusUpdated{}, fmt.Errorf("invalid task_id: %w", err)
	}
	scheduleID, err := uuid.Parse(scheduleIDStr)
	if err != nil {
		return events.TaskStatusUpdated{}, fmt.Errorf("invalid schedule_id: %w", err)
	}
	status, err := run.ParseTaskStatus(strings.ToLower(statusStr))
	if err != nil {
		return events.TaskStatusUpdated{}, fmt.Errorf("invalid status %q: %w", statusStr, err)
	}
	var retryCount int32
	if retryCountStr != "" {
		n, err := strconv.ParseInt(retryCountStr, 10, 32)
		if err == nil {
			retryCount = int32(n)
		}
	}
	return events.TaskStatusUpdated{
		TaskID:     taskID,
		ScheduleID: scheduleID,
		Status:     status,
		RetryCount: retryCount,
	}, nil
}
