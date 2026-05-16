package redis

import (
	"encoding/json"
	"fmt"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseRunEntriesDispatched translates a run.entries.dispatched:v1 XMessage
// into a typed domain event. All errors are parse-permanent — the binding
// wraps with events.ErrPermanent.
func ParseRunEntriesDispatched(msg goredis.XMessage) (events.RunEntriesDispatched, error) {
	payloadStr, _ := msg.Values["payload"].(string)
	if payloadStr == "" {
		return events.RunEntriesDispatched{}, fmt.Errorf("missing payload field")
	}
	var wire pkgevents.RunEntriesDispatched
	if err := json.Unmarshal([]byte(payloadStr), &wire); err != nil {
		return events.RunEntriesDispatched{}, fmt.Errorf("invalid JSON: %w", err)
	}
	scheduleID, err := uuid.Parse(wire.ScheduleID)
	if err != nil {
		return events.RunEntriesDispatched{}, fmt.Errorf("invalid schedule_id: %w", err)
	}
	tasks := make([]events.RunEntriesDispatchedTask, 0, len(wire.AllTasks))
	for _, t := range wire.AllTasks {
		taskID, err := uuid.Parse(t.TaskID)
		if err != nil {
			return events.RunEntriesDispatched{}, fmt.Errorf("invalid task_id %q: %w", t.TaskID, err)
		}
		status := run.TaskStatusPending
		if t.Status != "" {
			s, err := run.ParseTaskStatus(t.Status)
			if err != nil {
				return events.RunEntriesDispatched{}, fmt.Errorf("invalid task status %q: %w", t.Status, err)
			}
			status = s
		}
		var inherited *uuid.UUID
		if t.InheritedFromTaskID != "" {
			parsed, err := uuid.Parse(t.InheritedFromTaskID)
			if err != nil {
				return events.RunEntriesDispatched{}, fmt.Errorf("invalid inherited_from_task_id %q: %w", t.InheritedFromTaskID, err)
			}
			inherited = &parsed
		}
		tasks = append(tasks, events.RunEntriesDispatchedTask{
			TaskID:              taskID,
			ServiceName:         t.ServiceName,
			SchemaName:          t.SchemaName,
			TableName:           t.TableName,
			Status:              status,
			MaxRetries:          t.MaxRetries,
			ManifestVersion:     t.ManifestVersion,
			ImageTag:            t.ImageTag,
			InheritedFromTaskID: inherited,
		})
	}
	return events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: wire.TotalTaskCount,
		AllTasks:       tasks,
	}, nil
}
