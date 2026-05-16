package events

import (
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// TaskStatusUpdated is the typed form of task.status.updated:v1.
type TaskStatusUpdated struct {
	TaskID     uuid.UUID
	ScheduleID uuid.UUID
	Status     run.TaskStatus
	RetryCount int32
}
