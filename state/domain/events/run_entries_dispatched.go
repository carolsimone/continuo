package events

import (
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
)

// RunEntriesDispatched is the typed form of run.entries.dispatched:v1.
type RunEntriesDispatched struct {
	ScheduleID     uuid.UUID
	TotalTaskCount int32
	AllTasks       []RunEntriesDispatchedTask
}

// RunEntriesDispatchedTask is one task in a RunEntriesDispatched event.
// InheritedFromTaskID is nil unless this row inherits from a previous run.
type RunEntriesDispatchedTask struct {
	TaskID              uuid.UUID
	ServiceName         string
	SchemaName          string
	TableName           string
	Status              model.TaskStatus
	MaxRetries          int32
	ManifestVersion     string
	ImageTag            string
	InheritedFromTaskID *uuid.UUID
}
