package run

import (
	"time"

	"github.com/google/uuid"
)

// Task is a child entity of a Run. Fields mirror the columns of task_tracker.
// Construction goes through NewTask / NewInheritedTask; mutation goes through
// the Run aggregate's methods (Run is the consistency boundary).
type Task struct {
	TaskID              uuid.UUID
	ScheduleID          uuid.UUID
	CreatedAt           time.Time
	ServiceName         string
	SchemaName          string
	TableName           string
	JobName             string
	Status              TaskStatus
	RetryCount          int
	MaxRetries          int
	CancelledAt         *time.Time
	CancelledBy         *string
	ManifestVersion     string
	ImageTag            string
	InheritedFromTaskID *uuid.UUID
}

// Node returns the (service, schema, table) triple identifying which model
// this task targets.
func (t Task) Node() NodeID {
	return NodeID{ServiceName: t.ServiceName, SchemaName: t.SchemaName, TableName: t.TableName}
}

// DispatchedTask is the per-task projection carried by run.entries.dispatched:v1
// and consumed by Run.AcceptDispatch. It is a value object (no methods that
// mutate state); the aggregate builds Task entities from it.
type DispatchedTask struct {
	TaskID              uuid.UUID
	ServiceName         string
	SchemaName          string
	TableName           string
	Status              TaskStatus
	MaxRetries          int32
	ManifestVersion     string
	ImageTag            string
	InheritedFromTaskID *uuid.UUID
}

// IsTerminal mirrors TaskStatus.IsTerminal for ergonomic call sites in the
// auto-rollup loop.
func (d DispatchedTask) IsTerminal() bool { return d.Status.IsTerminal() }
