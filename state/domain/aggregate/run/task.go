package run

import (
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

// Task is a child entity of a Run. Fields mirror the columns of tasks.
// Construction goes through NewTask / NewInheritedTask; mutation goes through
// the Run aggregate's methods (Run is the consistency boundary).
type Task struct {
	TaskID              uuid.UUID
	ScheduleID          uuid.UUID
	CreatedAt           time.Time
	ServiceName         string
	SchemaName          string
	TableName           string
	NodeKey             topology.NodeKey
	JobName             string
	Status              TaskStatus
	Attempt             int
	MaxRetries          int
	CancelledAt         *time.Time
	CancelledBy         *string
	ImageTag            string
	InheritedFromTaskID *uuid.UUID
	Operation           model.Operation
	// DispatchedAt is when the task's dispatch was written; nil until then,
	// and always nil on an inherited task, which is never dispatched.
	DispatchedAt *time.Time
}

// Node returns the (service, schema, table) triple identifying which model
// this task targets.
func (t Task) Node() NodeID {
	return NodeID{ServiceName: t.ServiceName, SchemaName: t.SchemaName, TableName: t.TableName}
}
