package run

import (
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
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
	// PlanReason says why the task runs with its operation; empty when its
	// plan stated none.
	PlanReason string
	// SeedPreflight is the check a seed's pod runs before a load that drops
	// its table; nil on every other task.
	SeedPreflight *seedreload.Preflight
	// RebuildOf is the seed whose load drops the relation this task
	// rebuilds, the smallest seed key when the run loads several seeds above
	// it; empty on every other task.
	RebuildOf topology.NodeKey
	// DispatchedAt is when the task's dispatch was written; nil until then,
	// and always nil on an inherited task, which is never dispatched.
	DispatchedAt *time.Time
}

// Spec is what every dispatch of t carries: its operation, plan reason and
// seed preflight.
func (t Task) Spec() DispatchSpec {
	return DispatchSpec{Operation: t.Operation, Reason: t.PlanReason, Preflight: t.SeedPreflight}
}

// Node returns the (service, schema, table) triple identifying which model
// this task targets.
func (t Task) Node() NodeID {
	return NodeID{ServiceName: t.ServiceName, SchemaName: t.SchemaName, TableName: t.TableName}
}
