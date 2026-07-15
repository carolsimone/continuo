// Package projection holds read-only join shapes returned by query-side
// repositories. These are not aggregates and carry no invariants.
package projection

import (
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// NodeRun is one row in a node's execution history — the audit-loud projection
// of a (scheduler_tracker × task_tracker × task_execution) join filtered by
// node identity (service_name, schema_name, table_name). Returned by the
// ListNodeRuns gRPC method.
//
// One NodeRun corresponds to one task_tracker row, not one scheduler run. Per-
// task timing comes from the latest task_execution row for that task_id; rows
// with no execution yet carry nil timings and empty ErrorMessage / LogS3Key.
type NodeRun struct {
	ScheduleID      uuid.UUID
	ScheduleName    string
	Kind            string
	TerminalStatus  string
	TaskID          uuid.UUID
	TaskStatus      run.TaskStatus
	RetryCount      int
	ImageTag        string
	ManifestVersion string
	CreatedAt       time.Time
	StartedAt       *time.Time
	CompletedAt     *time.Time
	ErrorMessage    *string
	LogS3Key        *string
	Operation       string
}
