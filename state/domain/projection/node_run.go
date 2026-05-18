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
	ScheduleID      uuid.UUID      `db:"run_id"`
	ScheduleName    string         `db:"schedule_name"`
	Kind            string         `db:"kind"`
	TerminalStatus  string         `db:"terminal_status"`
	TaskID          uuid.UUID      `db:"task_id"`
	TaskStatus      run.TaskStatus `db:"task_status"`
	RetryCount      int            `db:"retry_count"`
	ImageTag        string         `db:"image_tag"`
	ManifestVersion string         `db:"manifest_version"`
	CreatedAt       time.Time      `db:"created_at"`
	StartedAt       *time.Time     `db:"started_at"`
	CompletedAt     *time.Time     `db:"completed_at"`
	ErrorMessage    *string        `db:"error_message"`
	LogS3Key        *string        `db:"log_s3_key"`
}
