// Package projection holds read-only join shapes returned by query-side
// repositories. These are not aggregates and carry no invariants.
package projection

import (
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// NodeRun is one row in a node's execution history — the audit-loud projection
// of a (runs × tasks × task_attempts) join filtered by
// node identity (service_name, schema_name, table_name). Returned by the
// ListNodeRuns gRPC method.
//
// One NodeRun corresponds to one task row, not one scheduler run. The
// timings, error, log, results and failure fields come from the task's latest
// attempt; a task with no attempt yet carries nil for all of them.
type NodeRun struct {
	ScheduleID     uuid.UUID
	ScheduleName   string
	Kind           string
	TerminalStatus string
	TaskID         uuid.UUID
	TaskStatus     run.TaskStatus
	// Attempt is the number of the task's current attempt, 0-based.
	Attempt       int
	ImageTag      string
	CreatedAt     time.Time
	StartedAt     *time.Time
	CompletedAt   *time.Time
	ErrorMessage  *string
	LogS3Key      *string
	RunResultsURI *string
	// FailureClass and FailureReason say why the latest attempt failed; nil
	// while it has not failed.
	FailureClass  *string
	FailureReason *string
	Operation     string
	// ContentHash is the node's content hash in the run's release; empty when
	// the run names no release.
	ContentHash string
	// WaitReason is what the pending task waits for besides its upstreams;
	// empty when it waits for nothing else. WaitingSince is when it began
	// waiting, nil without a reason.
	WaitReason   run.WaitReason
	WaitingSince *time.Time
	// WaitingOnRunID is the run whose task holds the node the task waits to
	// write, read from the node's lease when the history is read; nil when the
	// task waits for no lease or no task holds the node.
	WaitingOnRunID *uuid.UUID
}
