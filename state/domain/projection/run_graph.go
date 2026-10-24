package projection

import (
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
)

// RunGraphNode is one task of a run drawn as a node of the run's graph: the
// node's identity, schedule and type in the run's release, and the task's
// status. ScheduleName and NodeType are empty for a run that names no release.
type RunGraphNode struct {
	ServiceName  string
	SchemaName   string
	TableName    string
	ScheduleName string
	NodeType     string
	Status       run.TaskStatus
	TaskID       uuid.UUID
	// WaitReason is what the pending task waits for besides its upstreams;
	// empty when it waits for nothing else. WaitingSince is when it began
	// waiting, nil without a reason.
	WaitReason   run.WaitReason
	WaitingSince *time.Time
	// WaitingOnRunID is the run whose task holds the node the task waits to
	// write, read from the node's lease when the graph is read; nil when the
	// task waits for no lease or no task holds its node.
	WaitingOnRunID *uuid.UUID
}

// RunGraphEdge is one dependency between two tasks of a run, read from the
// run's release: FromNodeID depends on ToNodeID. Both are
// "service.schema.table".
type RunGraphEdge struct {
	FromNodeID string
	ToNodeID   string
}

// RunGraph is a run's tasks and the dependencies among them, the promotion
// seq the run's release was live under, and the live one. A run that names no
// release has nodes, no edges and RunPromotionSeq 0.
type RunGraph struct {
	Nodes              []RunGraphNode
	Edges              []RunGraphEdge
	RunPromotionSeq    int64
	LatestPromotionSeq int64
}

// RunSummary is one completed run of a schedule.
type RunSummary struct {
	RunID          uuid.UUID
	ScheduleName   string
	TerminalStatus run.SchedulerStatus
	// FailureReason is why the run failed when no task of it failed; empty
	// otherwise.
	FailureReason run.RunFailureReason
	CreatedAt     time.Time
	CompletedAt   time.Time
}

// ActiveRunDrift is the newest active run of a schedule and the promotion seq
// its release was live under (0 when it names no release).
type ActiveRunDrift struct {
	ScheduleName    string
	RunID           uuid.UUID
	RunPromotionSeq int64
}
