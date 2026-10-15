package run

import (
	"context"

	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

// ReadyCandidate is a pending, undispatched task of a run that reads, directly,
// a task of the run that has just succeeded: its id, its node as the run's
// release holds it, its Job name and retry budget as its row stores them, and
// the status of every task of the run it reads.
type ReadyCandidate struct {
	TaskID           uuid.UUID
	Node             topology.Node
	JobName          string
	MaxRetries       int32
	UpstreamStatuses []TaskStatus
}

// Ready reports whether every task of the run the candidate reads has
// succeeded. An inherited task is stored succeeded, so it counts.
func (c ReadyCandidate) Ready() bool {
	for _, s := range c.UpstreamStatuses {
		if s != TaskStatusSucceeded {
			return false
		}
	}
	return true
}

// TaskGraph is the port that reads how a run's tasks connect over the edges
// of its release. An edge counts only when both its ends are
// tasks of the run. The adapter that satisfies it reads inside the current
// Unit-of-Work transaction, so it sees the statuses that transaction wrote.
type TaskGraph interface {
	// ReadyCandidates returns, in key order, the pending, undispatched tasks
	// of runID that read the task at key directly over releaseID's edges, each
	// with the statuses of its upstream tasks in key order.
	ReadyCandidates(ctx context.Context, runID uuid.UUID, releaseID string, key topology.NodeKey) ([]ReadyCandidate, error)

	// PendingDescendants returns, in key order, the ids of the pending,
	// undispatched tasks of runID reached from the task at key over
	// releaseID's edges, passing only through such tasks: once key has failed
	// for good, none of them can run. Each is returned once, however many
	// paths reach it.
	PendingDescendants(ctx context.Context, runID uuid.UUID, releaseID string, key topology.NodeKey) ([]uuid.UUID, error)
}
