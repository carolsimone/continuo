package repository

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
)

// RunReadRepository reads runs the way people see them: a run's graph from
// its own release, a schedule's completed runs, and how far each schedule's
// newest active run is behind the live release.
type RunReadRepository interface {
	// GetRunGraph returns runID's tasks and the dependencies among them in
	// the run's release, with the run's and the tenant's live promotion seq.
	// An unknown run yields no nodes and RunPromotionSeq 0.
	GetRunGraph(ctx context.Context, tenantID string, runID uuid.UUID) (projection.RunGraph, error)

	// ListCompletedRuns returns one page of scheduleName's completed runs,
	// newest first, and how many completed runs the schedule has.
	ListCompletedRuns(ctx context.Context, scheduleName string, limit, offset int) ([]projection.RunSummary, int, error)

	// ListActiveRunDrifts returns the tenant's live promotion seq (0 when no
	// release is live) and the newest active run of every schedule.
	ListActiveRunDrifts(ctx context.Context, tenantID string) (int64, []projection.ActiveRunDrift, error)

	// LastSuccessDurations returns how long each node's latest successful
	// execution took: the latest execution without an error and with a
	// positive duration of a succeeded task of the node that is not a test.
	// A node without one, inherited tasks included since they record no
	// execution, is absent from the map.
	LastSuccessDurations(ctx context.Context, nodes []run.NodeID) (map[run.NodeID]time.Duration, error)
}
