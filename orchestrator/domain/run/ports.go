package run

import (
	"context"

	"github.com/carolsimone/continuo/orchestrator/domain"
)

// Scope tells the AggregateRepository which subgraph to load when rehydrating
// the Run aggregate. Implementations live in the adapter; the domain only
// defines the sealed type.
type Scope interface{ scope() }

// ScopeFull rehydrates with every node and edge in the run.
// Used only at run initialisation to count TotalNodes.
type ScopeFull struct{}

func (ScopeFull) scope() {}

// ScopeNodeCompletion rehydrates the subgraph needed to complete the target node.
//
//	Status == "FAILED":    target + full transitive downstream
//	Status == "SUCCEEDED": target + immediate downstream + each downstream's upstreams
type ScopeNodeCompletion struct {
	Key    NodeKey
	Status string
}

func (ScopeNodeCompletion) scope() {}

// AggregateRepository is the write-side port for the Run aggregate.
// Rehydrate reconstitutes the aggregate from persistent state for the given
// scope. Save persists only the nodes present in the loaded subgraph plus
// updated counters, version, and run status.
type AggregateRepository interface {
	Rehydrate(ctx context.Context, runID string, scope Scope) (*Run, error)
	Save(ctx context.Context, run *Run) error
}

// RunQueryPort is the read-side port (CQRS). No aggregate involved.
type RunQueryPort interface {
	GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error)
	ListRuns(ctx context.Context, scheduleName string, limit, offset int) ([]*domain.RunSummary, int, error)
	GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error)
	GetRunTopologyGeneration(ctx context.Context, runID string) (int64, error)
	ListActiveRuns(ctx context.Context) ([]*domain.ActiveRun, error)
}
