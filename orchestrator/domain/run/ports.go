package run

import (
	"context"

	"github.com/carolsimone/continuo/orchestrator/domain"
)

// LoadHint tells the AggregateRepository what subgraph to load.
// Implementations live in the adapter; the domain only defines the sealed type.
type LoadHint interface{ loadHint() }

// LoadHintFull loads all nodes and edges for the run.
// Used only at run initialization to count TotalNodes.
type LoadHintFull struct{}

func (LoadHintFull) loadHint() {}

// LoadHintNodeCompletion loads the subgraph needed to complete the target node.
//   Status == "FAILED":    target + full transitive downstream
//   Status == "SUCCEEDED": target + immediate downstream + each downstream's upstreams
type LoadHintNodeCompletion struct {
	Key    NodeKey
	Status string
}

func (LoadHintNodeCompletion) loadHint() {}

// LoadHintResetDownstream loads the transitive downstream of the target node.
// Used by ResetDownstream to reset SKIPPED nodes back to PENDING.
type LoadHintResetDownstream struct {
	Key NodeKey
}

func (LoadHintResetDownstream) loadHint() {}

// AggregateRepository is the write-side port for the Run aggregate.
// Load rehydrates the aggregate with an operation-scoped subgraph.
// Save persists only the nodes present in the loaded subgraph plus
// updated counters, version, and run status.
type AggregateRepository interface {
	Load(ctx context.Context, runID string, hint LoadHint) (*Run, error)
	Save(ctx context.Context, run *Run) error
}

// RunQueryPort is the read-side port (CQRS). No aggregate involved.
type RunQueryPort interface {
	GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error)
	ListRuns(ctx context.Context, scheduleName string) ([]*domain.RunSummary, error)
	GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error)
	GetRunTopologyGeneration(ctx context.Context, runID string) (int64, error)
	ListActiveRuns(ctx context.Context) ([]*domain.ActiveRun, error)
}
