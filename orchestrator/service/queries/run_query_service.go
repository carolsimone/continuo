// File: orchestrator/service/queries/run_query_service.go
//
// Package queries holds query-side application services for the orchestrator.
// It mirrors service/handlers/ on the read side: command handlers compose
// write-side stores; query services compose read-side stores.
//
// RunQueryService is the first orchestrator component that joins Neo4j (the
// :Run node and its topology_generation) with Postgres (the topology_state
// singleton row). The interfaces are owned by this package per Go convention
// (consumer defines the interface).
package queries

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
)

// RunReader is the read-side surface RunQueryService needs from a run-repo
// adapter. Satisfied by adapters/neo4j/OrchestratorQueryRepository.
type RunReader interface {
	GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error)
	GetRunTopologyGeneration(ctx context.Context, runID string) (int64, error)
	ListActiveRuns(ctx context.Context) ([]*domain.ActiveRun, error)
}

// TopologyStateReader is the read-side surface RunQueryService needs from
// the topology-state adapter. Satisfied by adapters/postgres/topologyStateRepository.
type TopologyStateReader interface {
	GetGeneration(ctx context.Context) (int64, error)
}

// RunGraphView is the result of GetRunGraph — the existing run graph plus
// the run's pinned topology_generation and the orchestrator's current
// topology_state.topology_generation.
type RunGraphView struct {
	Nodes                    []*domain.TableNode
	Edges                    []*domain.GraphEdge
	RunTopologyGeneration    int64
	LatestTopologyGeneration int64
}

// ActiveRunDriftView is the result of ListActiveRunDrifts — every in-flight
// run with its pinned generation, plus the latest generation for drift
// computation in the consumer.
type ActiveRunDriftView struct {
	ActiveRuns               []*domain.ActiveRun
	LatestTopologyGeneration int64
}

// RunQueryService composes the run-side reads (Neo4j) with the topology-state
// read (Postgres) and assembles drift-aware views.
type RunQueryService struct {
	runReader         RunReader
	topologyStateRepo TopologyStateReader
	logger            *slog.Logger
}

// NewRunQueryService constructs a RunQueryService.
func NewRunQueryService(runReader RunReader, topologyStateRepo TopologyStateReader, logger *slog.Logger) *RunQueryService {
	return &RunQueryService{
		runReader:         runReader,
		topologyStateRepo: topologyStateRepo,
		logger:            logger,
	}
}

// GetRunGraph returns the run's nodes/edges plus its pinned topology_generation
// and the latest topology_generation from topology_state. Reads are sequential.
func (s *RunQueryService) GetRunGraph(ctx context.Context, runID string) (*RunGraphView, error) {
	nodes, edges, err := s.runReader.GetRunGraph(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("RunQueryService.GetRunGraph: %w", err)
	}
	runGen, err := s.runReader.GetRunTopologyGeneration(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("RunQueryService.GetRunGraph topology_generation: %w", err)
	}
	latestGen, err := s.topologyStateRepo.GetGeneration(ctx)
	if err != nil {
		return nil, fmt.Errorf("RunQueryService.GetRunGraph latest_generation: %w", err)
	}
	if runGen > latestGen {
		s.logger.Warn("run topology_generation exceeds latest — invariant violation",
			"run_id", runID, "run_gen", runGen, "latest_gen", latestGen)
	}
	return &RunGraphView{
		Nodes:                    nodes,
		Edges:                    edges,
		RunTopologyGeneration:    runGen,
		LatestTopologyGeneration: latestGen,
	}, nil
}

// ListActiveRunDrifts returns every in-flight run plus the latest topology
// generation, so consumers (ui-service /api/schedules) can render per-schedule
// drift badges. At most one active run per schedule_name in practice; if the
// upstream invariant is ever violated, all rows are surfaced and a warning is
// logged.
func (s *RunQueryService) ListActiveRunDrifts(ctx context.Context) (*ActiveRunDriftView, error) {
	runs, err := s.runReader.ListActiveRuns(ctx)
	if err != nil {
		return nil, fmt.Errorf("RunQueryService.ListActiveRunDrifts: %w", err)
	}
	latestGen, err := s.topologyStateRepo.GetGeneration(ctx)
	if err != nil {
		return nil, fmt.Errorf("RunQueryService.ListActiveRunDrifts latest_generation: %w", err)
	}

	seen := make(map[string]int)
	for _, r := range runs {
		seen[r.ScheduleName]++
	}
	for name, n := range seen {
		if n > 1 {
			s.logger.Warn("multiple active runs for one schedule — invariant violation",
				"schedule_name", name, "count", n)
		}
	}

	return &ActiveRunDriftView{
		ActiveRuns:               runs,
		LatestTopologyGeneration: latestGen,
	}, nil
}
