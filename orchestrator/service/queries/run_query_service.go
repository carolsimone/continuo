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
	// ListActiveRuns returns in-flight runs (completed_at IS NULL) ordered by
	// schedule_name, then newest-first (created_at DESC), so callers can keep
	// the head row per schedule as the current run.
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

// ListActiveRunDrifts returns one in-flight run per schedule (the newest) plus
// the latest topology generation, so consumers (ui /api/schedules) can
// render per-schedule drift badges. ListActiveRuns returns rows ordered by
// schedule_name then newest-first (created_at DESC); this method keeps the head
// row per schedule and drops any extras with a warning.
func (s *RunQueryService) ListActiveRunDrifts(ctx context.Context) (*ActiveRunDriftView, error) {
	runs, err := s.runReader.ListActiveRuns(ctx)
	if err != nil {
		return nil, fmt.Errorf("RunQueryService.ListActiveRunDrifts: %w", err)
	}
	latestGen, err := s.topologyStateRepo.GetGeneration(ctx)
	if err != nil {
		return nil, fmt.Errorf("RunQueryService.ListActiveRunDrifts latest_generation: %w", err)
	}

	// Keep one drift row per schedule — the newest in-flight run. ListActiveRuns
	// returns rows ordered by schedule_name then newest-first (created_at DESC),
	// so the first row seen for a schedule is the one to surface. Extra rows
	// (only possible for legacy un-finalized runs) are dropped and logged rather
	// than surfaced ambiguously.
	deduped := make([]*domain.ActiveRun, 0, len(runs))
	kept := make(map[string]bool, len(runs))
	for _, r := range runs {
		if kept[r.ScheduleName] {
			s.logger.Warn("extra active run for schedule — dropping older row",
				"schedule_name", r.ScheduleName, "run_id", r.RunID)
			continue
		}
		kept[r.ScheduleName] = true
		deduped = append(deduped, r)
	}

	return &ActiveRunDriftView{
		ActiveRuns:               deduped,
		LatestTopologyGeneration: latestGen,
	}, nil
}
