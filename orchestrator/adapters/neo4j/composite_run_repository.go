package neo4jinfra

import (
	"context"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
)

// CompositeRunRepository combines RunRepository (write-side) and QueryRepository
// (read-side) to satisfy the full run.Repository interface.
type CompositeRunRepository struct {
	*RunRepository
	query *QueryRepository
}

// NewCompositeRunRepository creates a composite that implements run.Repository by
// delegating writes to RunRepository and reads to QueryRepository.
func NewCompositeRunRepository(runRepo *RunRepository, queryRepo *QueryRepository) *CompositeRunRepository {
	return &CompositeRunRepository{
		RunRepository: runRepo,
		query:         queryRepo,
	}
}

// compile-time check
var _ run.Repository = (*CompositeRunRepository)(nil)

// GetScheduleGraph delegates to QueryRepository.
func (c *CompositeRunRepository) GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error) {
	return c.query.GetScheduleGraph(ctx, scheduleName)
}

// ListRuns delegates to QueryRepository.
func (c *CompositeRunRepository) ListRuns(ctx context.Context, scheduleName string) ([]*domain.RunSummary, error) {
	return c.query.ListRuns(ctx, scheduleName)
}

// GetRunGraph delegates to QueryRepository.
func (c *CompositeRunRepository) GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error) {
	return c.query.GetRunGraph(ctx, runID)
}

// GetRunTopologyGeneration delegates to QueryRepository.
func (c *CompositeRunRepository) GetRunTopologyGeneration(ctx context.Context, runID string) (int64, error) {
	return c.query.GetRunTopologyGeneration(ctx, runID)
}

// ListActiveRuns delegates to QueryRepository.
func (c *CompositeRunRepository) ListActiveRuns(ctx context.Context) ([]*domain.ActiveRun, error) {
	return c.query.ListActiveRuns(ctx)
}
