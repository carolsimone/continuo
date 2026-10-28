package runreads

import (
	"context"

	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
)

// ScheduleGraphs reads the schedules of the live release of the tenant each
// read is given.
type ScheduleGraphs struct {
	graphs repository.ScheduleGraphRepository
}

// NewScheduleGraphs returns a ScheduleGraphs over graphs.
func NewScheduleGraphs(graphs repository.ScheduleGraphRepository) *ScheduleGraphs {
	return &ScheduleGraphs{graphs: graphs}
}

// Graph returns tenantID's live view of scheduleName.
func (s *ScheduleGraphs) Graph(ctx context.Context, tenantID, scheduleName string) (topology.ScheduleGraph, error) {
	return s.graphs.Graph(ctx, tenantID, scheduleName)
}

// Summaries returns every schedule of tenantID's live release, by name.
func (s *ScheduleGraphs) Summaries(ctx context.Context, tenantID string) ([]topology.ScheduleSummary, error) {
	return s.graphs.Summaries(ctx, tenantID)
}
