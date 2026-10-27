// Package runreads serves the run views people read: a run's graph, a
// schedule's completed runs, how far active runs are behind the live release,
// and what a rebase of a failed run would run on the live release.
package runreads

import (
	"context"

	"github.com/google/uuid"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/carolsimone/continuo/state/domain/repository"
)

// DefaultPageSize is the run-list page size when the caller sets none;
// MaxPageSize is the largest page served.
const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)

// Service reads runs for the default tenant.
type Service struct {
	repo repository.RunReadRepository
}

// New returns a Service over repo.
func New(repo repository.RunReadRepository) *Service {
	return &Service{repo: repo}
}

// GetRunGraph returns runID's graph.
func (s *Service) GetRunGraph(ctx context.Context, runID uuid.UUID) (projection.RunGraph, error) {
	return s.repo.GetRunGraph(ctx, pkgevents.DefaultTenantID, runID)
}

// ListRuns returns one page of a schedule's completed runs. A page size below
// 1 becomes DefaultPageSize, one above MaxPageSize becomes MaxPageSize, and a
// negative offset becomes 0.
func (s *Service) ListRuns(ctx context.Context, scheduleName string, pageSize, pageOffset int) ([]projection.RunSummary, int, error) {
	if pageSize < 1 {
		pageSize = DefaultPageSize
	}
	pageSize = min(pageSize, MaxPageSize)
	pageOffset = max(pageOffset, 0)
	return s.repo.ListCompletedRuns(ctx, pkgevents.DefaultTenantID, scheduleName, pageSize, pageOffset)
}

// ListActiveRunDrifts returns the live promotion seq and every schedule's
// newest active run.
func (s *Service) ListActiveRunDrifts(ctx context.Context) (int64, []projection.ActiveRunDrift, error) {
	return s.repo.ListActiveRunDrifts(ctx, pkgevents.DefaultTenantID)
}
