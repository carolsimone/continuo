// Package runreads serves the run views people read: a run and its tasks and
// attempts, a run's graph, a schedule's completed runs, how far active runs
// are behind the live release, the node history and catalog, the schedules of
// the live release with their graphs, and what a rebase of a failed run would
// run on the live release. Each read names the tenant it reads and sees only
// that tenant's rows.
package runreads

import (
	"context"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/carolsimone/continuo/state/domain/repository"
)

// DefaultPageSize is the run-list page size when the caller sets none;
// MaxPageSize is the largest page served.
const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)

// Service reads the runs of the tenant each read is given; another tenant's
// rows are never read.
type Service struct {
	repo repository.RunReadRepository
}

// New returns a Service over repo.
func New(repo repository.RunReadRepository) *Service {
	return &Service{repo: repo}
}

// GetRunGraph returns the graph of tenantID's run runID.
func (s *Service) GetRunGraph(ctx context.Context, tenantID string, runID uuid.UUID) (projection.RunGraph, error) {
	return s.repo.GetRunGraph(ctx, tenantID, runID)
}

// ListRuns returns one page of the completed runs of tenantID's schedule. A
// page size below 1 becomes DefaultPageSize, one above MaxPageSize becomes
// MaxPageSize, and a negative offset becomes 0.
func (s *Service) ListRuns(ctx context.Context, tenantID, scheduleName string, pageSize, pageOffset int) ([]projection.RunSummary, int, error) {
	if pageSize < 1 {
		pageSize = DefaultPageSize
	}
	pageSize = min(pageSize, MaxPageSize)
	pageOffset = max(pageOffset, 0)
	return s.repo.ListCompletedRuns(ctx, tenantID, scheduleName, pageSize, pageOffset)
}

// ListActiveRunDrifts returns tenantID's live promotion seq and the newest
// active run of each of the tenant's schedules.
func (s *Service) ListActiveRunDrifts(ctx context.Context, tenantID string) (int64, []projection.ActiveRunDrift, error) {
	return s.repo.ListActiveRunDrifts(ctx, tenantID)
}
