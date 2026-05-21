// Package policy hosts cross-aggregate Domain Services. These exist when a
// rule does not naturally fit on any single aggregate. SchedulePolicy answers
// pre-condition questions about a run's schedule that span the Run and
// ScheduleCatalog aggregates.
package policy

import (
	"context"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
)

// SchedulePolicy is stateless. Instantiate per use case or reuse a value;
// either works.
type SchedulePolicy struct{}

// RunRepo is the narrow contract SchedulePolicy needs from the run repository.
// The same interface is satisfied by repository.RunRepository in production code.
type RunRepo interface {
	HasActiveSchedule(ctx context.Context, name string) (bool, error)
}

// CatalogRepo is the narrow contract SchedulePolicy needs from the catalog
// repository. Satisfied by repository.ScheduleCatalogRepository in production.
type CatalogRepo interface {
	ExistsActive(ctx context.Context, name string) (bool, error)
}

// IsScheduleAvailable returns run.ErrScheduleHasActiveRun when a PENDING or
// RUNNING run already exists on `name`.
func (SchedulePolicy) IsScheduleAvailable(ctx context.Context, repo RunRepo, name string) error {
	active, err := repo.HasActiveSchedule(ctx, name)
	if err != nil {
		return err
	}
	if active {
		return run.ErrScheduleHasActiveRun
	}
	return nil
}

// ScheduleExistsInCatalog returns run.ErrScheduleNotInCatalog when the named
// schedule has no active row in schedule_catalog.
func (SchedulePolicy) ScheduleExistsInCatalog(ctx context.Context, cat CatalogRepo, name string) error {
	exists, err := cat.ExistsActive(ctx, name)
	if err != nil {
		return err
	}
	if !exists {
		return run.ErrScheduleNotInCatalog
	}
	return nil
}
