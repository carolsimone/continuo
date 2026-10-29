// Package policy hosts cross-aggregate Domain Services. These exist when a
// rule does not naturally fit on any single aggregate. SchedulePolicy answers
// pre-condition questions about a schedule name, for a run of it or for its
// first cron, that span the Run and Schedule aggregates and the live release.
package policy

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/schedule"
)

// SchedulePolicy is stateless. Instantiate per use case or reuse a value;
// either works.
type SchedulePolicy struct{}

// RunRepo is the narrow contract SchedulePolicy needs from the run repository.
// The same interface is satisfied by repository.RunRepository in production code.
type RunRepo interface {
	HasActiveRun(ctx context.Context, tenantID, name string) (bool, error)
}

// LiveModels is the narrow contract SchedulePolicy needs from the live
// release's models. Satisfied by repository.LiveScheduleModels in production.
type LiveModels interface {
	CountOf(ctx context.Context, tenantID, scheduleName string) (int, error)
}

// ScheduleRows is the narrow contract SchedulePolicy needs from the schedule
// repository. Satisfied by repository.ScheduleRepository in production.
type ScheduleRows interface {
	Exists(ctx context.Context, tenantID, name string) (bool, error)
}

// EnsureNoActiveRun returns run.ErrScheduleHasActiveRun when the tenant
// already has a PENDING, RUNNING or CANCELLING run on `name`.
func (SchedulePolicy) EnsureNoActiveRun(ctx context.Context, repo RunRepo, tenantID, name string) error {
	active, err := repo.HasActiveRun(ctx, tenantID, name)
	if err != nil {
		return err
	}
	if active {
		return run.ErrScheduleHasActiveRun
	}
	return nil
}

// EnsureScheduleKnown returns run.ErrScheduleNotFound, naming the schedule,
// unless the tenant knows the schedule: a model of the tenant's live release
// carries its name, or the tenant has a schedule of that name. A schedule only
// its row knows has no models to run, which the run's selection then refuses.
func (SchedulePolicy) EnsureScheduleKnown(ctx context.Context, models LiveModels, rows ScheduleRows, tenantID, name string) error {
	n, err := models.CountOf(ctx, tenantID, name)
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	exists, err := rows.Exists(ctx, tenantID, name)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: no model of the live release carries %q and it has no cron", run.ErrScheduleNotFound, name)
	}
	return nil
}

// EnsureNameCarried returns schedule.ErrUnknownName, naming the schedule,
// unless a model of the tenant's live release carries the name: a schedule
// name gets its first cron only when the live release has models for it to
// run. A name that already has a cron is not asked about, so it keeps its cron
// whatever later releases hold.
func (SchedulePolicy) EnsureNameCarried(ctx context.Context, models LiveModels, tenantID, name string) error {
	n, err := models.CountOf(ctx, tenantID, name)
	if err != nil {
		return fmt.Errorf("count the live release's models of schedule %q: %w", name, err)
	}
	if n == 0 {
		return fmt.Errorf("schedule %q: %w", name, schedule.ErrUnknownName)
	}
	return nil
}
