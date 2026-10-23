// Package runsettle cancels, when state starts, every active run whose tasks
// state did not plan. Such a run's tasks record neither when they were
// dispatched nor which of them wait on which, so no status update can move it
// on; cancelling it ends it, or stops the work it has in flight (Run.Cancel),
// and schedule.cancelled:v1 tells execution-controller to suppress its Jobs.
package runsettle

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// Reason is the cancellation reason recorded on each run Settle cancels.
const Reason = "the run's tasks were not planned by state, so it cannot continue"

// Settle cancels every pending or running run whose tasks state did not plan,
// each in a transaction of its own, and returns how many it cancelled. A run
// that finished, or that state planned, between the list and its own
// transaction is left as it is.
func Settle(ctx context.Context, newUoW func() uow.UnitOfWork, logger *slog.Logger) (int, error) {
	ids, err := listUnplanned(ctx, newUoW())
	if err != nil {
		return 0, err
	}
	cancelled := 0
	for _, id := range ids {
		done, err := cancel(ctx, newUoW(), id)
		if err != nil {
			return cancelled, err
		}
		if done {
			cancelled++
			logger.Warn("Cancelled a run whose tasks state did not plan", "schedule_id", id, "reason", Reason)
		}
	}
	return cancelled, nil
}

func listUnplanned(ctx context.Context, u uow.UnitOfWork) ([]uuid.UUID, error) {
	if err := u.Begin(ctx); err != nil {
		return nil, fmt.Errorf("begin the list of unplanned runs: %w", err)
	}
	defer func() { _ = u.Rollback() }()
	ids, err := u.Run().ListUnplannedActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the active runs state did not plan: %w", err)
	}
	return ids, nil
}

// cancel cancels run id in u's transaction when it is still active and
// unplanned, and reports whether it did.
func cancel(ctx context.Context, u uow.UnitOfWork, id uuid.UUID) (bool, error) {
	if err := u.Begin(ctx); err != nil {
		return false, fmt.Errorf("begin the cancel of run %s: %w", id, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()
	r, err := u.Run().LoadRunForUpdate(ctx, id)
	if err != nil {
		return false, fmt.Errorf("load run %s: %w", id, err)
	}
	if !r.IsActive() || r.PlannedAt() != nil {
		return false, nil
	}
	events, err := r.Cancel(ctx, u.TaskCollection(), identity.SystemUserID, Reason, u.Clock().Now())
	if err != nil {
		return false, fmt.Errorf("cancel run %s: %w", id, err)
	}
	if err := u.Run().SaveRun(ctx, r); err != nil {
		return false, fmt.Errorf("save cancelled run %s: %w", id, err)
	}
	if err := u.Outbox().Append(ctx, events, uuid.Nil); err != nil {
		return false, fmt.Errorf("append the cancel of run %s: %w", id, err)
	}
	if err := u.Commit(); err != nil {
		return false, fmt.Errorf("commit the cancel of run %s: %w", id, err)
	}
	committed = true
	return true, nil
}
