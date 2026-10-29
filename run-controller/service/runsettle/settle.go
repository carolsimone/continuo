// Package runsettle cancels, when state starts, every active run of every
// tenant whose tasks state did not plan. Such a run's tasks record neither
// when they were dispatched nor which of them wait on which, so no status
// update can move it on; cancelling it ends it, or stops the work it has in
// flight (Run.Cancel), and run.cancelled:v1 tells execution-controller to
// fence the run and stop its Jobs.
package runsettle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
)

// Reason is the cancellation reason recorded on each run Settle cancels.
const Reason = "the run's tasks were not planned by state, so it cannot continue"

// Settle cancels every pending or running run of every tenant whose tasks
// state did not plan, each in a transaction of its own under the run's
// tenant, and returns how many it cancelled. A run that finished, or that
// state planned, between the list and its own transaction is left as it is.
func Settle(ctx context.Context, newUoW func() uow.UnitOfWork, logger *slog.Logger) (int, error) {
	refs, err := listUnplanned(ctx, newUoW())
	if err != nil {
		return 0, err
	}
	cancelled := 0
	for _, ref := range refs {
		done, err := cancel(ctx, newUoW(), ref)
		if err != nil {
			return cancelled, err
		}
		if done {
			cancelled++
			logger.Warn("Cancelled a run whose tasks state did not plan", "tenant_id", ref.TenantID, "run_id", ref.RunID,
				"reason", Reason)
		}
	}
	return cancelled, nil
}

func listUnplanned(ctx context.Context, u uow.UnitOfWork) ([]repository.RunRef, error) {
	if err := u.Begin(ctx); err != nil {
		return nil, fmt.Errorf("begin the list of unplanned runs: %w", err)
	}
	defer func() { _ = u.Rollback() }()
	refs, err := u.Run().ListUnplannedActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the active runs state did not plan: %w", err)
	}
	return refs, nil
}

// cancel cancels the run ref names, under its tenant, in u's transaction when
// it is still active and unplanned, and reports whether it did. A run that is
// already cancelling has had its cancel written: it is left as it is.
func cancel(ctx context.Context, u uow.UnitOfWork, ref repository.RunRef) (bool, error) {
	id := ref.RunID
	if err := u.Begin(ctx); err != nil {
		return false, fmt.Errorf("begin the cancel of run %s: %w", id, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()
	r, err := u.Run().LoadRunForUpdate(ctx, ref.TenantID, id)
	if err != nil {
		return false, fmt.Errorf("load run %s of tenant %s: %w", id, ref.TenantID, err)
	}
	if !r.IsActive() || r.PlannedAt() != nil {
		return false, nil
	}
	events, err := r.Cancel(ctx, u.TaskCollection(r.TenantID()), identity.SystemUserID, Reason, u.Clock().Now())
	if errors.Is(err, run.ErrAlreadyCancelling) {
		return false, nil
	}
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
