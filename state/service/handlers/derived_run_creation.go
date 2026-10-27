package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/policy"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// derivedRunCreation creates a run derived from a source run: it enforces the
// source's eligibility through the aggregate and the schedule's active-run
// policy, then creates the run, stores its tasks and dispatches the ready ones
// in one transaction. A rerun and a rebase differ in the run kind, the
// eligibility check, the selection and the log label.
type derivedRunCreation struct {
	policy  policy.SchedulePolicy
	planner runstart.Planner
	logger  *slog.Logger
	// kind is the derived-run kind NewDerivedRun mints (KindRerun or
	// KindRebase).
	kind run.Kind
	// eligible runs the source aggregate's eligibility check
	// (CanBeRerunSource or CanBeRebaseSource); it returns the kind's own
	// "nothing to do" error so the gRPC adapter can tell the two apart.
	eligible func(ctx context.Context, src *run.Run, tasks run.TaskCollection) error
	// logLabel is the message of the success log line.
	logLabel string
}

// selectDerived chooses a derived run's tasks and the release it runs on,
// inside the creating transaction.
type selectDerived func(ctx context.Context, u uow.UnitOfWork, src *run.Run) (runstart.Selection, error)

// create creates the run derived from sourceID with the tasks selectRun
// chooses, and returns its id and schedule name. A selection the planner
// refuses is returned as is, and nothing is written.
func (c derivedRunCreation) create(ctx context.Context, u uow.UnitOfWork, sourceID uuid.UUID, initiator identity.Identity, selectRun selectDerived) (uuid.UUID, string, error) {
	src, err := u.Run().GetRun(ctx, c.planner.TenantID, sourceID)
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("get source: %w", err)
	}
	if err := c.eligible(ctx, src, u.TaskCollection(c.planner.TenantID)); err != nil {
		return uuid.Nil, "", err
	}
	if err := c.policy.EnsureNoActiveRun(ctx, u.Run(), src.TenantID(), src.ScheduleName()); err != nil {
		return uuid.Nil, "", err
	}

	if err := u.Begin(ctx); err != nil {
		return uuid.Nil, "", fmt.Errorf("begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()

	sel, err := selectRun(ctx, u, src)
	if err != nil {
		return uuid.Nil, "", err
	}
	newRun, err := run.NewDerivedRun(src.TenantID(), src.ScheduleName(), c.kind, sourceID, sel.Stamp, initiator.UserID, src.Operation(), u.Clock().Now())
	if err != nil {
		return uuid.Nil, "", err
	}
	if err := u.Run().SaveRun(ctx, newRun); err != nil {
		return uuid.Nil, "", fmt.Errorf("save: %w", err)
	}
	if err := c.planner.Start(ctx, u, newRun, sel, uuid.Nil); err != nil {
		return uuid.Nil, "", err
	}
	if err := u.Commit(); err != nil {
		return uuid.Nil, "", fmt.Errorf("commit: %w", err)
	}
	committed = true

	c.logger.Info(c.logLabel, "new_run_id", newRun.RunID(), "schedule_name", newRun.ScheduleName(), "source_run_id", sourceID,
		"release_id", sel.Stamp.ReleaseID, "tasks", len(sel.Plan.Tasks), "dispatched", len(sel.Plan.Ready))
	return newRun.RunID(), newRun.ScheduleName(), nil
}
