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
// in the caller's transaction. A rerun and a rebase differ in the run kind,
// the eligibility check, the selection and the log label.
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

// createInTransaction creates the run runID derived from tenantID's run
// sourceID with the tasks selectRun chooses, under the source's tenant,
// inside u's open transaction, which the caller commits or rolls back, and
// returns the new run's schedule name. Another tenant's sourceID is not
// found. A selection the planner refuses is returned as is.
func (c derivedRunCreation) createInTransaction(ctx context.Context, u uow.UnitOfWork, runID uuid.UUID, tenantID string, sourceID uuid.UUID, initiator identity.Identity, selectRun selectDerived) (string, error) {
	src, err := c.source(ctx, u, tenantID, sourceID)
	if err != nil {
		return "", err
	}
	if err := c.write(ctx, u, runID, src, initiator, selectRun); err != nil {
		return "", err
	}
	return src.ScheduleName(), nil
}

// source loads tenantID's run sourceID and refuses it unless it may be the
// source of the kind and its schedule has no active run.
func (c derivedRunCreation) source(ctx context.Context, u uow.UnitOfWork, tenantID string, sourceID uuid.UUID) (*run.Run, error) {
	src, err := u.Run().GetRun(ctx, tenantID, sourceID)
	if err != nil {
		return nil, fmt.Errorf("get source: %w", err)
	}
	if err := c.eligible(ctx, src, u.TaskCollection(src.TenantID())); err != nil {
		return nil, err
	}
	if err := c.policy.EnsureNoActiveRun(ctx, u.Run(), src.TenantID(), src.ScheduleName()); err != nil {
		return nil, err
	}
	return src, nil
}

// write creates the run runID derived from src with the tasks selectRun
// chooses, stores its tasks and dispatches the ready ones, inside u's open
// transaction.
func (c derivedRunCreation) write(ctx context.Context, u uow.UnitOfWork, runID uuid.UUID, src *run.Run, initiator identity.Identity, selectRun selectDerived) error {
	sel, err := selectRun(ctx, u, src)
	if err != nil {
		return err
	}
	newRun, err := run.NewDerivedRun(src.TenantID(), runID, src.ScheduleName(), c.kind, src.RunID(), sel.Stamp, initiator.UserID, src.Operation(), u.Clock().Now())
	if err != nil {
		return err
	}
	if err := u.Run().SaveRun(ctx, newRun); err != nil {
		return fmt.Errorf("save: %w", err)
	}
	if err := c.planner.Start(ctx, u, newRun, sel, uuid.Nil); err != nil {
		return err
	}
	u.AfterCommit(func() {
		c.logger.Info(c.logLabel, "tenant_id", newRun.TenantID(), "new_run_id", newRun.RunID(), "schedule_name", newRun.ScheduleName(), "source_run_id", src.RunID(),
			"release_id", sel.Stamp.ReleaseID, "tasks", len(sel.Plan.Tasks), "dispatched", len(sel.Plan.Ready))
	})
	return nil
}
