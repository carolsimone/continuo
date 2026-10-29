package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/policy"
	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	"github.com/carolsimone/continuo/run-controller/service/runstart"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
)

// ActivationOutcome says what a start of a schedule's run did.
type ActivationOutcome int

const (
	// OutcomeRunStarted: the run was created, planned and its ready tasks
	// dispatched.
	OutcomeRunStarted ActivationOutcome = iota
	// OutcomeSkippedActive: the schedule already has an active run; nothing
	// was written.
	OutcomeSkippedActive
	// OutcomeSlotTaken: the slot a cron fire is for already has its run;
	// nothing was written.
	OutcomeSlotTaken
	// OutcomeRunRefused: the run was recorded finished, without tasks,
	// because the live release gives its selection nothing it can run.
	OutcomeRunRefused
)

// StartRunHandler is the use case behind the cron fire (kind cron) and gRPC
// StartRun (kind trigger).
// It creates the run, chooses its tasks from the schedule's graph on the live
// release and dispatches the ready ones, in the caller's transaction.
//
// The kinds differ in three ways. A trigger needs a schedule the tenant knows,
// one a model of its live release carries or that has a cron, while a cron
// fire needs nothing beyond the schedule row that fired it. On an active run a
// cron fire is OutcomeSkippedActive and a trigger is
// run.ErrScheduleHasActiveRun. When the selection is refused, a cron run is
// still recorded, finished failed with the refusal as its reason, while a
// trigger creates no run and returns the refusal; a test run whose schedule
// has no node with data tests is recorded skipped for both.
type StartRunHandler struct {
	policy  policy.SchedulePolicy
	planner runstart.Planner
	logger  *slog.Logger
}

// NewStartRunHandler constructs the handler; planner plans its runs.
func NewStartRunHandler(planner runstart.Planner, logger *slog.Logger) *StartRunHandler {
	return &StartRunHandler{planner: planner, logger: logger}
}

// StartRunInput is a request to start the run RunID of TenantID's schedule
// ScheduleName, of Kind, applying Operation to its nodes, for Initiator.
// SourceRunID names the run it derives from, if any; ScheduledFor is the slot
// of the schedule a cron run is fired for, and nil for every other run.
type StartRunInput struct {
	RunID        uuid.UUID
	TenantID     string
	ScheduleName string
	Kind         run.Kind
	SourceRunID  *uuid.UUID
	Initiator    identity.Identity
	Operation    model.Operation
	ScheduledFor *time.Time
}

// CronFireResult is what a cron fire did.
type CronFireResult struct {
	// Outcome is what starting the slot's run did.
	Outcome ActivationOutcome
	// RunID is the run the fire created; uuid.Nil when it created none.
	RunID uuid.UUID
	// FailureReason is the reason a refused run was recorded failed with:
	// empty_selection or invalid_node_type for OutcomeRunRefused, and empty
	// for every other outcome.
	FailureReason run.RunFailureReason
}

// HandleCronFire fires the slot of tenantID's schedule name inside u's open
// transaction, which the caller commits or rolls back. It creates the cron run
// of the slot, stamped with it (scheduled_for), initiated by the system, of
// operation run, on the tenant's live release. The schedule's row is what
// authorises the fire, so the fire does not ask whether a model carries the
// name: a schedule no model carries records a run refused as an empty
// selection.
//
// Its result is OutcomeRunStarted with the run for a run created and
// dispatched; OutcomeRunRefused with the run and its failure reason for a run
// recorded failed because the live release gives the schedule nothing it can
// run (empty_selection) or a node of a type the node_type vocabulary does not
// declare (invalid_node_type); OutcomeSkippedActive when the schedule already
// has an active run; and OutcomeSlotTaken when the slot already has its run.
// The two skips write nothing and leave the transaction usable. A run another
// caller started between the active-run check and the insert ends the
// transaction: that is an error wrapping run.ErrScheduleHasActiveRun, and the
// caller rolls back. A schedule name longer than a run carries, which only a
// row written outside the schedule commands can hold, is
// run.ErrScheduleNameTooLong from the run's constructor, and the caller rolls
// back.
func (h *StartRunHandler) HandleCronFire(ctx context.Context, u uow.UnitOfWork, tenantID, name string, slot time.Time) (CronFireResult, error) {
	in := StartRunInput{
		RunID: uuid.New(), TenantID: tenantID, ScheduleName: name, Kind: run.KindCron,
		Initiator: identity.System(), Operation: model.OperationRun, ScheduledFor: &slot,
	}
	if err := h.policy.EnsureNoActiveRun(ctx, u.Run(), tenantID, name); err != nil {
		if errors.Is(err, run.ErrScheduleHasActiveRun) {
			return CronFireResult{Outcome: OutcomeSkippedActive}, nil
		}
		return CronFireResult{}, fmt.Errorf("check the active run of %s: %w", name, err)
	}
	outcome, failure, err := h.create(ctx, u, in)
	switch {
	case err != nil:
		return CronFireResult{}, err
	case outcome == OutcomeSkippedActive:
		return CronFireResult{}, fmt.Errorf("fire the %s slot of %s: %w", slot.UTC().Format(time.RFC3339), name, run.ErrScheduleHasActiveRun)
	case outcome == OutcomeSlotTaken:
		return CronFireResult{Outcome: OutcomeSlotTaken}, nil
	}
	return CronFireResult{Outcome: outcome, RunID: in.RunID, FailureReason: failure}, nil
}

// StartInTransaction starts the run in.RunID inside u's open transaction,
// which the caller commits or rolls back. A schedule name longer than a run
// carries is run.ErrScheduleNameTooLong, whatever the tenant knows of it. The
// tenant must know the schedule: a model of its live release carries the
// name, or the name has a cron (run.ErrScheduleNotFound otherwise). A schedule
// that already has an active run is run.ErrScheduleHasActiveRun, and a
// schedule with a cron that no model of the live release carries is the
// selection's refusal, runplan.ErrEmptySelection. A refusal writes nothing and
// leaves the caller to roll back.
func (h *StartRunHandler) StartInTransaction(ctx context.Context, u uow.UnitOfWork, in StartRunInput) error {
	outcome, err := h.check(ctx, u, in)
	if err != nil {
		return err
	}
	if outcome == OutcomeSkippedActive {
		return run.ErrScheduleHasActiveRun
	}
	outcome, _, err = h.create(ctx, u, in)
	if err != nil {
		return err
	}
	if outcome == OutcomeSkippedActive {
		return run.ErrScheduleHasActiveRun
	}
	return nil
}

// check enforces a trigger's pre-conditions, read in u's transaction before
// the run is written: a schedule name a run can carry, a schedule the tenant
// knows, and no active run of it. A schedule with an active run is
// OutcomeSkippedActive.
//
// The name is checked before anything is read, so a name no run can carry is
// refused alike whether or not the tenant knows it. The topology lock comes
// next, which the transaction then holds, so the live release whose models it
// counts is the one the run is created on: a promotion that has not committed
// is waited for, and one that commits after the lock is taken waits for the
// run.
func (h *StartRunHandler) check(ctx context.Context, u uow.UnitOfWork, in StartRunInput) (ActivationOutcome, error) {
	if err := run.ValidateScheduleName(in.ScheduleName); err != nil {
		return 0, err
	}
	if err := u.Topology().LockTopology(ctx); err != nil {
		return 0, fmt.Errorf("lock the topology for the schedule check: %w", err)
	}
	if err := h.policy.EnsureScheduleKnown(ctx, u.LiveScheduleModels(), u.Schedules(), in.TenantID, in.ScheduleName); err != nil {
		return 0, err
	}
	if err := h.policy.EnsureNoActiveRun(ctx, u.Run(), in.TenantID, in.ScheduleName); err != nil {
		if errors.Is(err, run.ErrScheduleHasActiveRun) {
			return OutcomeSkippedActive, nil
		}
		return 0, err
	}
	return OutcomeRunStarted, nil
}

// create chooses the run's tasks from the schedule's graph on the live
// release, creates the run in.RunID, stamped with in.ScheduledFor when it has
// one, and dispatches its ready tasks, inside u's open transaction. A refused
// selection of a trigger is returned and creates nothing; a refused cron
// selection, and a benign refusal of either kind, records the run finished and
// is OutcomeRunRefused, with the failure reason the run was recorded with
// (empty for a benign refusal, which records the run skipped). A run that
// loses the race for the schedule's one active run is OutcomeSkippedActive,
// after which the transaction cannot go on; a cron run whose slot already has
// its run is OutcomeSlotTaken, and writes nothing.
func (h *StartRunHandler) create(ctx context.Context, u uow.UnitOfWork, in StartRunInput) (ActivationOutcome, run.RunFailureReason, error) {
	name := in.ScheduleName
	sel, err := h.planner.Schedule(ctx, u, in.TenantID, name, in.Operation)
	refusal := scheduleRefusal(err)
	if err != nil && refusal == nil {
		return 0, "", fmt.Errorf("select the run of %s: %w", name, err)
	}
	benign := errors.Is(refusal, runplan.ErrNoTests)
	if refusal != nil && in.Kind != run.KindCron && !benign {
		return 0, "", refusal
	}

	now := u.Clock().Now()
	newRun, err := run.NewPendingRun(in.TenantID, in.RunID, name, in.Kind, in.SourceRunID, in.Initiator.UserID, in.Operation, sel.Stamp, now)
	if err != nil {
		return 0, "", err
	}
	if in.ScheduledFor != nil {
		if err := newRun.SetScheduledFor(*in.ScheduledFor); err != nil {
			return 0, "", err
		}
	}
	if err := u.Run().SaveRun(ctx, newRun); err != nil {
		// A concurrent start that passed the EnsureNoActiveRun pre-check too
		// wins the insert on the schedule's one-active-run index, which ends
		// the transaction. A trigger answers it as it answers the pre-check
		// (FailedPrecondition); a cron fire rolls back.
		if errors.Is(err, run.ErrScheduleHasActiveRun) {
			return OutcomeSkippedActive, "", nil
		}
		if errors.Is(err, run.ErrSlotTaken) {
			return OutcomeSlotTaken, "", nil
		}
		return 0, "", fmt.Errorf("save: %w", err)
	}
	outcome := OutcomeRunStarted
	var failure run.RunFailureReason
	if refusal != nil {
		if err := h.finishRefused(ctx, u, newRun, benign, refusal, now); err != nil {
			return 0, "", err
		}
		outcome = OutcomeRunRefused
		if recorded := newRun.FailureReason(); recorded != nil {
			failure = *recorded
		}
	} else if err := h.planner.Start(ctx, u, newRun, sel, uuid.Nil); err != nil {
		return 0, "", err
	}

	u.AfterCommit(func() {
		h.logger.Info("Schedule activated", "tenant_id", in.TenantID, "run_id", newRun.RunID(), "schedule_name", name, "kind", string(in.Kind),
			"release_id", sel.Stamp.ReleaseID, "tasks", len(sel.Plan.Tasks), "dispatched", len(sel.Plan.Ready))
	})
	return outcome, failure, nil
}

// finishRefused finishes r, whose selection was refused, without tasks:
// skipped when the refusal is benign, failed otherwise with the run failure
// reason the refusal names, and with the refusal as the reason it logs.
func (h *StartRunHandler) finishRefused(ctx context.Context, u uow.UnitOfWork, r *run.Run, benign bool, refusal error, now time.Time) error {
	events, err := r.MarkDispatchTerminal(benign, refusalFailureReason(refusal), refusal.Error(), now)
	if err != nil {
		return err
	}
	if err := u.Run().SaveRun(ctx, r); err != nil {
		return fmt.Errorf("save the refused run: %w", err)
	}
	if err := u.Outbox().Append(ctx, events, uuid.Nil); err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	logDispatchTerminal(h.logger, "activate", r.RunID(), events)
	return nil
}

// refusalFailureReason is the run failure reason of a schedule refusal that
// fails its run.
func refusalFailureReason(refusal error) run.RunFailureReason {
	if errors.Is(refusal, runplan.ErrInvalidNodeType) {
		return run.RunFailureInvalidNodeType
	}
	return run.RunFailureEmptySelection
}

// scheduleRefusal returns err when it is a refusal a schedule's selection can
// end in, and nil otherwise.
func scheduleRefusal(err error) error {
	for _, refusal := range []error{runplan.ErrEmptySelection, runplan.ErrNoTests, runplan.ErrInvalidNodeType} {
		if errors.Is(err, refusal) {
			return err
		}
	}
	return nil
}
