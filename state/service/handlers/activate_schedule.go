package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/policy"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// ActivationOutcome distinguishes "new run created" from "active run already
// exists". The gRPC adapter translates these to the appropriate response /
// FailedPrecondition; the cron caller treats OutcomeSkippedActive as a silent
// skip.
type ActivationOutcome int

const (
	OutcomeActivated     ActivationOutcome = iota
	OutcomeSkippedActive ActivationOutcome = iota
)

// ActivateScheduleHandler is the use case behind the cron fire, gRPC
// ActivateSchedule (both kind cron) and gRPC TriggerSchedule (kind trigger).
// It creates the run, chooses its tasks from the schedule's graph on the live
// release and dispatches the ready ones, in one transaction.
//
// The kinds differ in two ways. On OutcomeSkippedActive cron silently skips
// and a trigger returns FailedPrecondition. When the selection is refused, a
// cron run is still recorded, finished failed with the refusal as its reason,
// while a trigger creates no run and returns the refusal; a test run whose
// schedule has no node with data tests is recorded skipped for both.
type ActivateScheduleHandler struct {
	policy  policy.SchedulePolicy
	planner runstart.Planner
	logger  *slog.Logger
}

// NewActivateScheduleHandler constructs the handler; planner plans its runs.
func NewActivateScheduleHandler(planner runstart.Planner, logger *slog.Logger) *ActivateScheduleHandler {
	return &ActivateScheduleHandler{planner: planner, logger: logger}
}

// Handle runs the activation use case. The catalog existence and active-run
// pre-conditions are enforced via SchedulePolicy.
//
// Returns (id, OutcomeActivated) when a run was created, (uuid.Nil,
// OutcomeSkippedActive) when the schedule already has an active run, or an
// error otherwise: a refused trigger's runplan refusal, or an infrastructure
// failure, after which nothing is written.
//
// operation selects the dbt verb (run/test/build) the run applies to its
// nodes. Only the manual TriggerSchedule caller passes a non-default value;
// every other caller (cron, etc.) passes model.OperationRun.
func (h *ActivateScheduleHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	name string,
	kind run.Kind,
	sourceRunID *uuid.UUID,
	initiator identity.Identity,
	operation model.Operation,
) (uuid.UUID, ActivationOutcome, error) {
	if err := h.policy.ScheduleExistsInCatalog(ctx, u.Catalog(), name); err != nil {
		return uuid.Nil, 0, err
	}
	if err := h.policy.IsScheduleAvailable(ctx, u.Run(), name); err != nil {
		if errors.Is(err, run.ErrScheduleHasActiveRun) {
			return uuid.Nil, OutcomeSkippedActive, nil
		}
		return uuid.Nil, 0, err
	}
	metadata, err := u.Catalog().GetServiceMetadata(ctx, name)
	if err != nil {
		return uuid.Nil, 0, fmt.Errorf("get metadata: %w", err)
	}

	if err := u.Begin(ctx); err != nil {
		return uuid.Nil, 0, fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()

	sel, err := h.planner.Schedule(ctx, u, name, operation)
	refusal := scheduleRefusal(err)
	if err != nil && refusal == nil {
		return uuid.Nil, 0, fmt.Errorf("select the run of %s: %w", name, err)
	}
	benign := errors.Is(refusal, runplan.ErrNoTests)
	if refusal != nil && kind != run.KindCron && !benign {
		return uuid.Nil, 0, refusal
	}

	now := u.Clock().Now()
	newRun, err := run.NewPendingRun(name, kind, sourceRunID, initiator.UserID, metadata, operation, sel.Stamp, now)
	if err != nil {
		return uuid.Nil, 0, err
	}
	if err := u.Run().SaveRun(ctx, newRun); err != nil {
		// A concurrent activation that passed the IsScheduleAvailable pre-check
		// loses the partial-unique-index race on insert. Treat it identically to
		// the check-first active-run outcome so the gRPC caller maps it to the
		// same response (cron skip / manual FailedPrecondition).
		if errors.Is(err, run.ErrScheduleHasActiveRun) {
			return uuid.Nil, OutcomeSkippedActive, nil
		}
		return uuid.Nil, 0, fmt.Errorf("save: %w", err)
	}
	if refusal != nil {
		if err := h.finishRefused(ctx, u, newRun, benign, refusal, now); err != nil {
			return uuid.Nil, 0, err
		}
	} else if err := h.planner.Start(ctx, u, newRun, sel, uuid.Nil); err != nil {
		return uuid.Nil, 0, err
	}
	if err := u.Commit(); err != nil {
		return uuid.Nil, 0, fmt.Errorf("commit: %w", err)
	}
	committed = true

	h.logger.Info("Schedule activated", "schedule_id", newRun.ScheduleID(), "schedule_name", name, "kind", string(kind),
		"release_id", sel.Stamp.ReleaseID, "tasks", len(sel.Plan.Tasks), "dispatched", len(sel.Plan.Ready))
	return newRun.ScheduleID(), OutcomeActivated, nil
}

// finishRefused finishes r, whose selection was refused, without tasks:
// skipped when the refusal is benign, failed otherwise with the run failure
// reason the refusal names, and with the refusal as the reason it logs.
func (h *ActivateScheduleHandler) finishRefused(ctx context.Context, u uow.UnitOfWork, r *run.Run, benign bool, refusal error, now time.Time) error {
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
	logDispatchTerminal(h.logger, "activate", r.ScheduleID(), events)
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
