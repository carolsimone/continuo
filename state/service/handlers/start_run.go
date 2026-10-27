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
	OutcomeRunStarted    ActivationOutcome = iota
	OutcomeSkippedActive ActivationOutcome = iota
)

// StartRunHandler is the use case behind the cron fire, gRPC
// ActivateSchedule (both kind cron), and gRPC TriggerSchedule and StartRun
// (kind trigger).
// It creates the run, chooses its tasks from the schedule's graph on the live
// release and dispatches the ready ones, in one transaction.
//
// The kinds differ in two ways. On OutcomeSkippedActive cron silently skips
// and a trigger returns FailedPrecondition. When the selection is refused, a
// cron run is still recorded, finished failed with the refusal as its reason,
// while a trigger creates no run and returns the refusal; a test run whose
// schedule has no node with data tests is recorded skipped for both.
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
// SourceRunID names the run it derives from, if any.
type StartRunInput struct {
	RunID        uuid.UUID
	TenantID     string
	ScheduleName string
	Kind         run.Kind
	SourceRunID  *uuid.UUID
	Initiator    identity.Identity
	Operation    model.Operation
}

// Handle starts a run of tenantID's named schedule, for that tenant, in a
// transaction of its own. The catalog existence and active-run pre-conditions
// are enforced via SchedulePolicy.
//
// Returns (id, OutcomeRunStarted) when a run was created, (uuid.Nil,
// OutcomeSkippedActive) when the schedule already has an active run, or an
// error otherwise: a refused trigger's runplan refusal, or an infrastructure
// failure, after which nothing is written.
//
// operation selects the dbt verb (run/test/build) the run applies to its
// nodes. Only a manual trigger passes a non-default value; every other caller
// (cron, etc.) passes model.OperationRun.
func (h *StartRunHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	tenantID string,
	name string,
	kind run.Kind,
	sourceRunID *uuid.UUID,
	initiator identity.Identity,
	operation model.Operation,
) (uuid.UUID, ActivationOutcome, error) {
	in := StartRunInput{
		RunID: uuid.New(), TenantID: tenantID, ScheduleName: name, Kind: kind,
		SourceRunID: sourceRunID, Initiator: initiator, Operation: operation,
	}
	metadata, outcome, err := h.check(ctx, u, in)
	if err != nil || outcome == OutcomeSkippedActive {
		return uuid.Nil, outcome, err
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
	outcome, err = h.create(ctx, u, in, metadata)
	if err != nil || outcome == OutcomeSkippedActive {
		return uuid.Nil, outcome, err
	}
	if err := u.Commit(); err != nil {
		return uuid.Nil, 0, fmt.Errorf("commit: %w", err)
	}
	committed = true
	return in.RunID, OutcomeRunStarted, nil
}

// StartInTransaction starts the run in.RunID inside u's open transaction,
// which the caller commits or rolls back. It enforces the pre-conditions
// Handle does; a schedule that already has an active run is
// run.ErrScheduleHasActiveRun. A refusal leaves the caller to roll back.
func (h *StartRunHandler) StartInTransaction(ctx context.Context, u uow.UnitOfWork, in StartRunInput) error {
	metadata, outcome, err := h.check(ctx, u, in)
	if err != nil {
		return err
	}
	if outcome == OutcomeSkippedActive {
		return run.ErrScheduleHasActiveRun
	}
	outcome, err = h.create(ctx, u, in, metadata)
	if err != nil {
		return err
	}
	if outcome == OutcomeSkippedActive {
		return run.ErrScheduleHasActiveRun
	}
	return nil
}

// check enforces the schedule's pre-conditions, the schedule in the catalog
// and no active run, and returns the catalog's service metadata of the
// schedule. A schedule with an active run is OutcomeSkippedActive.
func (h *StartRunHandler) check(ctx context.Context, u uow.UnitOfWork, in StartRunInput) (map[string]run.ServiceMetadata, ActivationOutcome, error) {
	if err := h.policy.ScheduleExistsInCatalog(ctx, u.Catalog(), in.TenantID, in.ScheduleName); err != nil {
		return nil, 0, err
	}
	if err := h.policy.EnsureNoActiveRun(ctx, u.Run(), in.TenantID, in.ScheduleName); err != nil {
		if errors.Is(err, run.ErrScheduleHasActiveRun) {
			return nil, OutcomeSkippedActive, nil
		}
		return nil, 0, err
	}
	metadata, err := u.Catalog().GetServiceMetadata(ctx, in.TenantID, in.ScheduleName)
	if err != nil {
		return nil, 0, fmt.Errorf("get metadata: %w", err)
	}
	return metadata, OutcomeRunStarted, nil
}

// create chooses the run's tasks from the schedule's graph on the live
// release, creates the run in.RunID and dispatches its ready tasks, inside u's
// open transaction. A refused selection of a trigger is returned and creates
// nothing; a refused cron selection records the run finished. A run that
// loses the race for the schedule's one active run is OutcomeSkippedActive.
func (h *StartRunHandler) create(ctx context.Context, u uow.UnitOfWork, in StartRunInput, metadata map[string]run.ServiceMetadata) (ActivationOutcome, error) {
	name := in.ScheduleName
	sel, err := h.planner.Schedule(ctx, u, in.TenantID, name, in.Operation)
	refusal := scheduleRefusal(err)
	if err != nil && refusal == nil {
		return 0, fmt.Errorf("select the run of %s: %w", name, err)
	}
	benign := errors.Is(refusal, runplan.ErrNoTests)
	if refusal != nil && in.Kind != run.KindCron && !benign {
		return 0, refusal
	}

	now := u.Clock().Now()
	newRun, err := run.NewPendingRun(in.TenantID, in.RunID, name, in.Kind, in.SourceRunID, in.Initiator.UserID, metadata, in.Operation, sel.Stamp, now)
	if err != nil {
		return 0, err
	}
	if err := u.Run().SaveRun(ctx, newRun); err != nil {
		// A concurrent activation that passed the EnsureNoActiveRun pre-check
		// loses the partial-unique-index race on insert. Treat it identically to
		// the check-first active-run outcome so the gRPC caller maps it to the
		// same response (cron skip / manual FailedPrecondition).
		if errors.Is(err, run.ErrScheduleHasActiveRun) {
			return OutcomeSkippedActive, nil
		}
		return 0, fmt.Errorf("save: %w", err)
	}
	if refusal != nil {
		if err := h.finishRefused(ctx, u, newRun, benign, refusal, now); err != nil {
			return 0, err
		}
	} else if err := h.planner.Start(ctx, u, newRun, sel, uuid.Nil); err != nil {
		return 0, err
	}

	u.AfterCommit(func() {
		h.logger.Info("Schedule activated", "tenant_id", in.TenantID, "run_id", newRun.RunID(), "schedule_name", name, "kind", string(in.Kind),
			"release_id", sel.Stamp.ReleaseID, "tasks", len(sel.Plan.Tasks), "dispatched", len(sel.Plan.Ready))
	})
	return OutcomeRunStarted, nil
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
