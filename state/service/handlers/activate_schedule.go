package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/policy"
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

// ActivateScheduleHandler is the use case behind both gRPC ActivateSchedule
// (cron) and gRPC TriggerSchedule (manual). The only difference between the
// two callers is how they treat OutcomeSkippedActive: cron silently skips,
// manual returns FailedPrecondition.
type ActivateScheduleHandler struct {
	policy policy.SchedulePolicy
	logger *slog.Logger
}

// NewActivateScheduleHandler constructs the handler.
func NewActivateScheduleHandler(logger *slog.Logger) *ActivateScheduleHandler {
	return &ActivateScheduleHandler{logger: logger}
}

// Handle runs the activation use case. The catalog existence and active-run
// pre-conditions are enforced via SchedulePolicy. On success, a PENDING Run is
// minted, persisted, and RunStarted is emitted via the outbox.
//
// Returns (id, OutcomeActivated) on success, (uuid.Nil, OutcomeSkippedActive)
// when the schedule already has an active run, or an error otherwise.
func (h *ActivateScheduleHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	name string,
	kind run.Kind,
	sourceRunID *uuid.UUID,
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

	newRun, evt, err := run.NewPendingRun(name, kind, sourceRunID, metadata, u.Clock().Now())
	if err != nil {
		return uuid.Nil, 0, err
	}
	if err := u.Run().SaveRun(ctx, u.Tx(), newRun); err != nil {
		return uuid.Nil, 0, fmt.Errorf("save: %w", err)
	}
	if err := u.Outbox().Append(ctx, u.Tx(), []run.DomainEvent{evt}, uuid.Nil); err != nil {
		return uuid.Nil, 0, fmt.Errorf("outbox: %w", err)
	}
	if err := u.Commit(); err != nil {
		return uuid.Nil, 0, fmt.Errorf("commit: %w", err)
	}
	committed = true

	h.logger.Info("Schedule activated", "schedule_id", newRun.ScheduleID(), "schedule_name", name, "kind", string(kind))
	return newRun.ScheduleID(), OutcomeActivated, nil
}
