package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/policy"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// TriggerRebaseHandler is the use case behind the TriggerRebase gRPC method.
// It enforces eligibility on the source Run via the aggregate and writes a
// new rebase Run atomically with its outbox event.
type TriggerRebaseHandler struct {
	policy policy.SchedulePolicy
	logger *slog.Logger
}

// NewTriggerRebaseHandler constructs the handler.
func NewTriggerRebaseHandler(logger *slog.Logger) *TriggerRebaseHandler {
	return &TriggerRebaseHandler{logger: logger}
}

// Handle synthesises a fresh rebase Run from a source. Returns (newRunID, scheduleName).
func (h *TriggerRebaseHandler) Handle(ctx context.Context, u uow.UnitOfWork, sourceID uuid.UUID) (uuid.UUID, string, error) {
	src, err := u.Run().GetRun(ctx, sourceID)
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("get source: %w", err)
	}
	if err := src.CanBeRebaseSource(ctx, u.TaskCollection()); err != nil {
		return uuid.Nil, "", err
	}
	if err := h.policy.IsScheduleAvailable(ctx, u.Run(), src.ScheduleName()); err != nil {
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

	newRun, evt, err := run.NewDerivedRun(src.ScheduleName(), run.KindRebase, sourceID, u.Clock().Now())
	if err != nil {
		return uuid.Nil, "", err
	}
	if err := u.Run().SaveRun(ctx, u.Tx(), newRun); err != nil {
		return uuid.Nil, "", fmt.Errorf("save: %w", err)
	}
	if err := u.Outbox().Append(ctx, u.Tx(), []run.DomainEvent{evt}, uuid.Nil); err != nil {
		return uuid.Nil, "", fmt.Errorf("outbox: %w", err)
	}
	if err := u.Commit(); err != nil {
		return uuid.Nil, "", fmt.Errorf("commit: %w", err)
	}
	committed = true

	h.logger.Info("Rebase triggered", "new_run_id", newRun.ScheduleID(), "schedule_name", newRun.ScheduleName(), "source_run_id", sourceID)
	return newRun.ScheduleID(), newRun.ScheduleName(), nil
}
