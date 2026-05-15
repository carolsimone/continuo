package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
)

// ScheduleCancelledHandler records cancelled schedule IDs in the local
// cancelled_schedules table. Used by all three orchestrator guards
// (orchestrator/executor/k8s) to short-circuit work for cancelled runs.
type ScheduleCancelledHandler struct {
	repo   repository.CancelledSchedulesRepository
	logger *slog.Logger
}

// NewScheduleCancelledHandler constructs the handler.
func NewScheduleCancelledHandler(repo repository.CancelledSchedulesRepository, logger *slog.Logger) *ScheduleCancelledHandler {
	return &ScheduleCancelledHandler{repo: repo, logger: logger}
}

// Handle records the cancellation. The cancelled_schedules table is
// idempotent (ON CONFLICT DO NOTHING) so replay is safe.
func (h *ScheduleCancelledHandler) Handle(ctx context.Context, evt domain.ScheduleCancelled) error {
	if err := h.repo.Insert(ctx, evt.ScheduleID); err != nil {
		return fmt.Errorf("insert cancelled schedule %s: %w", evt.ScheduleID, err)
	}
	h.logger.Info("Recorded cancelled schedule", "schedule_id", evt.ScheduleID)
	return nil
}
