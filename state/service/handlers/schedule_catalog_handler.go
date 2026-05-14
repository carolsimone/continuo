package handlers

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// ScheduleCatalogHandler reconciles state's schedule_catalog from a
// ScheduleCatalogLoaded event. The handler is pure orchestration: it takes
// a UnitOfWork and a typed event, and never parses JSON, manages a
// transaction, or runs dedup itself — those responsibilities live in the
// adapter binding that wires the parser, dedup, and UoW lifecycle around
// this call.
type ScheduleCatalogHandler struct {
	logger *slog.Logger
}

// NewScheduleCatalogHandler constructs a ScheduleCatalogHandler.
func NewScheduleCatalogHandler(logger *slog.Logger) *ScheduleCatalogHandler {
	return &ScheduleCatalogHandler{logger: logger}
}

// Handle reconciles schedule_catalog by upserting the current name set
// (with per-service metadata) and soft-deleting any rows whose schedule
// name is no longer present. msgProcID is unused because this reconciler
// produces no outbox entries; it is kept on the signature for consistency
// with the other state handlers.
func (h *ScheduleCatalogHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.ScheduleCatalogLoaded,
	_ uuid.UUID,
) error {
	repo := u.ScheduleCatalogRepo()
	if err := repo.UpsertAll(ctx, evt.ScheduleNames, evt.ServiceMetadata); err != nil {
		return err
	}
	if err := repo.SoftDeleteAbsent(ctx, evt.ScheduleNames); err != nil {
		return err
	}
	h.logger.Info("Schedule catalog reconciled",
		"event_id", evt.EventID,
		"schedule_count", len(evt.ScheduleNames),
	)
	return nil
}
