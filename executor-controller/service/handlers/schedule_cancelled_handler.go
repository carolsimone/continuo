// executor-controller/service/handlers/schedule_cancelled_handler.go
package handlers

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// ScheduleCancelledHandler inserts a row into cancelled_schedules so the
// deploy bindings can drop subsequent query.model / retry.task messages
// whose schedule_id matches. Operation is idempotent (UPSERT semantics),
// so the binding does not need messageprocessing dedup.
type ScheduleCancelledHandler struct {
	logger *slog.Logger
}

// NewScheduleCancelledHandler constructs the handler.
func NewScheduleCancelledHandler(logger *slog.Logger) *ScheduleCancelledHandler {
	return &ScheduleCancelledHandler{logger: logger}
}

// Handle inserts the cancelled schedule. The msgProcID parameter is
// accepted to keep handler signatures uniform across streams but is
// not used here (no dedup row exists for schedule.cancelled).
func (h *ScheduleCancelledHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.ScheduleCancelled,
	_ uuid.UUID,
) error {
	return u.CancelledSchedulesRepo().Insert(ctx, evt.ScheduleID)
}
