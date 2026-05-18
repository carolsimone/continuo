package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// RunEntriesDispatchFailedHandler processes run.entries.dispatch_failed:v1
// events. Loads the Run aggregate, invokes MarkDispatchFailed (which records
// RunFinalized + RunDispatchFailed events when the run is not already
// terminal), persists via SaveRun, and publishes via OutboxPublisher.
//
// No-op when the run is already terminal — the aggregate handles the guard.
type RunEntriesDispatchFailedHandler struct {
	logger *slog.Logger
}

// NewRunEntriesDispatchFailedHandler constructs the handler.
func NewRunEntriesDispatchFailedHandler(logger *slog.Logger) *RunEntriesDispatchFailedHandler {
	return &RunEntriesDispatchFailedHandler{logger: logger}
}

// Handle orchestrates the dispatch-failed flow.
//
// Caller contract: u.Begin(ctx) has been called; the handler MUST NOT commit.
// Returning nil tells the binding to commit; returning an error triggers
// rollback. msgProcID is stored on the outbox entry for provenance back to
// the inbound message.
func (h *RunEntriesDispatchFailedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.RunEntriesDispatchFailed,
	msgProcID uuid.UUID,
) error {
	r, err := u.Run().LoadRunForUpdate(ctx, u.Tx(), evt.ScheduleID)
	if err != nil {
		return fmt.Errorf("load run: %w", err)
	}
	domainEvents, err := r.MarkDispatchFailed(string(evt.Reason), u.Clock().Now())
	if err != nil {
		return fmt.Errorf("mark dispatch failed: %w", err)
	}
	if err := u.Run().SaveRun(ctx, u.Tx(), r); err != nil {
		return fmt.Errorf("save run: %w", err)
	}
	if err := u.Outbox().Append(ctx, u.Tx(), domainEvents, msgProcID); err != nil {
		return fmt.Errorf("append outbox: %w", err)
	}
	return nil
}
