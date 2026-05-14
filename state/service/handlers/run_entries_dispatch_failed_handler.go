package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// RunEntriesDispatchFailedHandler processes run.entries.dispatch_failed:v1
// events. When the scheduler is not already terminal, it finalizes the
// scheduler as failed and writes a run.finalized:v1 outbox entry.
//
// The handler is pure orchestration: it takes a UnitOfWork and a typed event,
// and never parses JSON, manages a transaction, or runs dedup itself — those
// responsibilities live in the adapter binding that wires the parser, dedup,
// and UoW lifecycle around this call.
type RunEntriesDispatchFailedHandler struct {
	logger *slog.Logger
}

// NewRunEntriesDispatchFailedHandler constructs the handler.
func NewRunEntriesDispatchFailedHandler(logger *slog.Logger) *RunEntriesDispatchFailedHandler {
	return &RunEntriesDispatchFailedHandler{logger: logger}
}

// Handle orchestrates the dispatch-failed flow.
// Caller contract: u.Begin(ctx) has been called; the handler MUST NOT commit.
// Returning nil tells the binding to commit; returning an error triggers rollback.
// msgProcID is stored on the outbox entry for provenance back to the inbound message.
func (h *RunEntriesDispatchFailedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.RunEntriesDispatchFailed,
	msgProcID uuid.UUID,
) error {
	scheduler, err := u.SchedulerRepo().GetByIDForUpdateTx(ctx, u.Tx(), evt.ScheduleID)
	if err != nil {
		return fmt.Errorf("lock scheduler row: %w", err)
	}

	if isTerminalSchedulerStatus(scheduler.Status) {
		h.logger.Info("run.entries.dispatch_failed: scheduler already terminal — skipping",
			"schedule_id", evt.ScheduleID, "status", scheduler.Status)
		return nil
	}

	if err := u.SchedulerRepo().FinalizeRunTx(ctx, u.Tx(), evt.ScheduleID, string(model.SchedulerStatusFailed)); err != nil {
		return fmt.Errorf("finalize scheduler status to failed: %w", err)
	}

	finalizedEvt := pkgevents.RunFinalized{
		ScheduleID:   evt.ScheduleID.String(),
		ScheduleName: scheduler.ScheduleName,
		Status:       string(model.SchedulerStatusFailed),
	}
	payload, err := json.Marshal(finalizedEvt)
	if err != nil {
		return fmt.Errorf("marshal run.finalized payload: %w", err)
	}

	if err := u.OutboxRepo().Create(ctx, u.Tx(), &postgres.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcID,
		AggregateType:       "scheduler_tracker",
		AggregateID:         evt.ScheduleID,
		EventType:           "run.finalized:v1",
		Payload:             payload,
		StreamName:          "run.finalized:v1",
		Status:              "pending",
		MaxRetries:          5,
		RetryCount:          0,
		CreatedAt:           time.Now(),
	}); err != nil {
		return fmt.Errorf("create outbox entry for run.finalized: %w", err)
	}

	h.logger.Info("run.entries.dispatch_failed: processed",
		"schedule_id", evt.ScheduleID, "reason", evt.Reason)
	return nil
}

// isTerminalSchedulerStatus reports whether a scheduler can no longer transition.
func isTerminalSchedulerStatus(s model.SchedulerStatus) bool {
	switch s {
	case model.SchedulerStatusSucceeded,
		model.SchedulerStatusFailed,
		model.SchedulerStatusCancelled:
		return true
	}
	return false
}
