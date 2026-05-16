package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// isTerminalSchedulerStatus reports whether a scheduler status can no longer
// transition. Used by handlers that guard against re-processing already-final
// runs via the low-level tracker repos.
func isTerminalSchedulerStatus(s model.SchedulerStatus) bool {
	switch s {
	case model.SchedulerStatusSucceeded,
		model.SchedulerStatusFailed,
		model.SchedulerStatusCancelled:
		return true
	}
	return false
}

// emitRunFinalized writes a run.finalized:v1 outbox entry using the caller's
// UoW transaction. Used by every handler that finalizes a scheduler run so
// the outbox-row shape stays consistent (aggregate_type=scheduler_tracker,
// stream_name=run.finalized:v1, MessageProcessingID carrying provenance back
// to the inbound message, retry budget=5).
//
// Caller contract: u.Begin(ctx) has been called. Errors are returned as-is;
// the binding rolls back the surrounding tx on failure.
func emitRunFinalized(
	ctx context.Context,
	u uow.UnitOfWork,
	scheduleID uuid.UUID,
	scheduleName, outcome string,
	msgProcID uuid.UUID,
) error {
	payload, err := json.Marshal(pkgevents.RunFinalized{
		ScheduleID:   scheduleID.String(),
		ScheduleName: scheduleName,
		Status:       outcome,
	})
	if err != nil {
		return fmt.Errorf("marshal run.finalized payload: %w", err)
	}
	return u.OutboxRepo().Create(ctx, u.Tx(), &postgres.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcID,
		AggregateType:       "scheduler_tracker",
		AggregateID:         scheduleID,
		EventType:           "run.finalized:v1",
		Payload:             payload,
		StreamName:          "run.finalized:v1",
		Status:              "pending",
		MaxRetries:          5,
		RetryCount:          0,
		CreatedAt:           time.Now(),
	})
}
