package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// RunEntriesDispatchFailedHandler processes run.entries.dispatch_failed:v1 events.
// Counterpart of RunEntriesDispatchedHandler: when orchestrator cannot produce
// dispatch work for a run (e.g. snapshot.ErrTargetNotFound on a single-node run
// targeting an unknown table, or snapshot.ErrEmptyProjection on a rerun, rebase,
// or cron schedule whose topology yields zero tasks), state finalizes the
// scheduler_tracker as failed and emits run.finalized:v1.
type RunEntriesDispatchFailedHandler struct {
	db            *sqlx.DB
	schedulerRepo postgres.SchedulerTrackerRepository
	outboxRepo    postgres.OutboxRepository
	logger        *slog.Logger
}

// NewRunEntriesDispatchFailedHandler creates a RunEntriesDispatchFailedHandler.
func NewRunEntriesDispatchFailedHandler(
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	outboxRepo postgres.OutboxRepository,
	logger *slog.Logger,
) *RunEntriesDispatchFailedHandler {
	return &RunEntriesDispatchFailedHandler{
		db:            db,
		schedulerRepo: schedulerRepo,
		outboxRepo:    outboxRepo,
		logger:        logger,
	}
}

// Handle processes a raw payload string from a run.entries.dispatch_failed:v1 message.
// shouldACK=true means ACK regardless of err (permanent/unprocessable message).
// shouldACK=false + err means transient failure — do NOT ACK.
func (h *RunEntriesDispatchFailedHandler) Handle(ctx context.Context, messageID, payloadStr string) (shouldACK bool, err error) {
	if payloadStr == "" {
		h.logger.Error("run.entries.dispatch_failed: missing payload field — discarding")
		return true, nil
	}

	var evt events.RunEntriesDispatchFailed
	if err := json.Unmarshal([]byte(payloadStr), &evt); err != nil {
		h.logger.Error("run.entries.dispatch_failed: invalid JSON payload — discarding", "error", err)
		return true, nil
	}

	scheduleID, err := uuid.Parse(evt.ScheduleID)
	if err != nil {
		h.logger.Error("run.entries.dispatch_failed: invalid schedule_id UUID — discarding", "error", err)
		return true, nil
	}

	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	dedupID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("run.entries.dispatch_failed:"+messageID))
	var already bool
	if dbErr := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = $1)`,
		dedupID,
	).Scan(&already); dbErr != nil {
		return false, fmt.Errorf("dedup check: %w", dbErr)
	}
	if already {
		h.logger.Info("run.entries.dispatch_failed: duplicate message — skipping", "message_id", messageID)
		_ = tx.Commit()
		return true, nil
	}

	scheduler, dbErr := h.schedulerRepo.GetByIDForUpdateTx(ctx, tx, scheduleID)
	if dbErr != nil {
		return false, fmt.Errorf("lock scheduler row: %w", dbErr)
	}

	// Idempotency: if the scheduler is already terminal, just record dedup and ACK.
	if isTerminalSchedulerStatus(scheduler.Status) {
		h.logger.Info("run.entries.dispatch_failed: scheduler already terminal — skipping",
			"schedule_id", scheduleID, "status", scheduler.Status)
		if _, dbErr := tx.ExecContext(ctx,
			`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
			dedupID,
		); dbErr != nil {
			return false, fmt.Errorf("record processed event: %w", dbErr)
		}
		if txErr := tx.Commit(); txErr != nil {
			return false, fmt.Errorf("commit tx: %w", txErr)
		}
		return true, nil
	}

	if txErr := h.schedulerRepo.FinalizeRunTx(ctx, tx, scheduleID, string(model.SchedulerStatusFailed)); txErr != nil {
		return false, fmt.Errorf("finalize scheduler status to failed: %w", txErr)
	}

	finalizedEvt := events.RunFinalized{
		ScheduleID:   scheduleID.String(),
		ScheduleName: scheduler.ScheduleName,
		Status:       string(model.SchedulerStatusFailed),
	}
	finalizedPayload, marshalErr := json.Marshal(finalizedEvt)
	if marshalErr != nil {
		return false, fmt.Errorf("marshal run.finalized payload: %w", marshalErr)
	}

	outboxEntry := &postgres.OutboxEntry{
		ID:            uuid.New(),
		AggregateType: "scheduler_tracker",
		AggregateID:   scheduleID,
		EventType:     "run.finalized:v1",
		Payload:       finalizedPayload,
		StreamName:    "run.finalized:v1",
		Status:        "pending",
		MaxRetries:    5,
		RetryCount:    0,
		CreatedAt:     time.Now(),
	}
	if txErr := h.outboxRepo.Create(ctx, tx, outboxEntry); txErr != nil {
		return false, fmt.Errorf("create outbox entry for run.finalized: %w", txErr)
	}

	if _, dbErr := tx.ExecContext(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		dedupID,
	); dbErr != nil {
		return false, fmt.Errorf("record processed event: %w", dbErr)
	}

	if txErr := tx.Commit(); txErr != nil {
		return false, fmt.Errorf("commit tx: %w", txErr)
	}

	h.logger.Info("run.entries.dispatch_failed: processed",
		"schedule_id", scheduleID,
		"reason", evt.Reason,
	)
	return true, nil
}

func isTerminalSchedulerStatus(s model.SchedulerStatus) bool {
	switch s {
	case model.SchedulerStatusSucceeded,
		model.SchedulerStatusFailed,
		model.SchedulerStatusCancelled:
		return true
	}
	return false
}
