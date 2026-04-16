package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	grpcadapter "github.com/carolsimone/continuo/startup-controller/adapters/grpc"
	"github.com/carolsimone/continuo/startup-controller/domain/command"
	"github.com/carolsimone/continuo/startup-controller/domain/model"
	"github.com/carolsimone/continuo/startup-controller/service/uow"
	"github.com/google/uuid"
)

// RerunStateClient defines state service operations needed by RerunHandler.
type RerunStateClient interface {
	UpdateSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID, initStatus string) error
}

// RerunHandler emits an initialize.run:v1 event with rerun_target fields,
// delegating graph reset and node resolution to the orchestrator service.
type RerunHandler struct {
	uow                 uow.UnitOfWork
	stateClient         RerunStateClient
	initializeRunStream string
	logger              *slog.Logger
}

// NewRerunHandler creates a new RerunHandler.
func NewRerunHandler(
	uow uow.UnitOfWork,
	stateClient RerunStateClient,
	initializeRunStream string,
	logger *slog.Logger,
) *RerunHandler {
	return &RerunHandler{
		uow:                 uow,
		stateClient:         stateClient,
		initializeRunStream: initializeRunStream,
		logger:              logger,
	}
}

// Handle processes a RerunNode command.
func (h *RerunHandler) Handle(ctx context.Context, cmd command.RerunNode) error {
	h.logger.Info("Processing rerun request",
		"schedule_id", cmd.ScheduleID,
		"schedule_name", cmd.ScheduleName,
		"target", fmt.Sprintf("%s.%s", cmd.Schema, cmd.TableName),
	)

	// Idempotency gate: skip if already completed.
	err := h.stateClient.UpdateSchedulerInitStatus(ctx, cmd.ScheduleID, "in_progress")
	if errors.Is(err, grpcadapter.ErrAlreadyCompleted) {
		h.logger.Info("Re-run already completed, skipping", "schedule_id", cmd.ScheduleID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to set init status in_progress: %w", err)
	}

	// Begin transaction for outbox write
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback()

	// Write initialize.run:v1 event with rerun_target fields
	payload, err := json.Marshal(map[string]interface{}{
		"schedule_name": cmd.ScheduleName,
		"run_id":        cmd.ScheduleID.String(),
		"rerun_target": map[string]string{
			"service_name": cmd.ServiceName,
			"schema_name":  cmd.Schema,
			"table_name":   cmd.TableName,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to marshal event payload: %w", err)
	}

	outboxEntry := &model.OutboxEntry{
		ID:            uuid.New(),
		AggregateType: "scheduler",
		AggregateID:   cmd.ScheduleID,
		EventType:     "rerun_requested",
		Payload:       payload,
		StreamName:    h.initializeRunStream,
		Status:        string(model.OutboxStatusPending),
		MaxRetries:    3,
	}

	if err := h.uow.OutboxRepo().Create(ctx, outboxEntry); err != nil {
		return fmt.Errorf("failed to write outbox: %w", err)
	}

	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	h.logger.Info("Rerun event emitted",
		"schedule_id", cmd.ScheduleID,
		"target", cmd.TableName,
		"stream", h.initializeRunStream,
	)

	return nil
}
