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

// InitStateClient defines the state service operations needed by the handler.
type InitStateClient interface {
	UpdateSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID, initStatus string) error
}

// InitializeSchedulerHandler handles the InitializeScheduler command.
// It sets init_status = "in_progress" and emits an initialize.run:v1 event
// via the outbox, delegating graph operations to the orchestrator service.
type InitializeSchedulerHandler struct {
	uow                    uow.UnitOfWork
	stateClient            InitStateClient
	initializeRunStream    string
	logger                 *slog.Logger
}

// NewInitializeSchedulerHandler creates a new InitializeSchedulerHandler
func NewInitializeSchedulerHandler(
	uow uow.UnitOfWork,
	stateClient InitStateClient,
	initializeRunStream string,
	logger *slog.Logger,
) *InitializeSchedulerHandler {
	return &InitializeSchedulerHandler{
		uow:                 uow,
		stateClient:         stateClient,
		initializeRunStream: initializeRunStream,
		logger:              logger,
	}
}

// Handle processes the InitializeScheduler command.
// It sets init_status to "in_progress" and writes an initialize.run:v1 event
// to the outbox. The orchestrator will consume this event, perform the graph
// snapshot + node resolution, and publish run.initialized:v1 back.
func (h *InitializeSchedulerHandler) Handle(ctx context.Context, cmd command.InitializeScheduler) error {
	scheduleID := cmd.RunnerID
	scheduleName := cmd.ScheduleName

	h.logger.Info("Initializing scheduler",
		"schedule_id", scheduleID,
		"schedule_name", scheduleName,
	)

	// Step 1: Begin PostgreSQL transaction
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback()

	// Step 2: Check and update initialization status (idempotency)
	err := h.stateClient.UpdateSchedulerInitStatus(ctx, scheduleID, "in_progress")
	if errors.Is(err, grpcadapter.ErrAlreadyCompleted) {
		h.logger.Info("Initialization already completed, skipping",
			"schedule_id", scheduleID,
		)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to update initialization status: %w", err)
	}

	// Step 3: Write initialize.run:v1 event to outbox
	payload, err := json.Marshal(map[string]string{
		"schedule_name": scheduleName,
		"run_id":        scheduleID.String(),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal event payload: %w", err)
	}

	outboxEntry := &model.OutboxEntry{
		ID:            uuid.New(),
		AggregateType: "scheduler",
		AggregateID:   scheduleID,
		EventType:     "run_initialization_requested",
		Payload:       payload,
		StreamName:    h.initializeRunStream,
		Status:        string(model.OutboxStatusPending),
		MaxRetries:    3,
	}

	if err := h.uow.OutboxRepo().Create(ctx, outboxEntry); err != nil {
		return fmt.Errorf("failed to write to outbox: %w", err)
	}

	// Step 4: Commit PostgreSQL transaction
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	h.logger.Info("Initialize run event emitted",
		"schedule_id", scheduleID,
		"schedule_name", scheduleName,
		"stream", h.initializeRunStream,
	)

	return nil
}
