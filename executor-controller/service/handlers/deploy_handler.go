package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// DeployHandler handles DeployJob commands by writing to the outbox table
type DeployHandler struct {
	uow    uow.UnitOfWork
	logger *slog.Logger
}

// NewDeployHandler creates a new DeployHandler
func NewDeployHandler(uow uow.UnitOfWork, logger *slog.Logger) *DeployHandler {
	return &DeployHandler{
		uow:    uow,
		logger: logger,
	}
}

// Handle processes a DeployJob command
func (h *DeployHandler) Handle(ctx context.Context, cmd command.DeployJob) error {
	h.logger.Info("Handling DeployJob command",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"table", cmd.TableName,
	)

	// Step 1: Begin transaction
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback()

	// Step 2: Write deployment intent to outbox table
	taskMaxRetries := cmd.MaxRetries
	if taskMaxRetries <= 0 {
		taskMaxRetries = 3
	}

	entry := &model.DeploymentOutboxEntry{
		ID:             uuid.New(),
		TaskID:         cmd.TaskID,
		ScheduleID:     cmd.ScheduleID,
		ScheduleName:   cmd.ScheduleName,
		ServiceName:    cmd.ServiceName,
		SchemaName:     cmd.SchemaName,
		TableName:      cmd.TableName,
		JobName:        cmd.JobName,
		NodeType:       string(cmd.NodeType),
		TaskRetryCount: cmd.TaskRetryCount,
		TaskMaxRetries: taskMaxRetries,
		Status:         string(model.OutboxStatusPending),
		CreatedAt:      time.Now(),
		OutboxRetryCount: 0,
		OutboxMaxRetries: 3,
	}

	if err := h.uow.OutboxRepo().Create(ctx, entry); err != nil {
		return fmt.Errorf("failed to create outbox entry: %w", err)
	}

	// Step 3: Commit transaction
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	h.logger.Info("DeployJob command handled successfully",
		"task_id", cmd.TaskID,
		"outbox_id", entry.ID,
		"job_name", cmd.JobName,
	)

	return nil
}
