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
	txRunner uow.TransactionRunner
	logger   *slog.Logger
}

// NewDeployHandler creates a new DeployHandler
func NewDeployHandler(txRunner uow.TransactionRunner, logger *slog.Logger) *DeployHandler {
	return &DeployHandler{
		txRunner: txRunner,
		logger:   logger,
	}
}

// Handle processes a DeployJob command
func (h *DeployHandler) Handle(ctx context.Context, cmd command.DeployJob) error {
	h.logger.Info("Handling DeployJob command",
		"task_id", cmd.TaskID,
		"job_name", cmd.JobName,
		"table", cmd.TableName,
	)

	taskMaxRetries := cmd.MaxRetries
	if taskMaxRetries <= 0 {
		taskMaxRetries = 2
	}

	entry := &model.DeploymentOutboxEntry{
		ID:               uuid.New(),
		TaskID:           cmd.TaskID,
		ScheduleID:       cmd.ScheduleID,
		ScheduleName:     cmd.ScheduleName,
		ServiceName:      cmd.ServiceName,
		SchemaName:       cmd.SchemaName,
		TableName:        cmd.TableName,
		JobName:          cmd.JobName,
		NodeType:         string(cmd.NodeType),
		ImageTag:         cmd.ImageTag,
		TaskRetryCount:   cmd.TaskRetryCount,
		TaskMaxRetries:   taskMaxRetries,
		Status:           string(model.OutboxStatusPending),
		CreatedAt:        time.Now(),
		OutboxRetryCount: 0,
		OutboxMaxRetries: 3,
	}

	if err := h.txRunner.WithinTransaction(ctx, func(tx uow.Transaction) error {
		return tx.OutboxRepo().Create(ctx, entry)
	}); err != nil {
		return fmt.Errorf("failed to create outbox entry: %w", err)
	}

	h.logger.Info("DeployJob command handled successfully",
		"task_id", cmd.TaskID,
		"outbox_id", entry.ID,
		"job_name", cmd.JobName,
	)

	return nil
}
