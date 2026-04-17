package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/startup-controller/domain/command"
	"github.com/carolsimone/continuo/startup-controller/domain/event"
	"github.com/carolsimone/continuo/startup-controller/domain/model"
	"github.com/carolsimone/continuo/startup-controller/service/uow"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
)

// RerunReadyStateClient defines state operations needed by HandleRerunReady.
type RerunReadyStateClient interface {
	UpdateSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID, initStatus string) error
	GetTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error)
	ResetTask(ctx context.Context, taskID uuid.UUID) error
}

// HandleRerunReadyHandler consumes rerun.ready:v1 events from the
// orchestrator. It resets target node tasks and dispatches them for execution.
type HandleRerunReadyHandler struct {
	uow         uow.UnitOfWork
	stateClient RerunReadyStateClient
	logger      *slog.Logger
}

// NewHandleRerunReadyHandler creates a new HandleRerunReadyHandler.
func NewHandleRerunReadyHandler(
	uow uow.UnitOfWork,
	stateClient RerunReadyStateClient,
	logger *slog.Logger,
) *HandleRerunReadyHandler {
	return &HandleRerunReadyHandler{
		uow:         uow,
		stateClient: stateClient,
		logger:      logger,
	}
}

// Handle processes a RerunReady command.
func (h *HandleRerunReadyHandler) Handle(ctx context.Context, cmd command.RerunReady) error {
	scheduleID := cmd.RunID

	h.logger.Info("Handling rerun ready",
		"schedule_id", scheduleID,
		"target_nodes", len(cmd.TargetNodes),
	)

	// Begin transaction for outbox writes
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback()

	for _, node := range cmd.TargetNodes {
		// Reset the task in state
		task, err := h.stateClient.GetTask(ctx, scheduleID, node.ServiceName, node.SchemaName, node.TableName)
		if err != nil {
			return fmt.Errorf("failed to get task for %s.%s: %w", node.SchemaName, node.TableName, err)
		}

		taskID, err := uuid.Parse(task.TaskId)
		if err != nil {
			return fmt.Errorf("invalid task_id: %w", err)
		}

		// Only reset if the task is in a non-PENDING state (e.g., FAILED)
		if task.Status != statev1.TaskStatus_TASK_STATUS_PENDING {
			if err := h.stateClient.ResetTask(ctx, taskID); err != nil {
				return fmt.Errorf("failed to reset task for %s.%s: %w", node.SchemaName, node.TableName, err)
			}
		}

		// Validate node_type
		_, err = pkg_model.ParseNodeType(node.NodeType)
		if err != nil {
			return fmt.Errorf("node %s.%s has invalid node_type %q: %w", node.SchemaName, node.TableName, node.NodeType, err)
		}

		jobName, err := pkgDomain.ComputeJobName(node.ServiceName, node.SchemaName, node.TableName)
		if err != nil {
			return fmt.Errorf("failed to compute job_name: %w", err)
		}

		scheduleName := node.ScheduleName

		evt := event.NodeReadyForExecution{
			ScheduleID:   scheduleID.String(),
			ScheduleName: scheduleName,
			ServiceName:  node.ServiceName,
			SchemaName:   node.SchemaName,
			TableName:    node.TableName,
			TaskID:       task.TaskId,
			JobName:      jobName,
			NodeType:     node.NodeType,
		}

		payload, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		outboxEntry := &model.OutboxEntry{
			ID:            uuid.New(),
			AggregateType: "scheduler",
			AggregateID:   scheduleID,
			EventType:     "node_ready_for_execution",
			Payload:       payload,
			StreamName:    "query.model:v1",
			Status:        string(model.OutboxStatusPending),
			MaxRetries:    3,
		}

		if err := h.uow.OutboxRepo().Create(ctx, outboxEntry); err != nil {
			return fmt.Errorf("failed to write to outbox: %w", err)
		}

		h.logger.Debug("Wrote rerun dispatch to outbox",
			"table", node.TableName,
			"task_id", task.TaskId,
		)
	}

	// Mark initialization as completed after outbox writes
	if err := h.stateClient.UpdateSchedulerInitStatus(ctx, scheduleID, "completed"); err != nil {
		return fmt.Errorf("failed to set init status completed: %w", err)
	}

	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	h.logger.Info("Rerun ready handling completed",
		"schedule_id", scheduleID,
		"nodes_dispatched", len(cmd.TargetNodes),
	)

	return nil
}
