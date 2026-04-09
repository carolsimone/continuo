package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/startup-controller/adapters/grpc"
	"github.com/carolsimone/continuo/startup-controller/domain/command"
	"github.com/carolsimone/continuo/startup-controller/domain/event"
	"github.com/carolsimone/continuo/startup-controller/domain/model"
	"github.com/carolsimone/continuo/startup-controller/service/uow"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
)

// InitStateClient defines the state service operations needed by the handler.
type InitStateClient interface {
	UpdateSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID, initStatus string) error
	GetTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error)
	CreateTask(ctx context.Context, taskID, scheduleID uuid.UUID, serviceName, schemaName, tableName string, maxRetries int) error
	UpdateTaskStatus(ctx context.Context, taskID uuid.UUID, taskStatus statev1.TaskStatus) error
}

// InitGraphClient fetches the three node sets needed for scheduler initialization.
type InitGraphClient interface {
	SnapshotGraph(ctx context.Context, runID, scheduleName string) error
	GetScheduleInitNodes(ctx context.Context, scheduleName, runID string) ([]model.NodeInfo, []model.NodeInfo, []model.NodeInfo, error)
	// returns allNodes, rootNodes, seedNodes
}

// InitializeSchedulerHandler handles the InitializeScheduler command
type InitializeSchedulerHandler struct {
	uow         uow.UnitOfWork
	graphClient InitGraphClient
	stateClient InitStateClient
	logger      *slog.Logger
}

// NewInitializeSchedulerHandler creates a new InitializeSchedulerHandler
func NewInitializeSchedulerHandler(
	uow uow.UnitOfWork,
	graphClient InitGraphClient,
	stateClient InitStateClient,
	logger *slog.Logger,
) *InitializeSchedulerHandler {
	return &InitializeSchedulerHandler{
		uow:         uow,
		graphClient: graphClient,
		stateClient: stateClient,
		logger:      logger,
	}
}

// Handle processes the InitializeScheduler command
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
	if err == grpc.ErrAlreadyCompleted {
		h.logger.Info("Initialization already completed, skipping",
			"schedule_id", scheduleID,
		)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to update initialization status: %w", err)
	}

	if err := h.graphClient.SnapshotGraph(ctx, scheduleID.String(), scheduleName); err != nil {
		return fmt.Errorf("failed to snapshot graph: %w", err)
	}

	// Step 3+4: Fetch all three node sets in one graph service call (single Neo4j transaction).
	allNodes, rootNodes, seedNodes, err := h.graphClient.GetScheduleInitNodes(ctx, scheduleName, scheduleID.String())
	if err != nil {
		return fmt.Errorf("failed to get schedule init nodes: %w", err)
	}

	preRegistered := 0
	for _, node := range allNodes {
		_, err := h.stateClient.GetTask(ctx, scheduleID, node.ServiceName, node.Schema, node.TableName)
		if errors.Is(err, grpc.ErrTaskNotFound) {
			taskID := uuid.New()
			if err := h.stateClient.CreateTask(ctx, taskID, scheduleID, node.ServiceName, node.Schema, node.TableName, 3); err != nil {
				return fmt.Errorf("failed to pre-register task for %s.%s: %w", node.Schema, node.TableName, err)
			}
			preRegistered++
		} else if err != nil {
			return fmt.Errorf("failed to check task existence for %s.%s: %w", node.Schema, node.TableName, err)
		}
		// task already exists — leave it untouched (idempotent on retry)
	}

	h.logger.Info("Pre-registering nodes",
		"schedule_id", scheduleID,
		"total_nodes", len(allNodes),
	)

	h.logger.Info("Found root nodes", "count", len(rootNodes), "schedule_name", scheduleName)
	h.logger.Info("Found upstream seed nodes", "count", len(seedNodes), "schedule_name", scheduleName)

	// Step 5: Determine which nodes to dispatch.
	// If upstream seeds exist: dispatch ONLY seeds now.
	// Model root nodes will be dispatched by dependency-controller after seeds complete.
	// If no seeds: dispatch model root nodes directly (backward-compatible path).
	var nodesToDispatch []model.NodeInfo
	if len(seedNodes) > 0 {
		nodesToDispatch = seedNodes
	} else {
		nodesToDispatch = rootNodes
	}

	nodesToPublish := []model.NodeInfo{}

	for _, node := range nodesToDispatch {
		var taskIDStr string

		task, err := h.stateClient.GetTask(ctx, scheduleID, node.ServiceName, node.Schema, node.TableName)
		if errors.Is(err, grpc.ErrTaskNotFound) {
			return fmt.Errorf("task not found for %s.%s - pre-registration should have created it: %w", node.Schema, node.TableName, err)
		} else if err != nil {
			return fmt.Errorf("failed to get task for %s.%s: %w", node.Schema, node.TableName, err)
		}

		if task.Status != statev1.TaskStatus_TASK_STATUS_PENDING {
			taskID, err := uuid.Parse(task.TaskId)
			if err != nil {
				return fmt.Errorf("invalid task_id: %w", err)
			}
			if err := h.stateClient.UpdateTaskStatus(ctx, taskID, statev1.TaskStatus_TASK_STATUS_PENDING); err != nil {
				return fmt.Errorf("failed to update task status: %w", err)
			}
			taskIDStr = task.TaskId
			h.logger.Info("Updated task to pending",
				"task_id", taskID,
				"table", node.TableName,
			)
		} else {
			taskIDStr = task.TaskId
			h.logger.Info("Task already pending, skipping",
				"task_id", task.TaskId,
				"table", node.TableName,
			)
		}

		jobName, err := pkgDomain.ComputeJobName(node.ServiceName, node.Schema, node.TableName)
		if err != nil {
			h.logger.Error("failed to compute job_name", "error", err)
			return fmt.Errorf("failed to compute job_name: %w", err)
		}

		node.TaskID = taskIDStr
		node.JobName = jobName

		nodesToPublish = append(nodesToPublish, node)
	}

	// Step 6: Write events to outbox table (atomic with transaction)
	for _, node := range nodesToPublish {
		evt := event.NodeReadyForExecution{
			ScheduleID:   scheduleID.String(),
			ScheduleName: scheduleName,
			ServiceName:  node.ServiceName,
			Schema:       node.Schema,
			TableName:    node.TableName,
			TaskID:       node.TaskID,
			JobName:      node.JobName,
			NodeType:     string(node.NodeType), // new
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

		h.logger.Debug("Wrote event to outbox",
			"outbox_id", outboxEntry.ID,
			"table", node.TableName,
		)
	}

	// Step 7: Mark initialization as completed (idempotency gate on retry)
	if err := h.stateClient.UpdateSchedulerInitStatus(ctx, scheduleID, "completed"); err != nil {
		return fmt.Errorf("failed to mark initialization complete: %w", err)
	}

	// Step 8: Commit PostgreSQL transaction
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	h.logger.Info("Initialization completed",
		"schedule_id", scheduleID,
		"nodes_queued", len(nodesToPublish),
	)

	return nil
}
