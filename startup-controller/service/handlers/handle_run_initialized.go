package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	grpcadapter "github.com/carolsimone/continuo/startup-controller/adapters/grpc"
	"github.com/carolsimone/continuo/startup-controller/domain/command"
	"github.com/carolsimone/continuo/startup-controller/domain/event"
	"github.com/carolsimone/continuo/startup-controller/domain/model"
	"github.com/carolsimone/continuo/startup-controller/service/uow"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
)

// RunInitializedStateClient defines the state service operations needed by
// the HandleRunInitialized handler.
type RunInitializedStateClient interface {
	UpdateSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID, initStatus string) error
	GetTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error)
	CreateTask(ctx context.Context, taskID, scheduleID uuid.UUID, serviceName, schemaName, tableName string, maxRetries int) error
	UpdateTaskStatus(ctx context.Context, taskID uuid.UUID, taskStatus statev1.TaskStatus) error
}

// HandleRunInitializedHandler consumes run.initialized:v1 events from the
// orchestrator. It pre-registers all nodes as tasks and dispatches seed or
// root nodes for execution via the outbox.
type HandleRunInitializedHandler struct {
	uow         uow.UnitOfWork
	stateClient RunInitializedStateClient
	logger      *slog.Logger
}

// NewHandleRunInitializedHandler creates a new HandleRunInitializedHandler.
func NewHandleRunInitializedHandler(
	uow uow.UnitOfWork,
	stateClient RunInitializedStateClient,
	logger *slog.Logger,
) *HandleRunInitializedHandler {
	return &HandleRunInitializedHandler{
		uow:         uow,
		stateClient: stateClient,
		logger:      logger,
	}
}

// Handle processes a RunInitialized command.
func (h *HandleRunInitializedHandler) Handle(ctx context.Context, cmd command.RunInitialized) error {
	scheduleID := cmd.RunID

	h.logger.Info("Handling run initialized",
		"schedule_id", scheduleID,
		"all_nodes", len(cmd.AllNodes),
		"root_nodes", len(cmd.RootNodes),
		"seed_nodes", len(cmd.SeedNodes),
	)

	// Step 1: Begin PostgreSQL transaction
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback()

	// Step 2: Pre-register all nodes as tasks
	preRegistered := 0
	for _, node := range cmd.AllNodes {
		_, err := h.stateClient.GetTask(ctx, scheduleID, node.ServiceName, node.SchemaName, node.TableName)
		if errors.Is(err, grpcadapter.ErrTaskNotFound) {
			taskID := uuid.New()
			if err := h.stateClient.CreateTask(ctx, taskID, scheduleID, node.ServiceName, node.SchemaName, node.TableName, 3); err != nil {
				return fmt.Errorf("failed to pre-register task for %s.%s: %w", node.SchemaName, node.TableName, err)
			}
			preRegistered++
		} else if err != nil {
			return fmt.Errorf("failed to check task existence for %s.%s: %w", node.SchemaName, node.TableName, err)
		}
	}

	h.logger.Info("Pre-registered nodes",
		"schedule_id", scheduleID,
		"pre_registered", preRegistered,
		"total_nodes", len(cmd.AllNodes),
	)

	// Step 3: Determine which nodes to dispatch
	var nodesToDispatch []command.NodePayload
	if len(cmd.SeedNodes) > 0 {
		nodesToDispatch = cmd.SeedNodes
		h.logger.Info("Dispatching seed nodes", "count", len(cmd.SeedNodes))
	} else {
		nodesToDispatch = cmd.RootNodes
		h.logger.Info("Dispatching root nodes", "count", len(cmd.RootNodes))
	}

	// Step 4: Write dispatch events to outbox
	for _, node := range nodesToDispatch {
		task, err := h.stateClient.GetTask(ctx, scheduleID, node.ServiceName, node.SchemaName, node.TableName)
		if errors.Is(err, grpcadapter.ErrTaskNotFound) {
			return fmt.Errorf("task not found for %s.%s - pre-registration should have created it: %w", node.SchemaName, node.TableName, err)
		} else if err != nil {
			return fmt.Errorf("failed to get task for %s.%s: %w", node.SchemaName, node.TableName, err)
		}

		if task.Status != statev1.TaskStatus_TASK_STATUS_PENDING {
			taskID, err := uuid.Parse(task.TaskId)
			if err != nil {
				return fmt.Errorf("invalid task_id: %w", err)
			}
			if err := h.stateClient.UpdateTaskStatus(ctx, taskID, statev1.TaskStatus_TASK_STATUS_PENDING); err != nil {
				return fmt.Errorf("failed to update task status: %w", err)
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

		h.logger.Debug("Wrote event to outbox",
			"outbox_id", outboxEntry.ID,
			"table", node.TableName,
		)
	}

	// Step 5: Mark initialization as completed
	if err := h.stateClient.UpdateSchedulerInitStatus(ctx, scheduleID, "completed"); err != nil {
		return fmt.Errorf("failed to mark initialization complete: %w", err)
	}

	// Step 6: Commit PostgreSQL transaction
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	h.logger.Info("Run initialization completed",
		"schedule_id", scheduleID,
		"nodes_dispatched", len(nodesToDispatch),
	)

	return nil
}
