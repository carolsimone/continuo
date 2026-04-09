package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	graphv1 "github.com/carolsimone/continuo/graph/api/graph/v1"
	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	grpcadapter "github.com/carolsimone/continuo/startup-controller/adapters/grpc"
	"github.com/carolsimone/continuo/startup-controller/domain/command"
	"github.com/carolsimone/continuo/startup-controller/domain/event"
	"github.com/carolsimone/continuo/startup-controller/domain/model"
	"github.com/carolsimone/continuo/startup-controller/service/uow"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
)

// RerunStateClient defines state service operations needed by RerunHandler.
type RerunStateClient interface {
	UpdateSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID, initStatus string) error
	GetTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error)
	ResetTask(ctx context.Context, taskID uuid.UUID) error
}

// RerunGraphClient defines graph service operations needed by RerunHandler.
type RerunGraphClient interface {
	UpdateNodeStatus(ctx context.Context, scheduleName, schemaName, tableName, status, runID string) error
	GetTransitiveDownstream(ctx context.Context, scheduleName, schemaName, tableName string) ([]*graphv1.TableNode, error)
	GetScheduleInitNodes(ctx context.Context, scheduleName, runID string) (allNodes []model.NodeInfo, rootNodes []model.NodeInfo, seedNodes []model.NodeInfo, err error)
}

// RerunHandler orchestrates graph and state resets for a re-run request.
type RerunHandler struct {
	uow         uow.UnitOfWork
	stateClient RerunStateClient
	graphClient RerunGraphClient
	logger      *slog.Logger
}

// NewRerunHandler creates a new RerunHandler.
func NewRerunHandler(
	uow uow.UnitOfWork,
	stateClient RerunStateClient,
	graphClient RerunGraphClient,
	logger *slog.Logger,
) *RerunHandler {
	return &RerunHandler{uow: uow, stateClient: stateClient, graphClient: graphClient, logger: logger}
}

// Handle processes a RerunNode command.
func (h *RerunHandler) Handle(ctx context.Context, cmd command.RerunNode) error {
	// Idempotency gate: skip if already completed.
	err := h.stateClient.UpdateSchedulerInitStatus(ctx, cmd.ScheduleID, "in_progress")
	if errors.Is(err, grpcadapter.ErrAlreadyCompleted) {
		h.logger.Info("Re-run already completed, skipping", "schedule_id", cmd.ScheduleID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to set init status in_progress: %w", err)
	}

	// Get transitive downstream (returns only non-SUCCEEDED nodes).
	downstream, err := h.graphClient.GetTransitiveDownstream(ctx, cmd.ScheduleName, cmd.Schema, cmd.TableName)
	if err != nil {
		return fmt.Errorf("failed to get transitive downstream: %w", err)
	}

	// Reset target node in graph (always — it was FAILED by definition).
	if err := h.graphClient.UpdateNodeStatus(ctx, cmd.ScheduleName, cmd.Schema, cmd.TableName, "PENDING", cmd.ScheduleID.String()); err != nil {
		return fmt.Errorf("failed to reset target graph node: %w", err)
	}

	// Reset target node task (status=PENDING, retry_count=0).
	// Fetch once here and reuse the task_id later for the dispatch outbox entry.
	targetTask, err := h.stateClient.GetTask(ctx, cmd.ScheduleID, cmd.ServiceName, cmd.Schema, cmd.TableName)
	if err != nil {
		return fmt.Errorf("failed to get target task: %w", err)
	}
	targetTaskID, err := uuid.Parse(targetTask.TaskId)
	if err != nil {
		return fmt.Errorf("invalid target task_id: %w", err)
	}
	// The HTTP rerun handler already resets the target task to PENDING atomically.
	// Only call ResetTask if the task is still FAILED (e.g. startup-controller
	// is replaying a lost message after a crash).
	if targetTask.Status == statev1.TaskStatus_TASK_STATUS_FAILED {
		if err := h.stateClient.ResetTask(ctx, targetTaskID); err != nil {
			return fmt.Errorf("failed to reset target task: %w", err)
		}
	}

	// Reset only FAILED downstream nodes in graph and state.
	// (GetTransitiveDownstream may also return PENDING nodes that were never run;
	//  those don't need resetting — only FAILED ones do.)
	for _, node := range downstream {
		if node.Status != "FAILED" {
			continue
		}
		if err := h.graphClient.UpdateNodeStatus(ctx, cmd.ScheduleName, node.SchemaName, node.TableName, "PENDING", cmd.ScheduleID.String()); err != nil {
			return fmt.Errorf("failed to reset downstream graph node %s.%s: %w", node.SchemaName, node.TableName, err)
		}
		task, err := h.stateClient.GetTask(ctx, cmd.ScheduleID, node.ServiceName, node.SchemaName, node.TableName)
		if err != nil {
			return fmt.Errorf("failed to get task for %s.%s: %w", node.SchemaName, node.TableName, err)
		}
		taskID, _ := uuid.Parse(task.TaskId)
		if err := h.stateClient.ResetTask(ctx, taskID); err != nil {
			return fmt.Errorf("failed to reset task for %s.%s: %w", node.SchemaName, node.TableName, err)
		}
	}

	// Look up the target node's NodeType and current service_name from the graph.
	// service_name may differ from cmd.ServiceName if the node was re-pointed to a
	// different service during the fix step (e.g. service-3-broken → service-3).
	allNodes, _, _, err := h.graphClient.GetScheduleInitNodes(ctx, cmd.ScheduleName, cmd.ScheduleID.String())
	if err != nil {
		return fmt.Errorf("failed to get schedule nodes for node_type lookup: %w", err)
	}
	var targetNodeType string
	effectiveServiceName := cmd.ServiceName // fallback: use request value if graph lookup fails
	for _, n := range allNodes {
		if n.Schema == cmd.Schema && n.TableName == cmd.TableName {
			targetNodeType = string(n.NodeType)
			effectiveServiceName = n.ServiceName
			break
		}
	}
	if targetNodeType == "" {
		return fmt.Errorf("could not determine node_type for %s.%s", cmd.Schema, cmd.TableName)
	}

	// Write dispatch event to outbox for target node only.
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback()

	jobName, err := pkgDomain.ComputeJobName(effectiveServiceName, cmd.Schema, cmd.TableName)
	if err != nil {
		return fmt.Errorf("failed to compute job_name: %w", err)
	}

	evt := event.NodeReadyForExecution{
		ScheduleID:   cmd.ScheduleID.String(),
		ScheduleName: cmd.ScheduleName,
		ServiceName:  effectiveServiceName,
		Schema:       cmd.Schema,
		TableName:    cmd.TableName,
		TaskID:       targetTask.TaskId,
		JobName:      jobName,
		NodeType:     targetNodeType,
	}
	payload, _ := json.Marshal(evt)

	if err := h.uow.OutboxRepo().Create(ctx, &model.OutboxEntry{
		ID:            uuid.New(),
		AggregateType: "scheduler",
		AggregateID:   cmd.ScheduleID,
		EventType:     "node_ready_for_execution",
		Payload:       payload,
		StreamName:    "query.model:v1",
		Status:        string(model.OutboxStatusPending),
		MaxRetries:    3,
	}); err != nil {
		return fmt.Errorf("failed to write outbox: %w", err)
	}

	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	// IMPORTANT: mark completed AFTER graph resets and outbox commit.
	// dependency-controller skips schedule finalization while init_status != "completed".
	if err := h.stateClient.UpdateSchedulerInitStatus(ctx, cmd.ScheduleID, "completed"); err != nil {
		return fmt.Errorf("failed to set init status completed: %w", err)
	}

	h.logger.Info("Re-run dispatched", "schedule_id", cmd.ScheduleID, "target", cmd.TableName)
	return nil
}
