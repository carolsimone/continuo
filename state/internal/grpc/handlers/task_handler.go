package handlers

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TaskHandler handles all task-related gRPC requests
type TaskHandler struct {
	repo   postgres.TaskTrackerRepository
	logger *slog.Logger
}

// NewTaskHandler creates a new TaskHandler.
func NewTaskHandler(repo postgres.TaskTrackerRepository, logger *slog.Logger) *TaskHandler {
	return &TaskHandler{
		repo:   repo,
		logger: logger,
	}
}

// GetTask retrieves a task by ID
func (h *TaskHandler) GetTask(ctx context.Context, req *statev1.GetTaskRequest) (*statev1.TaskResponse, error) {
	h.logger.Debug("Getting task", "task_id", req.TaskId)

	if req.TaskId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "task_id is required")
	}

	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task_id format")
	}

	task, err := h.repo.GetByID(ctx, taskID)
	if err != nil {
		if err == postgres.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "task not found")
		}
		h.logger.Error("Failed to get task", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get task")
	}

	return &statev1.TaskResponse{
		Task: domainToProtoTask(task),
	}, nil
}

// GetTaskByScheduleAndNode retrieves a task by schedule_id and node information
func (h *TaskHandler) GetTaskByScheduleAndNode(ctx context.Context, req *statev1.GetTaskByScheduleAndNodeRequest) (*statev1.TaskResponse, error) {
	h.logger.Debug("Getting task by schedule and node",
		"schedule_id", req.ScheduleId,
		"service_name", req.ServiceName,
		"schema_name", req.SchemaName,
		"table_name", req.TableName,
	)

	// Validate required fields
	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}

	if req.ServiceName == "" || req.SchemaName == "" || req.TableName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "service_name, schema_name, and table_name are required")
	}

	scheduleID, err := uuid.Parse(req.ScheduleId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id format")
	}

	task, err := h.repo.GetByScheduleAndNode(ctx, scheduleID, req.ServiceName, req.SchemaName, req.TableName)
	if err != nil {
		if err == postgres.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "task not found")
		}
		h.logger.Error("Failed to get task by schedule and node", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get task")
	}

	return &statev1.TaskResponse{
		Task: domainToProtoTask(task),
	}, nil
}

// ListTasks lists tasks for a specific schedule with optional filters
func (h *TaskHandler) ListTasks(ctx context.Context, req *statev1.ListTasksRequest) (*statev1.ListTasksResponse, error) {
	h.logger.Debug("Listing tasks", "schedule_id", req.ScheduleId, "status", req.Status)

	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}

	scheduleID, err := uuid.Parse(req.ScheduleId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id format")
	}

	// Build filters
	limit := 50 // Default page size
	offset := 0

	if req.PageSize > 0 {
		limit = int(req.PageSize)
	}

	if req.PageOffset > 0 {
		offset = int(req.PageOffset)
	}

	var statusFilter *run.TaskStatus
	if req.Status != statev1.TaskStatus_TASK_STATUS_UNSPECIFIED {
		s := protoToDomainTaskStatus(req.Status)
		statusFilter = &s
	}

	tasks, total, err := h.repo.ListByScheduleID(ctx, scheduleID, statusFilter, limit, offset)
	if err != nil {
		h.logger.Error("Failed to list tasks", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to list tasks")
	}

	protoTasks := make([]*statev1.Task, len(tasks))
	for i, t := range tasks {
		protoTasks[i] = domainToProtoTask(t)
	}

	return &statev1.ListTasksResponse{
		Tasks:      protoTasks,
		TotalCount: num.ClampInt32(total),
	}, nil
}

// ============================================================================
// CONVERSION HELPERS
// ============================================================================

// protoToDomainTaskStatus converts proto status to domain model status
func protoToDomainTaskStatus(s statev1.TaskStatus) run.TaskStatus {
	switch s {
	case statev1.TaskStatus_TASK_STATUS_PENDING:
		return run.TaskStatusPending
	case statev1.TaskStatus_TASK_STATUS_RUNNING:
		return run.TaskStatusRunning
	case statev1.TaskStatus_TASK_STATUS_SUCCEEDED:
		return run.TaskStatusSucceeded
	case statev1.TaskStatus_TASK_STATUS_FAILED:
		return run.TaskStatusFailed
	case statev1.TaskStatus_TASK_STATUS_CANCELLED:
		return run.TaskStatusCancelled
	case statev1.TaskStatus_TASK_STATUS_SKIPPED:
		return run.TaskStatusSkipped
	default:
		return run.TaskStatusPending
	}
}

// domainToProtoTaskStatus converts domain model status to proto status
func domainToProtoTaskStatus(s run.TaskStatus) statev1.TaskStatus {
	switch s {
	case run.TaskStatusPending:
		return statev1.TaskStatus_TASK_STATUS_PENDING
	case run.TaskStatusRunning:
		return statev1.TaskStatus_TASK_STATUS_RUNNING
	case run.TaskStatusSucceeded:
		return statev1.TaskStatus_TASK_STATUS_SUCCEEDED
	case run.TaskStatusFailed:
		return statev1.TaskStatus_TASK_STATUS_FAILED
	case run.TaskStatusCancelled:
		return statev1.TaskStatus_TASK_STATUS_CANCELLED
	case run.TaskStatusSkipped:
		return statev1.TaskStatus_TASK_STATUS_SKIPPED
	default:
		return statev1.TaskStatus_TASK_STATUS_UNSPECIFIED
	}
}

// domainToProtoTask converts domain model to proto message
func domainToProtoTask(t *postgres.TaskTracker) *statev1.Task {
	task := &statev1.Task{
		TaskId:      t.TaskID.String(),
		ScheduleId:  t.ScheduleID.String(),
		CreatedAt:   timestamppb.New(t.CreatedAt),
		ServiceName: t.ServiceName,
		SchemaName:  t.SchemaName,
		TableName:   t.TableName,
		JobName:     t.JobName,
		Status:      domainToProtoTaskStatus(t.Status),
		RetryCount:  num.ClampInt32(t.RetryCount),
		MaxRetries:  num.ClampInt32(t.MaxRetries),
	}

	if t.CancelledAt != nil {
		task.CancelledAt = timestamppb.New(*t.CancelledAt)
	}

	if t.CancelledBy != nil {
		task.CancelledBy = *t.CancelledBy
	}

	return task
}
