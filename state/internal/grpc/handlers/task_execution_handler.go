package handlers

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TaskExecutionHandler handles all task_execution-related gRPC requests
type TaskExecutionHandler struct {
	repo   postgres.TaskExecutionRepository
	logger *slog.Logger
}

// NewTaskExecutionHandler creates a new TaskExecutionHandler
func NewTaskExecutionHandler(repo postgres.TaskExecutionRepository, logger *slog.Logger) *TaskExecutionHandler {
	return &TaskExecutionHandler{
		repo:   repo,
		logger: logger,
	}
}

// ListTaskExecutions lists task executions for a given schedule
func (h *TaskExecutionHandler) ListTaskExecutions(ctx context.Context, req *statev1.ListTaskExecutionsRequest) (*statev1.ListTaskExecutionsResponse, error) {
	h.logger.Debug("Listing task executions", "schedule_id", req.ScheduleId)

	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}

	pageSize := int(req.PageSize)
	if pageSize <= 0 {
		pageSize = 500
	}

	executions, total, err := h.repo.ListByScheduleID(ctx, req.ScheduleId, pageSize, int(req.PageOffset))
	if err != nil {
		h.logger.Error("Failed to list task executions", "schedule_id", req.ScheduleId, "error", err)
		return nil, status.Errorf(codes.Internal, "failed to list task executions")
	}

	protoExecs := make([]*statev1.TaskExecution, 0, len(executions))
	for _, e := range executions {
		protoExecs = append(protoExecs, domainToProtoTaskExecution(e))
	}

	return &statev1.ListTaskExecutionsResponse{
		TaskExecutions: protoExecs,
		TotalCount:     int32(total),
	}, nil
}

// GetTaskExecution retrieves a task execution by ID
func (h *TaskExecutionHandler) GetTaskExecution(ctx context.Context, req *statev1.GetTaskExecutionRequest) (*statev1.TaskExecutionResponse, error) {
	h.logger.Debug("Getting task execution", "id", req.Id)

	if req.Id == "" {
		return nil, status.Errorf(codes.InvalidArgument, "id is required")
	}

	executionID, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid id format")
	}

	execution, err := h.repo.GetByID(ctx, executionID)
	if err != nil {
		if err == postgres.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "task_execution not found")
		}
		h.logger.Error("Failed to get task_execution", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get task_execution")
	}

	return &statev1.TaskExecutionResponse{
		TaskExecution: domainToProtoTaskExecution(execution),
	}, nil
}

// ============================================================================
// CONVERSION HELPERS
// ============================================================================

// domainToProtoTaskExecution converts domain model to proto message
func domainToProtoTaskExecution(e *postgres.TaskExecution) *statev1.TaskExecution {
	execution := &statev1.TaskExecution{
		Id:        e.ID.String(),
		TaskId:    e.TaskID.String(),
		CreatedAt: timestamppb.New(e.CreatedAt),
	}

	if e.StartedAt != nil {
		execution.StartedAt = timestamppb.New(*e.StartedAt)
	}

	if e.CompletedAt != nil {
		execution.CompletedAt = timestamppb.New(*e.CompletedAt)
	}

	if e.ExecutionTimeSeconds != nil {
		execution.ExecutionTimeSeconds = *e.ExecutionTimeSeconds
	}

	if e.ExecutorID != nil {
		execution.ExecutorId = *e.ExecutorID
	}

	if e.K8sJobName != nil {
		execution.K8SJobName = *e.K8sJobName
	}

	if e.ErrorMessage != nil {
		execution.ErrorMessage = *e.ErrorMessage
	}

	if e.CancelledAt != nil {
		execution.CancelledAt = timestamppb.New(*e.CancelledAt)
	}

	if e.CancelledBy != nil {
		execution.CancelledBy = *e.CancelledBy
	}

	if e.CancellationReason != nil {
		execution.CancellationReason = *e.CancellationReason
	}

	if e.LogS3Key != nil {
		execution.LogS3Key = *e.LogS3Key
	}

	return execution
}
