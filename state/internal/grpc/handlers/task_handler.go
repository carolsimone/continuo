package handlers

import (
	"context"
	"errors"
	"log/slog"
	"time"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/model"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TaskHandler handles all task-related gRPC requests
type TaskHandler struct {
	repo       postgres.TaskTrackerRepository
	uowFactory func() uow.UnitOfWork
	logger     *slog.Logger
}

// NewTaskHandler creates a new TaskHandler.
// uowFactory is used by the ResetTask method to load and update the Run
// aggregate within a transaction; other methods use repo directly.
func NewTaskHandler(repo postgres.TaskTrackerRepository, uowFactory func() uow.UnitOfWork, logger *slog.Logger) *TaskHandler {
	return &TaskHandler{
		repo:       repo,
		uowFactory: uowFactory,
		logger:     logger,
	}
}

// CreateTask creates a new task
func (h *TaskHandler) CreateTask(ctx context.Context, req *statev1.CreateTaskRequest) (*statev1.TaskResponse, error) {
	h.logger.Info("Creating task",
		"task_id", req.TaskId,
		"schedule_id", req.ScheduleId,
		"table", req.TableName,
	)

	// Validate required fields
	if req.TaskId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "task_id is required (application-generated)")
	}

	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}

	if req.ServiceName == "" || req.SchemaName == "" || req.TableName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "service_name, schema_name, and table_name are required")
	}

	// Parse UUIDs
	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task_id format: %v", err)
	}

	scheduleID, err := uuid.Parse(req.ScheduleId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id format: %v", err)
	}

	// Convert proto status to domain model (default to pending if not specified)
	domainStatus := protoToDomainTaskStatus(req.Status)
	if req.Status == statev1.TaskStatus_TASK_STATUS_UNSPECIFIED {
		domainStatus = model.TaskStatusPending
	}

	// Compute k8s-compliant job_name from service/schema/table + schedule_id suffix
	jobName, err := pkgDomain.ComputeJobName(req.ServiceName, req.SchemaName, req.TableName, req.ScheduleId)
	if err != nil {
		h.logger.Error("failed to compute job_name", "error", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid service/schema/table names: %v", err)
	}

	task := &model.TaskTracker{
		TaskID:      taskID,
		ScheduleID:  scheduleID,
		CreatedAt:   time.Now(),
		ServiceName: req.ServiceName,
		SchemaName:  req.SchemaName,
		TableName:   req.TableName,
		JobName:     jobName,
		Status:      domainStatus,
		RetryCount:  0,
		MaxRetries:  int(req.MaxRetries),
	}

	if err := h.repo.Create(ctx, task); err != nil {
		if err == postgres.ErrDuplicateKey {
			return nil, status.Errorf(codes.AlreadyExists, "task with id %s already exists", taskID)
		}
		h.logger.Error("Failed to create task", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to create task")
	}

	return &statev1.TaskResponse{
		Task: domainToProtoTask(task),
	}, nil
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

// DeleteTask deletes a task
func (h *TaskHandler) DeleteTask(ctx context.Context, req *statev1.DeleteTaskRequest) (*statev1.DeleteTaskResponse, error) {
	h.logger.Info("Deleting task", "task_id", req.TaskId)

	if req.TaskId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "task_id is required")
	}

	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task_id format")
	}

	if err := h.repo.Delete(ctx, taskID); err != nil {
		if err == postgres.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "task not found")
		}
		h.logger.Error("Failed to delete task", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to delete task")
	}

	return &statev1.DeleteTaskResponse{
		Success: true,
		Message: "Task deleted successfully",
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

	var statusFilter *model.TaskStatus
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
		TotalCount: int32(total),
	}, nil
}

// ResetTask resets a task to PENDING with retry_count=0 via the Run aggregate.
// Authorization and valid-transition enforcement live in Run.ResetTaskToPending.
func (h *TaskHandler) ResetTask(ctx context.Context, req *statev1.ResetTaskRequest) (*statev1.TaskResponse, error) {
	if req.TaskId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "task_id is required")
	}
	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task_id format")
	}
	caller, err := callerFromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "missing caller identity: set x-caller-id metadata")
	}

	// Look up the task to obtain its scheduleID for loading the Run aggregate.
	t, err := h.repo.GetByID(ctx, taskID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "task not found")
		}
		return nil, status.Errorf(codes.Internal, "get task: %v", err)
	}

	u := h.uowFactory()
	if err := u.Begin(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()

	r, err := u.Run().LoadRunForUpdate(ctx, u.Tx(), t.ScheduleID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load run: %v", err)
	}
	updated, err := r.ResetTaskToPending(ctx, u.TaskCollection(), taskID, run.CallerID(caller), u.Clock().Now())
	if err != nil {
		switch {
		case errors.Is(err, run.ErrAlreadyTerminal):
			return nil, status.Errorf(codes.FailedPrecondition, "run is in terminal state")
		case errors.Is(err, run.ErrInvalidTransition):
			return nil, status.Errorf(codes.FailedPrecondition, "cannot reset task in state %s", t.Status)
		case errors.Is(err, run.ErrUnauthorizedTransition):
			return nil, status.Errorf(codes.PermissionDenied, "caller %q is not authorized to reset tasks", caller)
		case errors.Is(err, run.ErrTaskNotFound):
			return nil, status.Errorf(codes.NotFound, "task not found")
		default:
			return nil, status.Errorf(codes.Internal, "reset: %v", err)
		}
	}
	if err := u.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}
	committed = true

	// Carry forward fields not set by ResetTaskToPending so the response is complete.
	updated.ServiceName = t.ServiceName
	updated.SchemaName = t.SchemaName
	updated.TableName = t.TableName
	updated.JobName = t.JobName
	updated.MaxRetries = t.MaxRetries
	updated.CreatedAt = t.CreatedAt
	// CancelledAt/CancelledBy are intentionally cleared by reset; leave nil.

	return &statev1.TaskResponse{Task: runTaskToProto(updated)}, nil
}

// ============================================================================
// CONVERSION HELPERS
// ============================================================================

// runTaskToProto converts a run.Task aggregate value to the proto Task message.
// Used by the ResetTask response path; call sites must fill in any fields not
// populated by the aggregate method before calling this helper.
func runTaskToProto(t run.Task) *statev1.Task {
	out := &statev1.Task{
		TaskId:      t.TaskID.String(),
		ScheduleId:  t.ScheduleID.String(),
		Status:      domainToProtoTaskStatus(t.Status),
		RetryCount:  int32(t.RetryCount),
		MaxRetries:  int32(t.MaxRetries),
		ServiceName: t.ServiceName,
		SchemaName:  t.SchemaName,
		TableName:   t.TableName,
		JobName:     t.JobName,
	}
	if !t.CreatedAt.IsZero() {
		out.CreatedAt = timestamppb.New(t.CreatedAt)
	}
	if t.CancelledAt != nil {
		out.CancelledAt = timestamppb.New(*t.CancelledAt)
	}
	if t.CancelledBy != nil {
		out.CancelledBy = *t.CancelledBy
	}
	return out
}

// protoToDomainTaskStatus converts proto status to domain model status
func protoToDomainTaskStatus(s statev1.TaskStatus) model.TaskStatus {
	switch s {
	case statev1.TaskStatus_TASK_STATUS_PENDING:
		return model.TaskStatusPending
	case statev1.TaskStatus_TASK_STATUS_RUNNING:
		return model.TaskStatusRunning
	case statev1.TaskStatus_TASK_STATUS_SUCCEEDED:
		return model.TaskStatusSucceeded
	case statev1.TaskStatus_TASK_STATUS_FAILED:
		return model.TaskStatusFailed
	case statev1.TaskStatus_TASK_STATUS_CANCELLED:
		return model.TaskStatusCancelled
	default:
		return model.TaskStatusPending
	}
}

// domainToProtoTaskStatus converts domain model status to proto status
func domainToProtoTaskStatus(s model.TaskStatus) statev1.TaskStatus {
	switch s {
	case model.TaskStatusPending:
		return statev1.TaskStatus_TASK_STATUS_PENDING
	case model.TaskStatusRunning:
		return statev1.TaskStatus_TASK_STATUS_RUNNING
	case model.TaskStatusSucceeded:
		return statev1.TaskStatus_TASK_STATUS_SUCCEEDED
	case model.TaskStatusFailed:
		return statev1.TaskStatus_TASK_STATUS_FAILED
	case model.TaskStatusCancelled:
		return statev1.TaskStatus_TASK_STATUS_CANCELLED
	default:
		return statev1.TaskStatus_TASK_STATUS_UNSPECIFIED
	}
}

// domainToProtoTask converts domain model to proto message
func domainToProtoTask(t *model.TaskTracker) *statev1.Task {
	task := &statev1.Task{
		TaskId:      t.TaskID.String(),
		ScheduleId:  t.ScheduleID.String(),
		CreatedAt:   timestamppb.New(t.CreatedAt),
		ServiceName: t.ServiceName,
		SchemaName:  t.SchemaName,
		TableName:   t.TableName,
		JobName:     t.JobName,
		Status:      domainToProtoTaskStatus(t.Status),
		RetryCount:  int32(t.RetryCount),
		MaxRetries:  int32(t.MaxRetries),
	}

	if t.CancelledAt != nil {
		task.CancelledAt = timestamppb.New(*t.CancelledAt)
	}

	if t.CancelledBy != nil {
		task.CancelledBy = *t.CancelledBy
	}

	return task
}
