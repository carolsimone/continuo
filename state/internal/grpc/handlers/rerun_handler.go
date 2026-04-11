package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RerunHandler implements the TriggerRerun gRPC method.
type RerunHandler struct {
	db            *sqlx.DB
	schedulerRepo postgres.SchedulerTrackerRepository
	taskRepo      postgres.TaskTrackerRepository
	outboxRepo    postgres.OutboxRepository
	logger        *slog.Logger
}

// NewRerunHandler creates a new RerunHandler.
func NewRerunHandler(
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	taskRepo postgres.TaskTrackerRepository,
	outboxRepo postgres.OutboxRepository,
	logger *slog.Logger,
) *RerunHandler {
	return &RerunHandler{
		db:            db,
		schedulerRepo: schedulerRepo,
		taskRepo:      taskRepo,
		outboxRepo:    outboxRepo,
		logger:        logger,
	}
}

// TriggerRerun atomically resets the scheduler + target task and writes a
// command.rerun:v1 outbox entry — all in a single Postgres transaction.
func (h *RerunHandler) TriggerRerun(ctx context.Context, req *statev1.TriggerRerunRequest) (*statev1.TriggerRerunResponse, error) {
	h.logger.Info("TriggerRerun called", "schedule_id", req.ScheduleId)

	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}

	scheduleID, err := uuid.Parse(req.ScheduleId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id format")
	}

	// 1. Schedule must exist.
	scheduler, err := h.schedulerRepo.GetByID(ctx, scheduleID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "schedule not found")
		}
		h.logger.Error("failed to get scheduler", "schedule_id", scheduleID, "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	// 2. Target task must exist.
	task, err := h.taskRepo.GetByScheduleAndNode(ctx, scheduleID, req.ServiceName, req.Schema, req.TableName)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "task not found")
		}
		h.logger.Error("failed to get task", "schedule_id", scheduleID, "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	// 3. No tasks currently RUNNING.
	runningStatus := model.TaskStatusRunning
	runningTasks, _, err := h.taskRepo.List(ctx, postgres.TaskFilters{
		ScheduleID: &scheduleID,
		Status:     &runningStatus,
		Limit:      1,
		Offset:     0,
	})
	if err != nil {
		h.logger.Error("failed to list running tasks", "schedule_id", scheduleID, "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	if len(runningTasks) > 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "schedule has running tasks")
	}

	// 4. Target task must be FAILED.
	if task.Status != model.TaskStatusFailed {
		return nil, status.Errorf(codes.FailedPrecondition, "target task is not in FAILED state")
	}

	// 5. Atomic transaction — reset scheduler, reset task, write outbox.
	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		h.logger.Error("failed to begin tx", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	defer tx.Rollback()

	now := time.Now()
	scheduler.Status = model.SchedulerStatusRunning
	scheduler.CompletedAt = nil
	scheduler.LastHeartbeatAt = &now
	if err := h.schedulerRepo.UpdateTx(ctx, tx, scheduler); err != nil {
		h.logger.Error("failed to reset scheduler", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	if err := h.schedulerRepo.UpdateInitializationStatusTx(ctx, tx, scheduleID, "pending"); err != nil {
		h.logger.Error("failed to reset init status", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	task.Status = model.TaskStatusPending
	task.RetryCount = 0
	if err := h.taskRepo.UpdateTx(ctx, tx, task); err != nil {
		h.logger.Error("failed to reset task", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	payload, _ := json.Marshal(map[string]string{
		"schedule_id":   scheduleID.String(),
		"schedule_name": scheduler.ScheduleName,
		"scope":         "node",
		"schema":        req.Schema,
		"table_name":    req.TableName,
		"service_name":  req.ServiceName,
	})
	if err := h.outboxRepo.Create(ctx, tx, &postgres.OutboxEntry{
		ID:            uuid.New(),
		AggregateType: "scheduler",
		AggregateID:   scheduleID,
		EventType:     "rerun_node",
		Payload:       payload,
		StreamName:    "command.rerun:v1",
		Status:        "pending",
		MaxRetries:    3,
		CreatedAt:     time.Now(),
	}); err != nil {
		h.logger.Error("failed to write outbox", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	if err := tx.Commit(); err != nil {
		h.logger.Error("failed to commit tx", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	return &statev1.TriggerRerunResponse{}, nil
}
