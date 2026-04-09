package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// RerunRequest is the body for POST /schedules/{schedule_id}/rerun.
type RerunRequest struct {
	Schema      string `json:"schema"`
	TableName   string `json:"table_name"`
	ServiceName string `json:"service_name"`
}

// RerunHandler handles POST /schedules/{schedule_id}/rerun.
// It atomically resets the scheduler and target task in Postgres and writes an
// outbox entry — all within a single transaction.
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

func (h *RerunHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	scheduleIDStr := r.PathValue("schedule_id")
	scheduleID, err := uuid.Parse(scheduleIDStr)
	if err != nil {
		http.Error(w, "invalid schedule_id", http.StatusBadRequest)
		return
	}

	var req RerunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// 1. Schedule must exist.
	scheduler, err := h.schedulerRepo.GetByID(ctx, scheduleID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			http.Error(w, "schedule not found", http.StatusNotFound)
			return
		}
		h.logger.Error("failed to get scheduler", "schedule_id", scheduleID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// 2. Target task must exist.
	task, err := h.taskRepo.GetByScheduleAndNode(ctx, scheduleID, req.ServiceName, req.Schema, req.TableName)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			http.Error(w, "task not found", http.StatusNotFound)
			return
		}
		h.logger.Error("failed to get task", "schedule_id", scheduleID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
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
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if len(runningTasks) > 0 {
		http.Error(w, "schedule has running tasks", http.StatusConflict)
		return
	}

	// 4. Target task must be FAILED.
	if task.Status != model.TaskStatusFailed {
		http.Error(w, "target task is not in FAILED state", http.StatusUnprocessableEntity)
		return
	}

	// 5. Atomic transaction — reset scheduler, reset task, write outbox.
	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		h.logger.Error("failed to begin tx", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	now := time.Now()
	scheduler.Status = model.SchedulerStatusRunning
	scheduler.CompletedAt = nil
	scheduler.LastHeartbeatAt = &now
	if err := h.schedulerRepo.UpdateTx(ctx, tx, scheduler); err != nil {
		h.logger.Error("failed to reset scheduler", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := h.schedulerRepo.UpdateInitializationStatusTx(ctx, tx, scheduleID, "pending"); err != nil {
		h.logger.Error("failed to reset init status", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	task.Status = model.TaskStatusPending
	task.RetryCount = 0
	if err := h.taskRepo.UpdateTx(ctx, tx, task); err != nil {
		h.logger.Error("failed to reset task", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	payload, _ := json.Marshal(map[string]interface{}{
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
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		h.logger.Error("failed to commit tx", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
