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
//
// PR2 unification: TriggerRerun now mints a NEW scheduler_tracker row on the
// source's schedule (kind='rerun', source_run_id=<source>) rather than
// mutating the source row in place. The source row stays at its terminal
// status forever — it's an immutable historical record. The new run reuses
// the source's schedule_name so it appears in the same schedule's history.
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

// TriggerRerun creates a new run on the source's schedule that re-executes
// the target failed task (and its cascade-skipped descendants) against the
// source's pinned (image_tag, manifest_version). Inherits SUCCEEDED tasks
// at their pinned metadata. Atomic: new scheduler_tracker row + trigger.rerun:v1
// outbox entry in a single Postgres transaction.
//
// Eligibility:
//   - Source schedule must exist and be in {FAILED, CANCELLED}.
//   - Target task must exist on the source and be in {FAILED, CANCELLED}.
//   - No active run on the source's schedule_name (concurrency guard).
func (h *RerunHandler) TriggerRerun(ctx context.Context, req *statev1.TriggerRerunRequest) (*statev1.TriggerRerunResponse, error) {
	h.logger.Info("TriggerRerun called",
		"schedule_id", req.ScheduleId,
		"target", req.ServiceName+"."+req.Schema+"."+req.TableName,
	)

	// ── Validate inputs ──────────────────────────────────────────────────────
	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}
	if req.ServiceName == "" || req.Schema == "" || req.TableName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "service_name, schema, and table_name are required")
	}
	srcID, err := uuid.Parse(req.ScheduleId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id format")
	}

	// ── Eligibility: source must exist and be terminal ───────────────────────
	src, err := h.schedulerRepo.GetByID(ctx, srcID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "source run not found")
		}
		h.logger.Error("failed to get source scheduler", "schedule_id", srcID, "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	if src.Status != model.SchedulerStatusFailed && src.Status != model.SchedulerStatusCancelled {
		return nil, status.Errorf(codes.FailedPrecondition,
			"source run must be FAILED or CANCELLED, got %s", src.Status)
	}

	// ── Eligibility: target task exists and is FAILED/CANCELLED ──────────────
	targetTask, err := h.taskRepo.GetByScheduleAndNode(ctx, srcID, req.ServiceName, req.Schema, req.TableName)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "target task not found on source run")
		}
		h.logger.Error("failed to get target task", "schedule_id", srcID, "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	if targetTask.Status != model.TaskStatusFailed && targetTask.Status != model.TaskStatusCancelled {
		return nil, status.Errorf(codes.FailedPrecondition,
			"target task must be FAILED or CANCELLED, got %s", targetTask.Status)
	}

	// ── Concurrency guard: no active run on the same schedule_name ──────────
	hasActive, err := h.schedulerRepo.HasActiveSchedule(ctx, src.ScheduleName)
	if err != nil {
		h.logger.Error("HasActiveSchedule failed", "schedule_name", src.ScheduleName, "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	if hasActive {
		return nil, status.Errorf(codes.FailedPrecondition,
			"an active run already exists on schedule %q", src.ScheduleName)
	}

	// ── Atomic write: new scheduler_tracker row + outbox entry ──────────────
	newID := uuid.New()
	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		h.logger.Error("failed to begin tx", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	defer tx.Rollback() //nolint:errcheck

	now := time.Now()
	tracker := &model.SchedulerTracker{
		ScheduleID:           newID,
		ScheduleName:         src.ScheduleName,
		Status:               model.SchedulerStatusPending,
		CreatedAt:            now,
		LastHeartbeatAt:      &now,
		Kind:                 "rerun",
		SourceRunID:          &srcID,
		InitializationStatus: "pending",
	}
	if err := h.schedulerRepo.CreateTx(ctx, tx, tracker); err != nil {
		h.logger.Error("failed to create scheduler_tracker", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	payload, _ := json.Marshal(map[string]string{
		"schedule_id":   newID.String(),
		"schedule_name": src.ScheduleName,
		"kind":          "rerun",
		"source_run_id": srcID.String(),
		"service_name":  req.ServiceName,
		"schema_name":   req.Schema,
		"table_name":    req.TableName,
	})
	if err := h.outboxRepo.Create(ctx, tx, &postgres.OutboxEntry{
		ID:            uuid.New(),
		AggregateType: "scheduler",
		AggregateID:   newID,
		EventType:     "rerun",
		Payload:       payload,
		StreamName:    "trigger.rerun:v1",
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

	return &statev1.TriggerRerunResponse{
		RunId:        newID.String(),
		ScheduleName: src.ScheduleName,
	}, nil
}
