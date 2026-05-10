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

// RebaseHandler implements the TriggerRebase gRPC method.
//
// A rebase synthesises a NEW scheduler_tracker row that inherits the source's
// schedule_name, points back to the source via source_run_id, and is marked
// kind='rebase'. The orchestrator (out-of-band, via trigger.rebase:v1 outbox
// fanout) is responsible for partitioning and projecting inherited tasks.
type RebaseHandler struct {
	db            *sqlx.DB
	schedulerRepo postgres.SchedulerTrackerRepository
	taskRepo      postgres.TaskTrackerRepository
	outboxRepo    postgres.OutboxRepository
	logger        *slog.Logger
}

// NewRebaseHandler creates a new RebaseHandler.
func NewRebaseHandler(
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	taskRepo postgres.TaskTrackerRepository,
	outboxRepo postgres.OutboxRepository,
	logger *slog.Logger,
) *RebaseHandler {
	return &RebaseHandler{
		db:            db,
		schedulerRepo: schedulerRepo,
		taskRepo:      taskRepo,
		outboxRepo:    outboxRepo,
		logger:        logger,
	}
}

// TriggerRebase synthesises a new run on the source's schedule and writes a
// trigger.rebase:v1 outbox entry — all in a single Postgres transaction.
//
// Eligibility (umbrella spec §9, §10):
//  1. source_run_id must reference an existing scheduler_tracker row,
//  2. source must be terminal in {FAILED, CANCELLED} (SUCCEEDED is a no-op
//     — nothing to rebase),
//  3. source must have ≥1 non-SUCCEEDED task (otherwise the new run would be
//     a fully-inherited shell — same UI gating rule),
//  4. concurrency guard: no active (PENDING/RUNNING) run on the same
//     schedule_name.
func (h *RebaseHandler) TriggerRebase(ctx context.Context, req *statev1.TriggerRebaseRequest) (*statev1.TriggerRebaseResponse, error) {
	h.logger.Info("TriggerRebase called", "source_run_id", req.SourceRunId)

	if req.SourceRunId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "source_run_id is required")
	}
	srcID, err := uuid.Parse(req.SourceRunId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid source_run_id format")
	}

	// 1. Source run must exist.
	src, err := h.schedulerRepo.GetByID(ctx, srcID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "source run not found")
		}
		h.logger.Error("get source", "source_run_id", srcID, "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	// 2. Source must be FAILED or CANCELLED (umbrella §9). SUCCEEDED runs
	//    have nothing to rebase; PENDING/RUNNING are not terminal yet.
	if src.Status != model.SchedulerStatusFailed && src.Status != model.SchedulerStatusCancelled {
		return nil, status.Errorf(codes.FailedPrecondition,
			"source run must be FAILED or CANCELLED, got %s", src.Status)
	}

	// 3. Source must have ≥1 non-SUCCEEDED task (umbrella §10 UI rule).
	hasNonSucceeded, err := h.taskRepo.HasNonSucceededTask(ctx, srcID)
	if err != nil {
		h.logger.Error("check non-succeeded tasks", "source_run_id", srcID, "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	if !hasNonSucceeded {
		return nil, status.Errorf(codes.FailedPrecondition,
			"source run has no non-SUCCEEDED tasks; nothing to rebase")
	}

	// 4. Concurrency guard: no active run on the same schedule_name.
	hasActive, err := h.schedulerRepo.HasActiveSchedule(ctx, src.ScheduleName)
	if err != nil {
		h.logger.Error("HasActiveSchedule", "schedule_name", src.ScheduleName, "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	if hasActive {
		return nil, status.Errorf(codes.FailedPrecondition,
			"an active run already exists on schedule %q", src.ScheduleName)
	}

	// 5. Atomic write: new scheduler_tracker row + outbox entry.
	newID := uuid.New()
	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		h.logger.Error("begin tx", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	defer tx.Rollback()

	now := time.Now()
	tracker := &model.SchedulerTracker{
		ScheduleID:           newID,
		ScheduleName:         src.ScheduleName,
		Status:               model.SchedulerStatusPending,
		CreatedAt:            now,
		LastHeartbeatAt:      &now,
		Kind:                 "rebase",
		SourceRunID:          &srcID,
		InitializationStatus: "pending",
	}
	if err := h.schedulerRepo.CreateTx(ctx, tx, tracker); err != nil {
		h.logger.Error("create scheduler_tracker", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	payload, _ := json.Marshal(map[string]string{
		"schedule_id":   newID.String(),
		"schedule_name": src.ScheduleName,
		"kind":          "rebase",
		"source_run_id": srcID.String(),
	})
	if err := h.outboxRepo.Create(ctx, tx, &postgres.OutboxEntry{
		ID:            uuid.New(),
		AggregateType: "scheduler",
		AggregateID:   newID,
		EventType:     "rebase",
		Payload:       payload,
		StreamName:    "trigger.rebase:v1",
		Status:        "pending",
		MaxRetries:    3,
		CreatedAt:     time.Now(),
	}); err != nil {
		h.logger.Error("write outbox", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	if err := tx.Commit(); err != nil {
		h.logger.Error("commit", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	return &statev1.TriggerRebaseResponse{
		RunId:        newID.String(),
		ScheduleName: src.ScheduleName,
	}, nil
}
