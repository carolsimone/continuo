package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// derivedRunSpec parametrises synthesiseDerivedRun for the two kinds of
// derived runs (rerun and rebase). The two operations share eligibility and
// atomic-write semantics; they differ only in the labels written to
// scheduler_tracker and to the outbox.
type derivedRunSpec struct {
	Kind       string // "rerun" | "rebase"
	StreamName string // "trigger.rerun:v1" | "trigger.rebase:v1"
	EventType  string // "rerun" | "rebase"
}

// synthesiseDerivedRun runs the shared eligibility + atomic-write flow for
// rerun and rebase:
//
//  1. source_run_id parses and the source row exists.
//  2. source is in {FAILED, CANCELLED}.
//  3. source has ≥1 non-SUCCEEDED task.
//  4. no active run on source.schedule_name.
//  5. atomic: new scheduler_tracker (kind=spec.Kind, source_run_id=src,
//     status=PENDING) + outbox entry on spec.StreamName.
//
// Returns the new run's UUID and the inherited schedule_name. Errors are
// gRPC status errors with codes from the standard library, callers
// surface them verbatim.
func synthesiseDerivedRun(
	ctx context.Context,
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	taskRepo postgres.TaskTrackerRepository,
	outboxRepo postgres.OutboxRepository,
	logger *slog.Logger,
	srcRunID uuid.UUID,
	spec derivedRunSpec,
) (uuid.UUID, string, error) {
	src, err := schedulerRepo.GetByID(ctx, srcRunID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return uuid.Nil, "", status.Errorf(codes.NotFound, "source run not found")
		}
		logger.Error("get source", "source_run_id", srcRunID, "error", err)
		return uuid.Nil, "", status.Errorf(codes.Internal, "internal error")
	}

	if src.Status != model.SchedulerStatusFailed && src.Status != model.SchedulerStatusCancelled {
		return uuid.Nil, "", status.Errorf(codes.FailedPrecondition,
			"source run must be FAILED or CANCELLED, got %s", src.Status)
	}

	hasNonSucceeded, err := taskRepo.HasNonSucceededTask(ctx, srcRunID)
	if err != nil {
		logger.Error("check non-succeeded tasks", "source_run_id", srcRunID, "error", err)
		return uuid.Nil, "", status.Errorf(codes.Internal, "internal error")
	}
	if !hasNonSucceeded {
		return uuid.Nil, "", status.Errorf(codes.FailedPrecondition,
			"source run has no non-SUCCEEDED tasks; nothing to %s", spec.Kind)
	}

	hasActive, err := schedulerRepo.HasActiveSchedule(ctx, src.ScheduleName)
	if err != nil {
		logger.Error("HasActiveSchedule", "schedule_name", src.ScheduleName, "error", err)
		return uuid.Nil, "", status.Errorf(codes.Internal, "internal error")
	}
	if hasActive {
		return uuid.Nil, "", status.Errorf(codes.FailedPrecondition,
			"an active run already exists on schedule %q", src.ScheduleName)
	}

	newID := uuid.New()
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		logger.Error("begin tx", "error", err)
		return uuid.Nil, "", status.Errorf(codes.Internal, "internal error")
	}
	defer tx.Rollback() //nolint:errcheck

	now := time.Now()
	tracker := &model.SchedulerTracker{
		ScheduleID:           newID,
		ScheduleName:         src.ScheduleName,
		Status:               model.SchedulerStatusPending,
		CreatedAt:            now,
		LastHeartbeatAt:      &now,
		Kind:                 spec.Kind,
		SourceRunID:          &srcRunID,
		InitializationStatus: "pending",
	}
	if err := schedulerRepo.CreateTx(ctx, tx, tracker); err != nil {
		logger.Error("create scheduler_tracker", "error", err)
		return uuid.Nil, "", status.Errorf(codes.Internal, "internal error")
	}

	payload, _ := json.Marshal(map[string]string{
		"schedule_id":   newID.String(),
		"schedule_name": src.ScheduleName,
		"kind":          spec.Kind,
		"source_run_id": srcRunID.String(),
	})
	if err := outboxRepo.Create(ctx, tx, &postgres.OutboxEntry{
		ID:            uuid.New(),
		AggregateType: "scheduler",
		AggregateID:   newID,
		EventType:     spec.EventType,
		Payload:       payload,
		StreamName:    spec.StreamName,
		Status:        "pending",
		MaxRetries:    3,
		CreatedAt:     time.Now(),
	}); err != nil {
		logger.Error("write outbox", "error", err)
		return uuid.Nil, "", status.Errorf(codes.Internal, "internal error")
	}

	if err := tx.Commit(); err != nil {
		logger.Error("commit", "error", err)
		return uuid.Nil, "", status.Errorf(codes.Internal, "internal error")
	}

	return newID, src.ScheduleName, nil
}
