package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SingleNodeRunHandler implements the TriggerSingleNodeRun gRPC method.
type SingleNodeRunHandler struct {
	db            *sqlx.DB
	schedulerRepo postgres.SchedulerTrackerRepository
	taskRepo      postgres.TaskTrackerRepository
	outboxRepo    postgres.OutboxRepository
	logger        *slog.Logger
}

// NewSingleNodeRunHandler creates a new SingleNodeRunHandler.
func NewSingleNodeRunHandler(
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	taskRepo postgres.TaskTrackerRepository,
	outboxRepo postgres.OutboxRepository,
	logger *slog.Logger,
) *SingleNodeRunHandler {
	return &SingleNodeRunHandler{
		db:            db,
		schedulerRepo: schedulerRepo,
		taskRepo:      taskRepo,
		outboxRepo:    outboxRepo,
		logger:        logger,
	}
}

// TriggerSingleNodeRun synthesises a new single-task run and writes a
// trigger.single_node_run:v1 outbox entry — all in a single Postgres transaction.
func (h *SingleNodeRunHandler) TriggerSingleNodeRun(ctx context.Context, req *statev1.TriggerSingleNodeRunRequest) (*statev1.TriggerSingleNodeRunResponse, error) {
	h.logger.Info("TriggerSingleNodeRun called",
		"service_name", req.ServiceName,
		"schema_name", req.SchemaName,
		"table_name", req.TableName,
		"metadata_source", req.MetadataSource,
	)

	// 1. Validate identity fields.
	if req.ServiceName == "" || req.SchemaName == "" || req.TableName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "service_name, schema_name, and table_name are required")
	}

	// 2. Validate metadata_source and source_run_id consistency.
	switch req.MetadataSource {
	case "latest":
		if req.SourceRunId != "" {
			return nil, status.Errorf(codes.InvalidArgument, "source_run_id must be empty when metadata_source is 'latest'")
		}
	case "snapshot_of_run":
		if req.SourceRunId == "" {
			return nil, status.Errorf(codes.InvalidArgument, "source_run_id is required when metadata_source is 'snapshot_of_run'")
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "metadata_source must be 'latest' or 'snapshot_of_run', got %q", req.MetadataSource)
	}

	// 3. Stale-mode: validate source run exists, is terminal, and has a matching task.
	var sourceRunUUID *uuid.UUID
	if req.MetadataSource == "snapshot_of_run" {
		parsed, err := uuid.Parse(req.SourceRunId)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid source_run_id format")
		}
		sourceRunUUID = &parsed

		src, err := h.schedulerRepo.GetByID(ctx, parsed)
		if err != nil {
			if errors.Is(err, postgres.ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "source run not found")
			}
			h.logger.Error("failed to get source run", "source_run_id", parsed, "error", err)
			return nil, status.Errorf(codes.Internal, "internal error")
		}
		if !isTerminal(src.Status) {
			return nil, status.Errorf(codes.FailedPrecondition, "source run is not in a terminal state")
		}

		_, err = h.taskRepo.GetByScheduleAndNode(ctx, parsed, req.ServiceName, req.SchemaName, req.TableName)
		if err != nil {
			if errors.Is(err, postgres.ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "source run does not have a task matching the identity")
			}
			h.logger.Error("failed to look up source task", "error", err)
			return nil, status.Errorf(codes.Internal, "internal error")
		}
	}

	// 4. Synthesise schedule_id + schedule_name.
	scheduleID := uuid.New()
	shortID := strings.ReplaceAll(scheduleID.String(), "-", "")[:8]
	scheduleName := "single-node-run-" + shortID

	// 5. Atomic write: scheduler_tracker + outbox in a single transaction.
	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		h.logger.Error("failed to begin tx", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	defer tx.Rollback()

	now := time.Now()
	tracker := &model.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         scheduleName,
		Status:               model.SchedulerStatusRunning,
		CreatedAt:            now,
		LastHeartbeatAt:      &now,
		Kind:                 "single_node_run",
		SourceRunID:          sourceRunUUID,
		InitializationStatus: "pending",
	}
	if err := h.schedulerRepo.CreateTx(ctx, tx, tracker); err != nil {
		h.logger.Error("failed to insert scheduler_tracker", "error", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	sourceIDStr := ""
	if sourceRunUUID != nil {
		sourceIDStr = sourceRunUUID.String()
	}
	payload, _ := json.Marshal(map[string]string{
		"schedule_id":     scheduleID.String(),
		"schedule_name":   scheduleName,
		"service_name":    req.ServiceName,
		"schema_name":     req.SchemaName,
		"table_name":      req.TableName,
		"kind":            "single_node_run",
		"metadata_source": req.MetadataSource,
		"source_run_id":   sourceIDStr,
	})
	if err := h.outboxRepo.Create(ctx, tx, &postgres.OutboxEntry{
		ID:            uuid.New(),
		AggregateType: "scheduler",
		AggregateID:   scheduleID,
		EventType:     "single_node_run",
		Payload:       payload,
		StreamName:    "trigger.single_node_run:v1",
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

	return &statev1.TriggerSingleNodeRunResponse{
		RunId:        scheduleID.String(),
		ScheduleName: scheduleName,
	}, nil
}

// isTerminal returns true for SUCCEEDED, FAILED, CANCELLED.
func isTerminal(s model.SchedulerStatus) bool {
	switch s {
	case model.SchedulerStatusSucceeded,
		model.SchedulerStatusFailed,
		model.SchedulerStatusCancelled:
		return true
	}
	return false
}
