package handlers

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RerunHandler implements the TriggerRerun gRPC method.
//
// TriggerRerun mints a new scheduler_tracker row on the source's schedule
// (kind='rerun', source_run_id=<source>); the source row stays at its
// terminal status as an immutable historical record. The new run reuses
// the source's schedule_name so it appears in the same schedule's history.
// Selection of which tasks to re-execute happens in the orchestrator via
// Snapshot(SourcePinnedDAG{}) — non-SUCCEEDED source tasks + descendants in
// the source's pinned :EXECUTES set.
type RerunHandler struct {
	db            *sqlx.DB
	schedulerRepo postgres.SchedulerTrackerRepository
	taskRepo      postgres.TaskTrackerRepository
	outboxRepo    postgres.OutboxRepository
	logger        *slog.Logger
}

func NewRerunHandler(
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	taskRepo postgres.TaskTrackerRepository,
	outboxRepo postgres.OutboxRepository,
	logger *slog.Logger,
) *RerunHandler {
	return &RerunHandler{db: db, schedulerRepo: schedulerRepo, taskRepo: taskRepo, outboxRepo: outboxRepo, logger: logger}
}

// TriggerRerun validates the request and delegates to synthesiseDerivedRun
// with rerun-specific spec constants (kind='rerun', stream='trigger.rerun:v1').
func (h *RerunHandler) TriggerRerun(ctx context.Context, req *statev1.TriggerRerunRequest) (*statev1.TriggerRerunResponse, error) {
	h.logger.Info("TriggerRerun called", "source_run_id", req.SourceRunId)

	if req.SourceRunId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "source_run_id is required")
	}
	srcID, err := uuid.Parse(req.SourceRunId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid source_run_id format")
	}

	newID, scheduleName, err := synthesiseDerivedRun(
		ctx, h.db, h.schedulerRepo, h.taskRepo, h.outboxRepo, h.logger, srcID,
		derivedRunSpec{Kind: "rerun", StreamName: "trigger.rerun:v1", EventType: "rerun"},
	)
	if err != nil {
		return nil, err
	}
	return &statev1.TriggerRerunResponse{
		RunId:        newID.String(),
		ScheduleName: scheduleName,
	}, nil
}
