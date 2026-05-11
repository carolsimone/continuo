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

func NewRebaseHandler(
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	taskRepo postgres.TaskTrackerRepository,
	outboxRepo postgres.OutboxRepository,
	logger *slog.Logger,
) *RebaseHandler {
	return &RebaseHandler{db: db, schedulerRepo: schedulerRepo, taskRepo: taskRepo, outboxRepo: outboxRepo, logger: logger}
}

// TriggerRebase validates the request and delegates to synthesiseDerivedRun
// with rebase-specific spec constants (kind='rebase', stream='trigger.rebase:v1').
func (h *RebaseHandler) TriggerRebase(ctx context.Context, req *statev1.TriggerRebaseRequest) (*statev1.TriggerRebaseResponse, error) {
	h.logger.Info("TriggerRebase called", "source_run_id", req.SourceRunId)

	if req.SourceRunId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "source_run_id is required")
	}
	srcID, err := uuid.Parse(req.SourceRunId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid source_run_id format")
	}

	newID, scheduleName, err := synthesiseDerivedRun(
		ctx, h.db, h.schedulerRepo, h.taskRepo, h.outboxRepo, h.logger, srcID,
		derivedRunSpec{Kind: "rebase", StreamName: "trigger.rebase:v1", EventType: "rebase"},
	)
	if err != nil {
		return nil, err
	}
	return &statev1.TriggerRebaseResponse{
		RunId:        newID.String(),
		ScheduleName: scheduleName,
	}, nil
}
