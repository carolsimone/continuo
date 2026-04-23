package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	schedulerpkg "github.com/carolsimone/continuo/state/internal/scheduler"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SchedulerHandler handles all scheduler-related gRPC requests
type SchedulerHandler struct {
	repo            postgres.SchedulerTrackerRepository
	activator       schedulerpkg.ScheduleActivator
	catalogRepo     postgres.ScheduleCatalogRepository
	schedulesConfig *schedulerpkg.SchedulesConfig // may be nil
	logger          *slog.Logger
	// Cancel dependencies — injected after construction via WithCancelDeps.
	db         *sqlx.DB
	taskRepo   postgres.TaskTrackerRepository
	outboxRepo postgres.OutboxRepository
}

// NewSchedulerHandler creates a new SchedulerHandler
func NewSchedulerHandler(
	repo postgres.SchedulerTrackerRepository,
	activator schedulerpkg.ScheduleActivator,
	catalogRepo postgres.ScheduleCatalogRepository,
	schedulesConfig *schedulerpkg.SchedulesConfig,
	logger *slog.Logger,
) *SchedulerHandler {
	return &SchedulerHandler{
		repo:            repo,
		activator:       activator,
		catalogRepo:     catalogRepo,
		schedulesConfig: schedulesConfig,
		logger:          logger,
	}
}

// WithCancelDeps injects the dependencies needed by CancelSchedule.
func (h *SchedulerHandler) WithCancelDeps(db *sqlx.DB, taskRepo postgres.TaskTrackerRepository, outboxRepo postgres.OutboxRepository) {
	h.db = db
	h.taskRepo = taskRepo
	h.outboxRepo = outboxRepo
}

// CreateScheduler creates a new scheduler
func (h *SchedulerHandler) CreateScheduler(ctx context.Context, req *statev1.CreateSchedulerRequest) (*statev1.SchedulerResponse, error) {
	h.logger.Info("Creating scheduler", "schedule_name", req.ScheduleName)

	// Parse or generate UUID
	var scheduleID uuid.UUID
	var err error
	if req.ScheduleId != "" {
		scheduleID, err = uuid.Parse(req.ScheduleId)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id: %v", err)
		}
	} else {
		scheduleID = uuid.New()
	}

	// Validate schedule name
	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}

	// Convert proto status to domain model (default to pending if not specified)
	domainStatus := protoToDomainSchedulerStatus(req.Status)
	if req.Status == statev1.SchedulerStatus_SCHEDULER_STATUS_UNSPECIFIED {
		domainStatus = model.SchedulerStatusPending
	}

	tracker := &model.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         req.ScheduleName,
		Status:               domainStatus,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
	}

	if err := h.repo.Create(ctx, tracker); err != nil {
		if err == postgres.ErrDuplicateKey {
			return nil, status.Errorf(codes.AlreadyExists, "scheduler with id %s already exists", scheduleID)
		}
		h.logger.Error("Failed to create scheduler", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to create scheduler")
	}

	return &statev1.SchedulerResponse{
		Scheduler: domainToProtoScheduler(tracker),
	}, nil
}

// GetScheduler retrieves a scheduler by ID
func (h *SchedulerHandler) GetScheduler(ctx context.Context, req *statev1.GetSchedulerRequest) (*statev1.SchedulerResponse, error) {
	h.logger.Debug("Getting scheduler", "schedule_id", req.ScheduleId)

	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}

	scheduleID, err := uuid.Parse(req.ScheduleId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id format")
	}

	tracker, err := h.repo.GetByID(ctx, scheduleID)
	if err != nil {
		if err == postgres.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "scheduler not found")
		}
		h.logger.Error("Failed to get scheduler", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get scheduler")
	}

	return &statev1.SchedulerResponse{
		Scheduler: domainToProtoScheduler(tracker),
	}, nil
}

// CancelScheduler cancels a scheduler
func (h *SchedulerHandler) CancelScheduler(ctx context.Context, req *statev1.CancelSchedulerRequest) (*statev1.SchedulerResponse, error) {
	h.logger.Info("Cancelling scheduler", "schedule_id", req.ScheduleId, "cancelled_by", req.CancelledBy)

	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}

	scheduleID, err := uuid.Parse(req.ScheduleId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id format")
	}

	if err := h.repo.Cancel(ctx, scheduleID, req.CancelledBy, req.CancellationReason); err != nil {
		if errors.Is(err, postgres.ErrNotCancellable) {
			return nil, status.Errorf(codes.FailedPrecondition, "scheduler not found or already in terminal state")
		}
		h.logger.Error("Failed to cancel scheduler", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to cancel scheduler: %v", err)
	}

	// Fetch updated scheduler
	tracker, err := h.repo.GetByID(ctx, scheduleID)
	if err != nil {
		h.logger.Error("Failed to fetch cancelled scheduler", "error", err)
		return nil, status.Errorf(codes.Internal, "scheduler cancelled but failed to fetch updated state")
	}

	return &statev1.SchedulerResponse{
		Scheduler: domainToProtoScheduler(tracker),
	}, nil
}

// ============================================================================
// CONVERSION HELPERS
// ============================================================================

// protoToDomainSchedulerStatus converts proto status to domain model status
func protoToDomainSchedulerStatus(s statev1.SchedulerStatus) model.SchedulerStatus {
	switch s {
	case statev1.SchedulerStatus_SCHEDULER_STATUS_PENDING:
		return model.SchedulerStatusPending
	case statev1.SchedulerStatus_SCHEDULER_STATUS_RUNNING:
		return model.SchedulerStatusRunning
	case statev1.SchedulerStatus_SCHEDULER_STATUS_SUCCEEDED:
		return model.SchedulerStatusSucceeded
	case statev1.SchedulerStatus_SCHEDULER_STATUS_FAILED:
		return model.SchedulerStatusFailed
	case statev1.SchedulerStatus_SCHEDULER_STATUS_CANCELLED:
		return model.SchedulerStatusCancelled
	default:
		return model.SchedulerStatusPending
	}
}

// domainToProtoSchedulerStatus converts domain model status to proto status
func domainToProtoSchedulerStatus(s model.SchedulerStatus) statev1.SchedulerStatus {
	switch s {
	case model.SchedulerStatusPending:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_PENDING
	case model.SchedulerStatusRunning:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_RUNNING
	case model.SchedulerStatusSucceeded:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_SUCCEEDED
	case model.SchedulerStatusFailed:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_FAILED
	case model.SchedulerStatusCancelled:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_CANCELLED
	default:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_UNSPECIFIED
	}
}

// domainToProtoScheduler converts domain model to proto message
func domainToProtoScheduler(t *model.SchedulerTracker) *statev1.Scheduler {
	s := &statev1.Scheduler{
		ScheduleId:   t.ScheduleID.String(),
		ScheduleName: t.ScheduleName,
		Status:       domainToProtoSchedulerStatus(t.Status),
		CreatedAt:    timestamppb.New(t.CreatedAt),
	}

	if t.StartedAt != nil {
		s.StartedAt = timestamppb.New(*t.StartedAt)
	}

	if t.CompletedAt != nil {
		s.CompletedAt = timestamppb.New(*t.CompletedAt)
	}

	if t.LastHeartbeatAt != nil {
		s.LastHeartbeatAt = timestamppb.New(*t.LastHeartbeatAt)
	}

	if t.CancelledAt != nil {
		s.CancelledAt = timestamppb.New(*t.CancelledAt)
	}

	if t.CancelledBy != nil {
		s.CancelledBy = *t.CancelledBy
	}

	if t.CancellationReason != nil {
		s.CancellationReason = *t.CancellationReason
	}

	s.InitializationStatus = t.InitializationStatus

	return s
}

// ActivateSchedule triggers a schedule run via the standard activator path.
// Returns the new schedule_id, or an empty string if a run was already active.
func (h *SchedulerHandler) ActivateSchedule(
	ctx context.Context,
	req *statev1.ActivateScheduleRequest,
) (*statev1.ActivateScheduleResponse, error) {
	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}

	exists, err := h.catalogRepo.ExistsActive(ctx, req.ScheduleName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "catalog lookup: %v", err)
	}
	if !exists {
		return nil, status.Errorf(codes.NotFound, "schedule %q not found in catalog", req.ScheduleName)
	}

	h.logger.Info("ActivateSchedule called", "schedule_name", req.ScheduleName)

	scheduleID, err := h.activator.ActivateSchedule(ctx, req.ScheduleName)
	if err != nil {
		h.logger.Error("Failed to activate schedule", "schedule_name", req.ScheduleName, "error", err)
		return nil, status.Errorf(codes.Internal, "failed to activate schedule: %v", err)
	}

	return &statev1.ActivateScheduleResponse{
		ScheduleId: scheduleID.String(),
	}, nil
}

// ListAllSchedules returns a summary of all active schedules from the catalog,
// merged with last-run data and cron config metadata.
func (h *SchedulerHandler) ListAllSchedules(
	ctx context.Context,
	req *statev1.ListAllSchedulesRequest,
) (*statev1.ListAllSchedulesResponse, error) {
	names, err := h.catalogRepo.ListActive(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list active schedules: %v", err)
	}

	lastRuns, err := h.repo.GetLastRunPerSchedule(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get last runs: %v", err)
	}

	// Build cron config lookup from in-memory schedules config
	cronLookup := map[string]schedulerpkg.ScheduleEntry{}
	timezone := ""
	if h.schedulesConfig != nil {
		timezone = h.schedulesConfig.Timezone
		for _, e := range h.schedulesConfig.Schedules {
			cronLookup[e.Name] = e
		}
	}

	summaries := make([]*statev1.ScheduleSummary, 0, len(names))
	for _, name := range names {
		s := &statev1.ScheduleSummary{
			ScheduleName: name,
			Timezone:     timezone,
		}
		if entry, ok := cronLookup[name]; ok {
			s.CronExpression = entry.Cron
			s.Description = entry.Description
		}
		if run, ok := lastRuns[name]; ok {
			s.IsRunning = run.IsRunning
			s.LastRunStatus = string(run.Status)
			s.LastRunAt = timestamppb.New(run.CreatedAt)
			s.LastRunId = run.ScheduleID.String()
		}
		summaries = append(summaries, s)
	}

	return &statev1.ListAllSchedulesResponse{Schedules: summaries}, nil
}

// TriggerSchedule manually triggers a schedule run after validating pre-conditions.
func (h *SchedulerHandler) TriggerSchedule(
	ctx context.Context,
	req *statev1.TriggerScheduleRequest,
) (*statev1.TriggerScheduleResponse, error) {
	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}

	// Pre-condition 1: schedule must exist in catalog
	exists, err := h.catalogRepo.ExistsActive(ctx, req.ScheduleName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "catalog lookup: %v", err)
	}
	if !exists {
		return nil, status.Errorf(codes.NotFound, "schedule %q not found", req.ScheduleName)
	}

	// Pre-condition 2: no concurrent run in progress
	active, err := h.repo.HasActiveSchedule(ctx, req.ScheduleName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "active schedule check: %v", err)
	}
	if active {
		return nil, status.Errorf(codes.FailedPrecondition,
			"schedule %q already has an active run", req.ScheduleName)
	}

	scheduleID, err := h.activator.ActivateSchedule(ctx, req.ScheduleName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "activate schedule: %v", err)
	}
	if scheduleID == uuid.Nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"schedule %q already has an active run", req.ScheduleName)
	}

	return &statev1.TriggerScheduleResponse{ScheduleId: scheduleID.String()}, nil
}

// GetSchedulerInitStatus returns the initialization_status for a scheduler.
// Used by orchestrator to guard premature finalization during re-run.
func (h *SchedulerHandler) GetSchedulerInitStatus(ctx context.Context, req *statev1.GetSchedulerInitStatusRequest) (*statev1.GetSchedulerInitStatusResponse, error) {
	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}

	scheduleID, err := uuid.Parse(req.ScheduleId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id format")
	}

	tracker, err := h.repo.GetByID(ctx, scheduleID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "scheduler not found")
		}
		h.logger.Error("Failed to get scheduler for init status", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get scheduler")
	}

	return &statev1.GetSchedulerInitStatusResponse{
		InitializationStatus: tracker.InitializationStatus,
	}, nil
}

// CancelSchedule cancels the active run of a named schedule.
func (h *SchedulerHandler) CancelSchedule(
	ctx context.Context,
	req *statev1.CancelScheduleRequest,
) (*statev1.CancelScheduleResponse, error) {
	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}

	active, err := h.repo.GetActiveScheduler(ctx, req.ScheduleName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get active scheduler: %v", err)
	}
	if active == nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"no active run for schedule %q", req.ScheduleName)
	}

	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck

	if err := h.repo.CancelTx(ctx, tx, active.ScheduleID, req.CancelledBy, req.CancellationReason); err != nil {
		if errors.Is(err, postgres.ErrNotCancellable) {
			return nil, status.Errorf(codes.FailedPrecondition,
				"schedule %q run already in terminal state", req.ScheduleName)
		}
		h.logger.Error("Failed to cancel scheduler", "error", err)
		return nil, status.Errorf(codes.Internal, "cancel scheduler: %v", err)
	}

	if _, err := h.taskRepo.BulkCancelByScheduleIDTx(ctx, tx, active.ScheduleID, req.CancelledBy); err != nil {
		h.logger.Error("Failed to bulk-cancel tasks", "schedule_id", active.ScheduleID, "error", err)
		return nil, status.Errorf(codes.Internal, "bulk cancel tasks: %v", err)
	}

	payload, _ := json.Marshal(map[string]string{
		"schedule_id":   active.ScheduleID.String(),
		"schedule_name": active.ScheduleName,
	})
	if err := h.outboxRepo.Create(ctx, tx, &postgres.OutboxEntry{
		ID:            uuid.New(),
		AggregateType: "scheduler",
		AggregateID:   active.ScheduleID,
		EventType:     "schedule_cancelled",
		Payload:       payload,
		StreamName:    "schedule.cancelled:v1",
		Status:        "pending",
		MaxRetries:    3,
		CreatedAt:     time.Now(),
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "write outbox: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}

	h.logger.Info("Schedule cancelled", "schedule_name", req.ScheduleName, "schedule_id", active.ScheduleID)
	return &statev1.CancelScheduleResponse{ScheduleId: active.ScheduleID.String()}, nil
}
