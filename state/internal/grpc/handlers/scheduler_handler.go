package handlers

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/model"
	schedulerpkg "github.com/carolsimone/continuo/state/internal/scheduler"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SchedulerHandler handles all scheduler-related gRPC requests.
type SchedulerHandler struct {
	repo            postgres.SchedulerTrackerRepository
	activate        *svchandlers.ActivateScheduleHandler
	catalogRepo     postgres.ScheduleCatalogRepository
	schedulesConfig *schedulerpkg.SchedulesConfig // may be nil
	uowFactory      func() uow.UnitOfWork
	logger          *slog.Logger
}

// NewSchedulerHandler creates a new SchedulerHandler.
func NewSchedulerHandler(
	repo postgres.SchedulerTrackerRepository,
	activate *svchandlers.ActivateScheduleHandler,
	catalogRepo postgres.ScheduleCatalogRepository,
	schedulesConfig *schedulerpkg.SchedulesConfig,
	uowFactory func() uow.UnitOfWork,
	logger *slog.Logger,
) *SchedulerHandler {
	return &SchedulerHandler{
		repo:            repo,
		activate:        activate,
		catalogRepo:     catalogRepo,
		schedulesConfig: schedulesConfig,
		uowFactory:      uowFactory,
		logger:          logger,
	}
}

// CreateScheduler creates a new scheduler.
func (h *SchedulerHandler) CreateScheduler(ctx context.Context, req *statev1.CreateSchedulerRequest) (*statev1.SchedulerResponse, error) {
	h.logger.Info("Creating scheduler", "schedule_name", req.ScheduleName)

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

	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}

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

// GetScheduler retrieves a scheduler by ID.
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

// CancelScheduler cancels a run by its schedule_id UUID. It loads the Run
// aggregate, calls Run.Cancel, and persists the result via SaveRun and
// OutboxPublisher.Append inside a single transaction.
func (h *SchedulerHandler) CancelScheduler(ctx context.Context, req *statev1.CancelSchedulerRequest) (*statev1.SchedulerResponse, error) {
	h.logger.Info("Cancelling scheduler", "schedule_id", req.ScheduleId, "cancelled_by", req.CancelledBy)

	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}
	scheduleID, err := uuid.Parse(req.ScheduleId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id format")
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

	r, err := u.Run().LoadRunForUpdate(ctx, u.Tx(), scheduleID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "scheduler not found")
		}
		return nil, status.Errorf(codes.Internal, "load: %v", err)
	}
	domainEvents, err := r.Cancel(ctx, u.TaskCollection(), req.CancelledBy, req.CancellationReason, u.Clock().Now())
	if err != nil {
		if errors.Is(err, run.ErrAlreadyTerminal) {
			return nil, status.Errorf(codes.FailedPrecondition, "scheduler already in terminal state")
		}
		return nil, status.Errorf(codes.Internal, "cancel: %v", err)
	}
	if err := u.Run().SaveRun(ctx, u.Tx(), r); err != nil {
		return nil, status.Errorf(codes.Internal, "save: %v", err)
	}
	if err := u.Outbox().Append(ctx, u.Tx(), domainEvents, uuid.Nil); err != nil {
		return nil, status.Errorf(codes.Internal, "outbox: %v", err)
	}
	if err := u.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}
	committed = true

	return &statev1.SchedulerResponse{Scheduler: runToProto(r)}, nil
}

// CancelSchedule cancels the active run of a named schedule. It looks up the
// active run, then loads its Run aggregate, calls Run.Cancel, and persists the
// result via SaveRun and OutboxPublisher.Append inside a single transaction.
func (h *SchedulerHandler) CancelSchedule(
	ctx context.Context,
	req *statev1.CancelScheduleRequest,
) (*statev1.CancelScheduleResponse, error) {
	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}

	// Look up the active run for the named schedule via a separate UoW
	// (read-only path, no transaction needed).
	lookup := h.uowFactory()
	active, err := lookup.Run().GetActiveScheduler(ctx, req.ScheduleName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get active: %v", err)
	}
	if active == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no active run for schedule %q", req.ScheduleName)
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

	r, err := u.Run().LoadRunForUpdate(ctx, u.Tx(), active.ScheduleID())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load: %v", err)
	}
	domainEvents, err := r.Cancel(ctx, u.TaskCollection(), req.CancelledBy, req.CancellationReason, u.Clock().Now())
	if err != nil {
		if errors.Is(err, run.ErrAlreadyTerminal) {
			return nil, status.Errorf(codes.FailedPrecondition, "schedule run already in terminal state")
		}
		return nil, status.Errorf(codes.Internal, "cancel: %v", err)
	}
	if err := u.Run().SaveRun(ctx, u.Tx(), r); err != nil {
		return nil, status.Errorf(codes.Internal, "save: %v", err)
	}
	if err := u.Outbox().Append(ctx, u.Tx(), domainEvents, uuid.Nil); err != nil {
		return nil, status.Errorf(codes.Internal, "outbox: %v", err)
	}
	if err := u.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}
	committed = true

	h.logger.Info("Schedule cancelled", "schedule_name", req.ScheduleName, "schedule_id", r.ScheduleID())
	return &statev1.CancelScheduleResponse{ScheduleId: r.ScheduleID().String()}, nil
}

// ============================================================================
// CONVERSION HELPERS
// ============================================================================

// protoToDomainSchedulerStatus converts proto status to domain model status.
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

// domainToProtoSchedulerStatus converts domain model status to proto status.
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

// domainToProtoScheduler converts a SchedulerTracker domain model to its proto
// representation. Used by non-cancel methods (CreateScheduler, GetScheduler)
// that still work against the low-level tracker repo.
func domainToProtoScheduler(t *model.SchedulerTracker) *statev1.Scheduler {
	s := &statev1.Scheduler{
		ScheduleId:           t.ScheduleID.String(),
		ScheduleName:         t.ScheduleName,
		Status:               domainToProtoSchedulerStatus(t.Status),
		CreatedAt:            timestamppb.New(t.CreatedAt),
		InitializationStatus: t.InitializationStatus,
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

	return s
}

// runToProto converts a Run aggregate to its proto Scheduler representation.
// Used by cancel methods that operate on the Run aggregate directly.
func runToProto(r *run.Run) *statev1.Scheduler {
	s := &statev1.Scheduler{
		ScheduleId:           r.ScheduleID().String(),
		ScheduleName:         r.ScheduleName(),
		Status:               domainToProtoSchedulerStatus(r.Status()),
		CreatedAt:            timestamppb.New(r.CreatedAt()),
		InitializationStatus: string(r.InitializationStatus()),
	}
	if r.StartedAt() != nil {
		s.StartedAt = timestamppb.New(*r.StartedAt())
	}
	if r.CompletedAt() != nil {
		s.CompletedAt = timestamppb.New(*r.CompletedAt())
	}
	if r.LastHeartbeatAt() != nil {
		s.LastHeartbeatAt = timestamppb.New(*r.LastHeartbeatAt())
	}
	if r.CancelledAt() != nil {
		s.CancelledAt = timestamppb.New(*r.CancelledAt())
	}
	if r.CancelledBy() != nil {
		s.CancelledBy = *r.CancelledBy()
	}
	if r.CancellationReason() != nil {
		s.CancellationReason = *r.CancellationReason()
	}
	return s
}

// ActivateSchedule triggers a cron schedule run. Returns the new schedule_id,
// or an empty string when a run was already active (silent skip for cron).
func (h *SchedulerHandler) ActivateSchedule(
	ctx context.Context,
	req *statev1.ActivateScheduleRequest,
) (*statev1.ActivateScheduleResponse, error) {
	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}
	u := h.uowFactory()
	id, outcome, err := h.activate.Handle(ctx, u, req.ScheduleName, run.KindCron, nil)
	if err != nil {
		switch {
		case errors.Is(err, run.ErrScheduleNotInCatalog):
			return nil, status.Errorf(codes.NotFound, "schedule %q not in catalog", req.ScheduleName)
		default:
			return nil, status.Errorf(codes.Internal, "activate: %v", err)
		}
	}
	if outcome == svchandlers.OutcomeSkippedActive {
		return &statev1.ActivateScheduleResponse{ScheduleId: ""}, nil
	}
	return &statev1.ActivateScheduleResponse{ScheduleId: id.String()}, nil
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

	// Build cron config lookup from in-memory schedules config.
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

// TriggerSchedule manually triggers a schedule run. Returns FailedPrecondition
// when a run is already active, NotFound when the schedule is not in catalog.
func (h *SchedulerHandler) TriggerSchedule(
	ctx context.Context,
	req *statev1.TriggerScheduleRequest,
) (*statev1.TriggerScheduleResponse, error) {
	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}
	u := h.uowFactory()
	id, outcome, err := h.activate.Handle(ctx, u, req.ScheduleName, run.KindCron, nil)
	if err != nil {
		switch {
		case errors.Is(err, run.ErrScheduleNotInCatalog):
			return nil, status.Errorf(codes.NotFound, "schedule %q not in catalog", req.ScheduleName)
		default:
			return nil, status.Errorf(codes.Internal, "trigger: %v", err)
		}
	}
	if outcome == svchandlers.OutcomeSkippedActive {
		return nil, status.Errorf(codes.FailedPrecondition, "schedule %q already has an active run", req.ScheduleName)
	}
	return &statev1.TriggerScheduleResponse{ScheduleId: id.String()}, nil
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
