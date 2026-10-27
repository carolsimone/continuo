package handlers

import (
	"context"
	"errors"
	"log/slog"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/repository"
	schedulerpkg "github.com/carolsimone/continuo/state/internal/scheduler"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// runReads is the part of the runs table the scheduler reads serve: one run
// row by id and the last run of every schedule.
type runReads interface {
	GetByID(ctx context.Context, tenantID string, runID uuid.UUID) (*postgres.RunRow, error)
	GetLastRunPerSchedule(ctx context.Context, tenantID string) (map[string]repository.LastRunSummary, error)
}

// activeSchedules lists the names of a tenant's active schedules.
type activeSchedules interface {
	ListActive(ctx context.Context, tenantID string) ([]string, error)
}

// SchedulerHandler handles all scheduler-related gRPC requests.
type SchedulerHandler struct {
	repo            runReads
	activate        *svchandlers.StartRunHandler
	catalogRepo     activeSchedules
	schedulesConfig *schedulerpkg.SchedulesConfig // may be nil
	uowFactory      func() uow.UnitOfWork
	logger          *slog.Logger
}

// NewSchedulerHandler creates a new SchedulerHandler.
func NewSchedulerHandler(
	repo runReads,
	activate *svchandlers.StartRunHandler,
	catalogRepo activeSchedules,
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

// GetScheduler retrieves a run of the request's tenant by ID; another
// tenant's run is NotFound.
func (h *SchedulerHandler) GetScheduler(ctx context.Context, req *statev1.GetSchedulerRequest) (*statev1.SchedulerResponse, error) {
	h.logger.Debug("Getting scheduler", "schedule_id", req.ScheduleId)
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}

	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}

	scheduleID, err := uuid.Parse(req.ScheduleId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id format")
	}

	tracker, err := h.repo.GetByID(ctx, tenantID, scheduleID)
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

// CancelScheduler cancels a run of the request's tenant by its schedule_id
// UUID; another tenant's run is NotFound. It loads the Run aggregate, calls
// Run.Cancel, and persists the result via SaveRun and OutboxPublisher.Append
// inside a single transaction.
func (h *SchedulerHandler) CancelScheduler(ctx context.Context, req *statev1.CancelSchedulerRequest) (*statev1.SchedulerResponse, error) {
	h.logger.Info("Cancelling scheduler", "schedule_id", req.ScheduleId, "cancelled_by", req.CancelledBy)
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}

	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}
	scheduleID, err := uuid.Parse(req.ScheduleId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id format")
	}

	r, _, gerr := h.cancelRunTx(ctx, cancelParams{
		tenantID:           tenantID,
		scheduleID:         scheduleID,
		cancelledBy:        effectiveCancelledBy(ctx, req.CancelledBy),
		cancellationReason: req.CancellationReason,
		loadNotFoundCode:   codes.NotFound,
		loadNotFoundMsg:    "scheduler not found",
		terminalMsg:        "scheduler already in terminal state",
	})
	if gerr != nil {
		return nil, gerr
	}
	return &statev1.SchedulerResponse{Scheduler: runToProto(r)}, nil
}

// effectiveCancelledBy resolves who to record as the cancelling user. The
// authenticated initiator carried in gRPC metadata is authoritative when it is
// a real user; an unauthenticated/system caller (no metadata) falls back to the
// request's cancelled_by so internal callers that supply their own label keep
// recording it. When neither is present — a platform cancel with no metadata
// and no request label — it resolves to the system sentinel so the
// authoritative runs.cancelled_by row keeps provenance rather
// than going blank.
func effectiveCancelledBy(ctx context.Context, requestCancelledBy string) string {
	id := identity.FromContext(ctx)
	if !id.IsSystem() {
		return id.UserID
	}
	if requestCancelledBy != "" {
		return requestCancelledBy
	}
	return identity.SystemUserID
}

// cancelParams carries the per-call inputs to cancelRunTx. tenantID and
// scheduleID name the run to cancel.
type cancelParams struct {
	tenantID           string
	scheduleID         uuid.UUID
	cancelledBy        string
	cancellationReason string
	// loadNotFoundCode / loadNotFoundMsg control how a LoadRunForUpdate
	// ErrNotFound is reported: CancelScheduler treats it as NotFound (the
	// caller passed a raw schedule_id), whereas CancelSchedule has already
	// resolved an active run and reports a stale lookup as Internal.
	loadNotFoundCode codes.Code
	loadNotFoundMsg  string
	// terminalMsg is the FailedPrecondition message when the run is already
	// terminal.
	terminalMsg string
}

// cancelRunTx runs the shared cancel transaction: begin → LoadRunForUpdate →
// Run.Cancel, under the run's tenant → SaveRun → Outbox.Append → commit. It
// returns the cancelled Run
// aggregate and true, or a ready-to-return gRPC status error. A run that is
// already cancelling is answered as it was loaded, with false: the
// transaction rolls back, so the repeated cancel writes nothing and sends
// nothing. The only behaviour that varies between the two cancel RPCs is
// captured in cancelParams.
func (h *SchedulerHandler) cancelRunTx(ctx context.Context, p cancelParams) (*run.Run, bool, error) {
	u := h.uowFactory()
	if err := u.Begin(ctx); err != nil {
		return nil, false, status.Errorf(codes.Internal, "begin tx: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()

	r, err := u.Run().LoadRunForUpdate(ctx, p.tenantID, p.scheduleID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, false, status.Errorf(p.loadNotFoundCode, "%s", p.loadNotFoundMsg)
		}
		return nil, false, status.Errorf(codes.Internal, "load: %v", err)
	}
	domainEvents, err := r.Cancel(ctx, u.TaskCollection(r.TenantID()), p.cancelledBy, p.cancellationReason, u.Clock().Now())
	if err != nil {
		if errors.Is(err, run.ErrAlreadyTerminal) {
			return nil, false, status.Errorf(codes.FailedPrecondition, "%s", p.terminalMsg)
		}
		if errors.Is(err, run.ErrAlreadyCancelling) {
			return r, false, nil
		}
		return nil, false, status.Errorf(codes.Internal, "cancel: %v", err)
	}
	if err := u.Run().SaveRun(ctx, r); err != nil {
		return nil, false, status.Errorf(codes.Internal, "save: %v", err)
	}
	if err := u.Outbox().Append(ctx, domainEvents, uuid.Nil); err != nil {
		return nil, false, status.Errorf(codes.Internal, "outbox: %v", err)
	}
	if err := u.Commit(); err != nil {
		return nil, false, status.Errorf(codes.Internal, "commit: %v", err)
	}
	committed = true
	return r, true, nil
}

// CancelSchedule cancels the active run of the request's tenant's named
// schedule. It looks up the active run, then loads its Run aggregate under the
// run's tenant, calls Run.Cancel, and persists the result via SaveRun and
// OutboxPublisher.Append inside a single transaction.
func (h *SchedulerHandler) CancelSchedule(
	ctx context.Context,
	req *statev1.CancelScheduleRequest,
) (*statev1.CancelScheduleResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}

	// Look up the active run for the named schedule via a separate UoW
	// (read-only path, no transaction needed).
	lookup := h.uowFactory()
	active, err := lookup.Run().GetActiveRun(ctx, tenantID, req.ScheduleName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get active: %v", err)
	}
	if active == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no active run for schedule %q", req.ScheduleName)
	}

	r, wrote, gerr := h.cancelRunTx(ctx, cancelParams{
		tenantID:           active.TenantID(),
		scheduleID:         active.RunID(),
		cancelledBy:        effectiveCancelledBy(ctx, req.CancelledBy),
		cancellationReason: req.CancellationReason,
		loadNotFoundCode:   codes.Internal,
		loadNotFoundMsg:    "load: run not found",
		terminalMsg:        "schedule run already in terminal state",
	})
	if gerr != nil {
		return nil, gerr
	}

	if wrote {
		h.logger.Info("Schedule cancelled", "schedule_name", req.ScheduleName, "schedule_id", r.RunID())
	} else {
		h.logger.Info("Schedule already cancelling; the repeated cancel wrote nothing",
			"schedule_name", req.ScheduleName, "schedule_id", r.RunID())
	}
	return &statev1.CancelScheduleResponse{ScheduleId: r.RunID().String()}, nil
}

// ============================================================================
// CONVERSION HELPERS
// ============================================================================

// domainToProtoSchedulerStatus converts domain model status to proto status.
func domainToProtoSchedulerStatus(s run.RunStatus) statev1.SchedulerStatus {
	switch s {
	case run.RunStatusPending:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_PENDING
	case run.RunStatusRunning:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_RUNNING
	case run.RunStatusSucceeded:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_SUCCEEDED
	case run.RunStatusFailed:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_FAILED
	case run.RunStatusCancelled:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_CANCELLED
	case run.RunStatusSkipped:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_SKIPPED
	case run.RunStatusCancelling:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_CANCELLING
	default:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_UNSPECIFIED
	}
}

// domainToProtoScheduler converts a RunRow domain model to its proto
// representation. Used by GetScheduler, which works against the low-level
// tracker repo rather than the Run aggregate.
func domainToProtoScheduler(t *postgres.RunRow) *statev1.Scheduler {
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
	if t.LastProgressAt != nil {
		s.LastHeartbeatAt = timestamppb.New(*t.LastProgressAt)
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
	if t.FailureReason != nil {
		s.FailureReason = *t.FailureReason
	}

	return s
}

// runToProto converts a Run aggregate to its proto Scheduler representation.
// Used by cancel methods that operate on the Run aggregate directly.
func runToProto(r *run.Run) *statev1.Scheduler {
	s := &statev1.Scheduler{
		ScheduleId:   r.RunID().String(),
		ScheduleName: r.ScheduleName(),
		Status:       domainToProtoSchedulerStatus(r.Status()),
		CreatedAt:    timestamppb.New(r.CreatedAt()),
	}
	if r.StartedAt() != nil {
		s.StartedAt = timestamppb.New(*r.StartedAt())
	}
	if r.CompletedAt() != nil {
		s.CompletedAt = timestamppb.New(*r.CompletedAt())
	}
	if r.LastProgressAt() != nil {
		s.LastHeartbeatAt = timestamppb.New(*r.LastProgressAt())
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
	if r.FailureReason() != nil {
		s.FailureReason = string(*r.FailureReason())
	}
	return s
}

// ActivateSchedule triggers a cron run of the request's tenant's schedule.
// Returns the new schedule_id, or an empty string when a run was already
// active (silent skip for cron).
func (h *SchedulerHandler) ActivateSchedule(
	ctx context.Context,
	req *statev1.ActivateScheduleRequest,
) (*statev1.ActivateScheduleResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}
	u := h.uowFactory()
	id, outcome, err := h.activate.Handle(ctx, u, tenantID, req.ScheduleName, run.KindCron, nil, identity.FromContext(ctx), pkg_model.OperationRun)
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

// ListAllSchedules returns a summary of all active schedules of the request's
// tenant from the catalog, merged with last-run data and cron config metadata.
func (h *SchedulerHandler) ListAllSchedules(
	ctx context.Context,
	req *statev1.ListAllSchedulesRequest,
) (*statev1.ListAllSchedulesResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	names, err := h.catalogRepo.ListActive(ctx, tenantID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list active schedules: %v", err)
	}

	lastRuns, err := h.repo.GetLastRunPerSchedule(ctx, tenantID)
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
			s.IsRunning = run.IsActive
			s.LastRunStatus = string(run.Status)
			s.LastRunAt = timestamppb.New(run.CreatedAt)
			s.LastRunId = run.RunID.String()
		}
		summaries = append(summaries, s)
	}

	return &statev1.ListAllSchedulesResponse{Schedules: summaries}, nil
}

// TriggerSchedule manually triggers a run of the request's tenant's schedule.
// Returns FailedPrecondition when a run is already active or when the
// schedule's selection is refused (no node of the schedule in the live
// release, or a node of an undeclared type), NotFound when the schedule is not
// in catalog. A test run of a schedule with no node with data tests is
// created, finished skipped.
func (h *SchedulerHandler) TriggerSchedule(
	ctx context.Context,
	req *statev1.TriggerScheduleRequest,
) (*statev1.TriggerScheduleResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.ScheduleName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
	}
	operation, err := pkg_model.ParseOperation(req.Operation)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid operation: %v", err)
	}
	if operation.IsSingleNodeOnly() {
		return nil, status.Errorf(codes.InvalidArgument, "operation %q targets a single node; use TriggerSingleNodeRun", operation)
	}
	u := h.uowFactory()
	id, outcome, err := h.activate.Handle(ctx, u, tenantID, req.ScheduleName, run.KindTrigger, nil, identity.FromContext(ctx), operation)
	if err != nil {
		if st, ok := planRefusal(err); ok {
			return nil, st.Err()
		}
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

// GetSchedulerInitStatus answers for a run of the request's tenant; another
// tenant's run is NotFound. A run has no initialization status, so the answer's
// status is empty.
func (h *SchedulerHandler) GetSchedulerInitStatus(ctx context.Context, req *statev1.GetSchedulerInitStatusRequest) (*statev1.GetSchedulerInitStatusResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.ScheduleId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schedule_id is required")
	}

	scheduleID, err := uuid.Parse(req.ScheduleId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule_id format")
	}

	_, err = h.repo.GetByID(ctx, tenantID, scheduleID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "scheduler not found")
		}
		h.logger.Error("Failed to get scheduler for init status", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get scheduler")
	}

	return &statev1.GetSchedulerInitStatusResponse{}, nil
}
