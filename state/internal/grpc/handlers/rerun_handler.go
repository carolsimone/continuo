package handlers

import (
	"context"
	"errors"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RerunHandler implements the TriggerRerun gRPC method.
//
// TriggerRerun mints a new Run on the source's schedule (kind='rerun',
// source_run_id=<source>) and on the source's release. The source row stays at
// its terminal status as an immutable historical record. The new run reuses
// the source's schedule_name so it appears in the same schedule's history. It
// re-executes the source's tasks that did not succeed and every descendant of
// theirs among the source's tasks, along the source release's edges, and
// carries every other source task over as succeeded.
type RerunHandler struct {
	useCase    *svchandlers.TriggerRerunHandler
	uowFactory func() uow.UnitOfWork
	logger     *slog.Logger
}

// NewRerunHandler creates a new RerunHandler.
func NewRerunHandler(
	useCase *svchandlers.TriggerRerunHandler,
	uowFactory func() uow.UnitOfWork,
	logger *slog.Logger,
) *RerunHandler {
	return &RerunHandler{useCase: useCase, uowFactory: uowFactory, logger: logger}
}

// TriggerRerun validates the request and delegates to the TriggerRerunHandler
// use case, for the request's tenant: another tenant's source run is NotFound.
func (h *RerunHandler) TriggerRerun(ctx context.Context, req *statev1.TriggerRerunRequest) (*statev1.TriggerRerunResponse, error) {
	h.logger.Info("TriggerRerun called", "source_run_id", req.SourceRunId)
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}

	if req.SourceRunId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "source_run_id is required")
	}
	srcID, err := uuid.Parse(req.SourceRunId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid source_run_id format")
	}

	id, name, err := h.useCase.Handle(ctx, h.uowFactory(), tenantID, srcID, identity.FromContext(ctx))
	if err != nil {
		if st, ok := planRefusal(err); ok {
			return nil, st.Err()
		}
		switch {
		case errors.Is(err, postgres.ErrNotFound):
			return nil, status.Errorf(codes.NotFound, "source run not found")
		case errors.Is(err, run.ErrSourceMustBeTerminallyFailedOrCancelled),
			errors.Is(err, run.ErrNothingToRerun),
			errors.Is(err, run.ErrScheduleHasActiveRun):
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		default:
			h.logger.Error("TriggerRerun failed", "error", err)
			return nil, status.Errorf(codes.Internal, "rerun: %v", err)
		}
	}
	return &statev1.TriggerRerunResponse{RunId: id.String(), ScheduleName: name}, nil
}
