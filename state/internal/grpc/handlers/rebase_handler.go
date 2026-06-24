package handlers

import (
	"context"
	"errors"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RebaseHandler implements the TriggerRebase gRPC method.
//
// A rebase synthesises a new Run that inherits the source's schedule_name,
// points back to the source via source_run_id, and is marked kind='rebase'.
// The orchestrator (via trigger.rebase:v1 outbox fanout) is responsible for
// partitioning and projecting inherited tasks.
type RebaseHandler struct {
	useCase    *svchandlers.TriggerDerivedRunHandler
	uowFactory func() uow.UnitOfWork
	logger     *slog.Logger
}

// NewRebaseHandler creates a new RebaseHandler.
func NewRebaseHandler(
	useCase *svchandlers.TriggerDerivedRunHandler,
	uowFactory func() uow.UnitOfWork,
	logger *slog.Logger,
) *RebaseHandler {
	return &RebaseHandler{useCase: useCase, uowFactory: uowFactory, logger: logger}
}

// TriggerRebase validates the request and delegates to the rebase-configured
// TriggerDerivedRunHandler use case.
func (h *RebaseHandler) TriggerRebase(ctx context.Context, req *statev1.TriggerRebaseRequest) (*statev1.TriggerRebaseResponse, error) {
	h.logger.Info("TriggerRebase called", "source_run_id", req.SourceRunId)

	if req.SourceRunId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "source_run_id is required")
	}
	srcID, err := uuid.Parse(req.SourceRunId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid source_run_id format")
	}

	id, name, err := h.useCase.Handle(ctx, h.uowFactory(), srcID, identity.FromContext(ctx))
	if err != nil {
		switch {
		case errors.Is(err, postgres.ErrNotFound):
			return nil, status.Errorf(codes.NotFound, "source run not found")
		case errors.Is(err, run.ErrSourceMustBeTerminallyFailedOrCancelled),
			errors.Is(err, run.ErrNothingToRebase),
			errors.Is(err, run.ErrScheduleHasActiveRun):
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		default:
			h.logger.Error("TriggerRebase failed", "error", err)
			return nil, status.Errorf(codes.Internal, "rebase: %v", err)
		}
	}
	return &statev1.TriggerRebaseResponse{RunId: id.String(), ScheduleName: name}, nil
}
