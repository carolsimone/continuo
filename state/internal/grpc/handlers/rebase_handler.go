package handlers

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/identity"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RebaseStarter creates a rebase run: the use case behind TriggerRebase.
type RebaseStarter interface {
	Handle(ctx context.Context, u uow.UnitOfWork, in svchandlers.TriggerRebaseInput) (uuid.UUID, string, error)
}

// RebaseHandler implements the TriggerRebase gRPC method.
//
// A rebase creates a new run (kind='rebase', source_run_id=<source>) on the
// source's schedule and the live release that restarts the source from its
// failure: it re-executes the source's tasks that did not succeed and whose
// node the live schedule still holds, the live schedule's nodes the source
// never ran, every descendant of those and, with include_changed_upstream,
// every changed node upstream of what it re-executes. Every other source
// success is carried over as succeeded. A non-empty expected_release_id must
// name the live release.
type RebaseHandler struct {
	useCase    RebaseStarter
	uowFactory func() uow.UnitOfWork
	logger     *slog.Logger
}

// NewRebaseHandler creates a new RebaseHandler.
func NewRebaseHandler(
	useCase RebaseStarter,
	uowFactory func() uow.UnitOfWork,
	logger *slog.Logger,
) *RebaseHandler {
	return &RebaseHandler{useCase: useCase, uowFactory: uowFactory, logger: logger}
}

// TriggerRebase validates the request and delegates to the rebase use case.
func (h *RebaseHandler) TriggerRebase(ctx context.Context, req *statev1.TriggerRebaseRequest) (*statev1.TriggerRebaseResponse, error) {
	h.logger.Info("TriggerRebase called", "source_run_id", req.GetSourceRunId(),
		"include_changed_upstream", req.GetIncludeChangedUpstream(), "expected_release_id", req.GetExpectedReleaseId())

	srcID, err := parseSourceRunID(req.GetSourceRunId())
	if err != nil {
		return nil, err
	}
	id, name, err := h.useCase.Handle(ctx, h.uowFactory(), svchandlers.TriggerRebaseInput{
		SourceRunID:            srcID,
		IncludeChangedUpstream: req.GetIncludeChangedUpstream(),
		ExpectedReleaseID:      req.GetExpectedReleaseId(),
		Initiator:              identity.FromContext(ctx),
	})
	if err != nil {
		if st, ok := rebaseRefusal(err); ok {
			return nil, st.Err()
		}
		h.logger.Error("TriggerRebase failed", "error", err)
		return nil, status.Errorf(codes.Internal, "rebase: %v", err)
	}
	return &statev1.TriggerRebaseResponse{RunId: id.String(), ScheduleName: name}, nil
}

// parseSourceRunID parses a rebase request's source_run_id; a missing or
// malformed one is InvalidArgument.
func parseSourceRunID(raw string) (uuid.UUID, error) {
	if raw == "" {
		return uuid.Nil, status.Error(codes.InvalidArgument, "source_run_id is required")
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "invalid source_run_id format")
	}
	return id, nil
}
