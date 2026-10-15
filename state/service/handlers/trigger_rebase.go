package handlers

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// TriggerRebaseInput is a request to rebase a failed or cancelled run onto
// the live release. IncludeChangedUpstream also re-executes the changed nodes
// upstream of what the rebase re-executes; a non-empty ExpectedReleaseID is
// the live release the caller previewed, and the rebase is refused when
// another release is live.
type TriggerRebaseInput struct {
	SourceRunID            uuid.UUID
	IncludeChangedUpstream bool
	ExpectedReleaseID      string
	Initiator              identity.Identity
}

// TriggerRebaseHandler is the use case behind the TriggerRebase gRPC method: a
// new run on the source's schedule and the live release that restarts the
// source from its failure.
type TriggerRebaseHandler struct {
	creation derivedRunCreation
}

// NewTriggerRebaseHandler constructs the rebase use case; planner plans its
// runs.
func NewTriggerRebaseHandler(planner runstart.Planner, logger *slog.Logger) *TriggerRebaseHandler {
	return &TriggerRebaseHandler{creation: derivedRunCreation{
		planner: planner,
		logger:  logger,
		kind:    run.KindRebase,
		eligible: func(ctx context.Context, src *run.Run, tasks run.TaskCollection) error {
			return src.CanBeRebaseSource(ctx, tasks)
		},
		logLabel: "Rebase triggered",
	}}
}

// Handle creates the rebase in.SourceRunID asks for and returns (newRunID,
// scheduleName). A live release other than in.ExpectedReleaseID is
// runstart.ErrLiveReleaseMoved; it and every selection the planner refuses
// are returned as is, and nothing is written.
func (h *TriggerRebaseHandler) Handle(ctx context.Context, u uow.UnitOfWork, in TriggerRebaseInput) (uuid.UUID, string, error) {
	opts := runstart.RebaseOptions{IncludeChangedUpstream: in.IncludeChangedUpstream, ExpectedReleaseID: in.ExpectedReleaseID}
	return h.creation.create(ctx, u, in.SourceRunID, in.Initiator, func(ctx context.Context, u uow.UnitOfWork, src *run.Run) (runstart.Selection, error) {
		sel, _, err := h.creation.planner.Rebase(ctx, u, src, opts)
		return sel, err
	})
}
