package handlers

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/service/runstart"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
)

// StartRebaseInput is a request to rebase a failed or cancelled run of
// TenantID onto the tenant's live release. IncludeChangedUpstream also
// re-executes the changed nodes upstream of what the rebase re-executes; a
// non-empty ExpectedReleaseID is the live release the caller previewed, and
// the rebase is refused when another release is live.
type StartRebaseInput struct {
	TenantID               string
	SourceRunID            uuid.UUID
	IncludeChangedUpstream bool
	ExpectedReleaseID      string
	Initiator              identity.Identity
}

// StartRebaseHandler is the use case behind the StartRebase gRPC method: a
// new run on the source's schedule and the live release that restarts the
// source from its failure.
type StartRebaseHandler struct {
	creation derivedRunCreation
}

// NewStartRebaseHandler constructs the rebase use case; planner plans its
// runs.
func NewStartRebaseHandler(planner runstart.Planner, logger *slog.Logger) *StartRebaseHandler {
	return &StartRebaseHandler{creation: derivedRunCreation{
		planner: planner,
		logger:  logger,
		kind:    run.KindRebase,
		eligible: func(ctx context.Context, src *run.Run, tasks run.TaskCollection) error {
			return src.CanBeRebaseSource(ctx, tasks)
		},
		logLabel: "Rebase triggered",
	}}
}

// HandleInTransaction creates the rebase runID in asks for inside u's open
// transaction, which the caller commits or rolls back, and returns the
// rebase's schedule name. A non-empty in.ExpectedReleaseID must name the live
// release, or the rebase is runstart.ErrLiveReleaseMoved; an empty one skips
// the check. That refusal and every selection the planner refuses are
// returned as is, and the caller rolls back.
func (h *StartRebaseHandler) HandleInTransaction(ctx context.Context, u uow.UnitOfWork, runID uuid.UUID, in StartRebaseInput) (string, error) {
	return h.creation.createInTransaction(ctx, u, runID, in.TenantID, in.SourceRunID, in.Initiator, h.selectRebase(in))
}

// selectRebase chooses the tasks of the rebase in asks for.
func (h *StartRebaseHandler) selectRebase(in StartRebaseInput) selectDerived {
	opts := runstart.RebaseOptions{IncludeChangedUpstream: in.IncludeChangedUpstream, ExpectedReleaseID: in.ExpectedReleaseID}
	return func(ctx context.Context, u uow.UnitOfWork, src *run.Run) (runstart.Selection, error) {
		sel, _, err := h.creation.planner.Rebase(ctx, u, src, opts)
		return sel, err
	}
}
