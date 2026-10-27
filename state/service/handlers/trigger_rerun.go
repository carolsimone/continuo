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

// TriggerRerunHandler is the use case behind the TriggerRerun gRPC method: a
// new run that replays a failed or cancelled source run on the source's own
// release.
type TriggerRerunHandler struct {
	creation derivedRunCreation
}

// NewTriggerRerunHandler constructs the rerun use case; planner plans its
// runs.
func NewTriggerRerunHandler(planner runstart.Planner, logger *slog.Logger) *TriggerRerunHandler {
	return &TriggerRerunHandler{creation: derivedRunCreation{
		planner: planner,
		logger:  logger,
		kind:    run.KindRerun,
		eligible: func(ctx context.Context, src *run.Run, tasks run.TaskCollection) error {
			return src.CanBeRerunSource(ctx, tasks)
		},
		logLabel: "Rerun triggered",
	}}
}

// Handle creates a rerun of tenantID's run sourceID and returns (newRunID,
// scheduleName). A selection the planner refuses (a source that names no
// release, a test source, nothing to execute again) is returned as is, and
// nothing is written.
func (h *TriggerRerunHandler) Handle(ctx context.Context, u uow.UnitOfWork, tenantID string, sourceID uuid.UUID, initiator identity.Identity) (uuid.UUID, string, error) {
	return h.creation.create(ctx, u, tenantID, sourceID, initiator, h.selectRerun)
}

// HandleInTransaction creates the rerun runID of tenantID's run sourceID, as
// Handle does, inside u's open transaction, which the caller commits or rolls
// back. It returns the rerun's schedule name.
func (h *TriggerRerunHandler) HandleInTransaction(ctx context.Context, u uow.UnitOfWork, runID uuid.UUID, tenantID string, sourceID uuid.UUID, initiator identity.Identity) (string, error) {
	return h.creation.createInTransaction(ctx, u, runID, tenantID, sourceID, initiator, h.selectRerun)
}

func (h *TriggerRerunHandler) selectRerun(ctx context.Context, u uow.UnitOfWork, src *run.Run) (runstart.Selection, error) {
	return h.creation.planner.Rerun(ctx, u, src)
}
