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

// HandleInTransaction creates the rerun runID of tenantID's run sourceID
// inside u's open transaction, which the caller commits or rolls back, and
// returns the rerun's schedule name. A selection the planner refuses (a
// source that names no release, a test source, nothing to execute again) is
// returned as is, and the caller rolls back.
func (h *TriggerRerunHandler) HandleInTransaction(ctx context.Context, u uow.UnitOfWork, runID uuid.UUID, tenantID string, sourceID uuid.UUID, initiator identity.Identity) (string, error) {
	return h.creation.createInTransaction(ctx, u, runID, tenantID, sourceID, initiator, h.selectRerun)
}

func (h *TriggerRerunHandler) selectRerun(ctx context.Context, u uow.UnitOfWork, src *run.Run) (runstart.Selection, error) {
	return h.creation.planner.Rerun(ctx, u, src)
}
