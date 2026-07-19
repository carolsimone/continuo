// executor-controller/service/handlers/seed_build_node_completed_handler.go
package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/carolsimone/continuo/executor-controller/service/validation"
	"github.com/google/uuid"
)

// SeedBuildNodeCompletedHandler processes seed.build.node.completed:v1 events.
// It attaches the per-node terminal outcome to the matching (release_id, node_id)
// mode=seed_build deployment, then runs the per-release seed-build aggregate-emit
// gate so seed.build.completed:v1 fires exactly once when every seed of a release
// is terminal. It mirrors ValidationNodeCompletedHandler but is scoped entirely
// to ModeSeedBuild, so it never reads or affects the release's validation rows.
// The handler is pure orchestration: it never parses JSON, manages the
// transaction, or runs dedup itself.
type SeedBuildNodeCompletedHandler struct {
	logger *slog.Logger
}

// NewSeedBuildNodeCompletedHandler constructs the handler.
func NewSeedBuildNodeCompletedHandler(logger *slog.Logger) *SeedBuildNodeCompletedHandler {
	return &SeedBuildNodeCompletedHandler{logger: logger}
}

// Handle records one seed's terminal outcome and triggers the seed-build
// aggregate gate.
//
// Redelivery is a no-op ACK: if the deployment already carries a recorded
// outcome, the message has already been processed, so the handler returns nil
// without double-recording or re-running the aggregate gate. An unknown
// (release_id, node_id) — no matching seed-build deployment row — is logged and
// ACKed (nil) rather than left pending forever.
func (h *SeedBuildNodeCompletedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.SeedBuildNodeCompleted,
	msgProcID uuid.UUID,
) error {
	dep, err := u.DeploymentsRepo().GetByReleaseNode(ctx, evt.ReleaseID, evt.NodeID, model.ModeSeedBuild)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			h.logger.Warn("seed.build.node.completed: no matching deployment",
				"release_id", evt.ReleaseID, "node_id", evt.NodeID)
			return nil
		}
		return fmt.Errorf("get seed-build deployment: %w", err)
	}

	// Redelivery: the outcome is already recorded. ACK without re-recording or
	// re-triggering the aggregate (RecordOutcome would reject the double-record).
	if dep.OutcomeAt() != nil {
		h.logger.Info("seed.build.node.completed: outcome already recorded — treating as redelivery",
			"release_id", evt.ReleaseID, "node_id", evt.NodeID)
		return nil
	}

	now := time.Now()
	if err := dep.RecordOutcome(evt.Outcome, evt.DBTLogURI, evt.RunResultsURI, "", now); err != nil {
		return fmt.Errorf("record seed-build outcome: %w", err)
	}
	if err := u.DeploymentsRepo().Save(ctx, dep); err != nil {
		return fmt.Errorf("save seed-build deployment: %w", err)
	}

	return validation.SettleSeedBuildNodeTerminal(
		ctx, u.DeploymentsRepo(), u.OutboxRepo(), u.ValidationAggregateRepo(),
		evt.ReleaseID, evt.NodeID, evt.Outcome, now,
	)
}
