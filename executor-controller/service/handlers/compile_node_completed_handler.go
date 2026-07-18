// executor-controller/service/handlers/compile_node_completed_handler.go
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

// CompileNodeCompletedHandler processes compile.node.completed:v1 events.
// It attaches the per-node terminal outcome to the matching (release_id, node_id)
// mode=compile deployment, then runs the per-release compile aggregate-emit
// gate so compile.completed:v1 fires exactly once when the compile node for a
// release is terminal. It mirrors SeedBuildNodeCompletedHandler but is scoped
// entirely to ModeCompile, so it never reads or affects the release's validation
// or seed-build rows. Compile is a single root node, so the aggregate gate fires
// immediately after this one node settles.
// The handler is pure orchestration: it never parses JSON, manages the
// transaction, or runs dedup itself.
type CompileNodeCompletedHandler struct {
	logger *slog.Logger
}

// NewCompileNodeCompletedHandler constructs the handler.
func NewCompileNodeCompletedHandler(logger *slog.Logger) *CompileNodeCompletedHandler {
	return &CompileNodeCompletedHandler{logger: logger}
}

// Handle records the compile node's terminal outcome and triggers the compile
// aggregate gate.
//
// Redelivery is a no-op ACK: if the deployment already carries a recorded
// outcome, the message has already been processed, so the handler returns nil
// without double-recording or re-running the aggregate gate. An unknown
// (release_id, node_id) — no matching compile deployment row — is logged and
// ACKed (nil) rather than left pending forever.
func (h *CompileNodeCompletedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.CompileNodeCompleted,
	msgProcID uuid.UUID,
) error {
	dep, err := u.DeploymentsRepo().GetByReleaseNode(ctx, evt.ReleaseID, evt.NodeID, model.ModeCompile)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			h.logger.Warn("compile.node.completed: no matching deployment",
				"release_id", evt.ReleaseID, "node_id", evt.NodeID)
			return nil
		}
		return fmt.Errorf("get compile deployment: %w", err)
	}

	// Redelivery: the outcome is already recorded. ACK without re-recording or
	// re-triggering the aggregate (RecordOutcome would reject the double-record).
	if dep.OutcomeAt() != nil {
		h.logger.Info("compile.node.completed: outcome already recorded — treating as redelivery",
			"release_id", evt.ReleaseID, "node_id", evt.NodeID)
		return nil
	}

	now := time.Now()
	if err := dep.RecordOutcome(evt.Outcome, evt.DBTLogURI, evt.RunResultsURI, evt.FailedContainer, now); err != nil {
		return fmt.Errorf("record compile outcome: %w", err)
	}
	if err := u.DeploymentsRepo().Save(ctx, dep); err != nil {
		return fmt.Errorf("save compile deployment: %w", err)
	}

	return validation.SettleCompileNodeTerminal(
		ctx, u.DeploymentsRepo(), u.OutboxRepo(), u.ValidationAggregateRepo(),
		evt.ReleaseID, evt.NodeID, evt.Outcome, now,
	)
}
