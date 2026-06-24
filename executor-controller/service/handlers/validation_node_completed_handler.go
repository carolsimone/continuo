// executor-controller/service/handlers/validation_node_completed_handler.go
package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/carolsimone/continuo/executor-controller/service/validation"
	"github.com/google/uuid"
)

// ValidationNodeCompletedHandler processes validation.node.completed:v1 events.
// It attaches the per-node terminal outcome to the matching (release_id, node_id)
// validation deployment, then runs the shared per-release aggregate-emit gate so
// validation.completed:v1 fires exactly once when every node of a release is
// terminal. The handler is pure orchestration: it never parses JSON, manages the
// transaction, or runs dedup itself.
type ValidationNodeCompletedHandler struct {
	logger *slog.Logger
}

// NewValidationNodeCompletedHandler constructs the handler.
func NewValidationNodeCompletedHandler(logger *slog.Logger) *ValidationNodeCompletedHandler {
	return &ValidationNodeCompletedHandler{logger: logger}
}

// Handle records one node's terminal outcome and triggers the aggregate gate.
//
// Redelivery is a no-op ACK: if the deployment already carries a recorded
// outcome, the message has already been processed, so the handler returns nil
// without double-recording or re-running the aggregate gate. An unknown
// (release_id, node_id) — no matching deployment row — is logged and ACKed (nil)
// rather than left pending forever; it means the message references a release
// the executor never enqueued.
func (h *ValidationNodeCompletedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.ValidationNodeCompleted,
	msgProcID uuid.UUID,
) error {
	dep, err := u.DeploymentsRepo().GetByReleaseNode(ctx, evt.ReleaseID, evt.NodeID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			h.logger.Warn("validation.node.completed: no matching deployment",
				"release_id", evt.ReleaseID, "node_id", evt.NodeID)
			return nil
		}
		return fmt.Errorf("get deployment: %w", err)
	}

	// Redelivery: the outcome is already recorded. ACK without re-recording or
	// re-triggering the aggregate (RecordOutcome would reject the double-record).
	if dep.OutcomeAt() != nil {
		h.logger.Info("validation.node.completed: outcome already recorded — treating as redelivery",
			"release_id", evt.ReleaseID, "node_id", evt.NodeID)
		return nil
	}

	now := time.Now()
	if err := dep.RecordOutcome(evt.Outcome, evt.DBTLogURI, evt.RunResultsURI, now); err != nil {
		return fmt.Errorf("record outcome: %w", err)
	}
	if err := u.DeploymentsRepo().Save(ctx, dep); err != nil {
		return fmt.Errorf("save deployment: %w", err)
	}

	return validation.SettleNodeTerminal(
		ctx, u.DeploymentsRepo(), u.OutboxRepo(), u.ValidationAggregateRepo(),
		validation.DedupNamespace, evt.ReleaseID, evt.NodeID, evt.Outcome, now,
	)
}
