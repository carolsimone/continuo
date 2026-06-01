// executor-controller/service/handlers/validation_node_completed_handler.go
package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
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
	if err := dep.RecordOutcome(evt.Outcome, evt.DBTLogURI, now); err != nil {
		return fmt.Errorf("record outcome: %w", err)
	}
	if err := u.DeploymentsRepo().Save(ctx, dep); err != nil {
		return fmt.Errorf("save deployment: %w", err)
	}

	if err := propagateGating(ctx, u.DeploymentsRepo(), evt.ReleaseID, evt.NodeID, evt.Outcome, now); err != nil {
		return fmt.Errorf("propagate gating: %w", err)
	}

	return validation.EmitValidationAggregateIfComplete(
		ctx,
		u.DeploymentsRepo(),
		u.OutboxRepo(),
		u.ValidationAggregateRepo(),
		validation.DedupNamespace,
		evt.ReleaseID,
		now,
	)
}

// propagateGating advances the gated downstream rows after one node terminates.
// On success: every blocked row whose in-set upstreams are now ALL satisfied
// (outcome="ok") is Unblocked to pending. On failure: every blocked row
// transitively downstream of the failed node is Skipped — it can never be
// validated. Readiness/reachability is computed in Go over the release's rows;
// transitions go through the guarded aggregate methods, one Save per change.
func propagateGating(ctx context.Context, repo repository.DeploymentRepository, releaseID, completedNode, outcome string, now time.Time) error {
	rows, err := repo.ListValidationByRelease(ctx, releaseID)
	if err != nil {
		return fmt.Errorf("list validation by release: %w", err)
	}
	byNode := make(map[string]*model.Deployment, len(rows))
	for _, d := range rows {
		byNode[d.NodeID()] = d
	}

	if outcome != "ok" {
		children := childIndex(rows)
		for _, victim := range reachable(children, completedNode) {
			d := byNode[victim]
			if d != nil && d.Status() == model.StatusBlocked {
				if err := d.Skip("upstream "+completedNode+" failed validation", now); err != nil {
					return fmt.Errorf("skip %s: %w", victim, err)
				}
				if err := repo.Save(ctx, d); err != nil {
					return fmt.Errorf("save skipped %s: %w", victim, err)
				}
			}
		}
		return nil
	}

	okOutcome := func(id string) bool {
		d := byNode[id]
		return d != nil && d.Outcome() == "ok"
	}
	for _, d := range rows {
		if d.Status() != model.StatusBlocked {
			continue
		}
		ups := d.ValidationCommand().UpstreamNodeIDs
		allOK := true
		for _, up := range ups {
			if !okOutcome(up) {
				allOK = false
				break
			}
		}
		if allOK {
			if err := d.Unblock(now); err != nil {
				return fmt.Errorf("unblock %s: %w", d.NodeID(), err)
			}
			if err := repo.Save(ctx, d); err != nil {
				return fmt.Errorf("save unblocked %s: %w", d.NodeID(), err)
			}
		}
	}
	return nil
}

// childIndex maps each node to the in-set nodes that declare it as an upstream.
func childIndex(rows []*model.Deployment) map[string][]string {
	children := map[string][]string{}
	for _, d := range rows {
		for _, up := range d.ValidationCommand().UpstreamNodeIDs {
			children[up] = append(children[up], d.NodeID())
		}
	}
	return children
}

// reachable returns all nodes strictly downstream of root (BFS over children),
// excluding root itself.
func reachable(children map[string][]string, root string) []string {
	seen := map[string]bool{}
	queue := append([]string{}, children[root]...)
	var out []string
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if seen[cur] {
			continue
		}
		seen[cur] = true
		out = append(out, cur)
		queue = append(queue, children[cur]...)
	}
	return out
}
