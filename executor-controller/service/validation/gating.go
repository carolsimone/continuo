package validation

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// SettleNodeTerminal advances gated downstreams and runs the aggregate-emit gate
// after one node reaches a terminal outcome, all UNDER the per-release advisory
// lock so concurrent terminals serialize: a later terminal always observes the
// earlier (committed) outcome and can unblock a child both gate. Call it after
// the node's own terminal outcome is recorded and saved. completedNodeID/outcome
// describe the node that just terminated ("ok" unblocks ready downstreams;
// anything else skips the failed node's transitive blocked downstreams).
func SettleNodeTerminal(
	ctx context.Context,
	depRepo repository.DeploymentRepository,
	outboxRepo outbox.Repository,
	aggRepo repository.ValidationAggregateRepository,
	namespace uuid.UUID,
	releaseID, completedNodeID, outcome string,
	now time.Time,
) error {
	cfg := validationEmit
	cfg.namespace = namespace
	if err := aggRepo.LockRelease(ctx, releaseID, cfg.mode); err != nil {
		return fmt.Errorf("lock release for settle: %w", err)
	}
	if err := propagateGating(ctx, depRepo, cfg.mode, releaseID, completedNodeID, outcome, now); err != nil {
		return fmt.Errorf("propagate gating: %w", err)
	}
	return emitAggregateIfComplete(ctx, depRepo, outboxRepo, aggRepo, cfg, releaseID, now)
}

// SettleSeedBuildNodeTerminal settles a seed-build node after it reaches a
// terminal outcome. Seeds are flat roots with no in-leg upstreams, so there is
// no gating to propagate (propagateGating over the mode=seed_build rows finds no
// blocked rows and no upstream edges — it is a no-op); the settle is just the
// aggregate-emit gate under the per-(release, seed-build) advisory lock. It emits
// seed.build.completed:v1 once every mode=seed_build row for the release settles.
func SettleSeedBuildNodeTerminal(
	ctx context.Context,
	depRepo repository.DeploymentRepository,
	outboxRepo outbox.Repository,
	aggRepo repository.ValidationAggregateRepository,
	releaseID, completedNodeID, outcome string,
	now time.Time,
) error {
	if err := aggRepo.LockRelease(ctx, releaseID, seedBuildEmit.mode); err != nil {
		return fmt.Errorf("lock release for seed-build settle: %w", err)
	}
	// No-op over flat seed roots, but kept for symmetry and to defend against a
	// future seed dependency edge: it only ever transitions blocked rows.
	if err := propagateGating(ctx, depRepo, seedBuildEmit.mode, releaseID, completedNodeID, outcome, now); err != nil {
		return fmt.Errorf("propagate seed-build gating: %w", err)
	}
	return emitAggregateIfComplete(ctx, depRepo, outboxRepo, aggRepo, seedBuildEmit, releaseID, now)
}

// propagateGating advances the gated downstream rows after one node terminates.
// On success: every blocked row whose in-set upstreams are now ALL satisfied
// (outcome="ok") is Unblocked to pending. On failure: every blocked row
// transitively downstream of the failed node is Skipped — it can never be
// validated. Readiness/reachability is computed in Go over the release's rows;
// transitions go through the guarded aggregate methods, one Save per change.
func propagateGating(ctx context.Context, repo repository.DeploymentRepository, mode model.Mode, releaseID, completedNode, outcome string, now time.Time) error {
	rows, err := repo.ListValidationByRelease(ctx, releaseID, mode)
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
