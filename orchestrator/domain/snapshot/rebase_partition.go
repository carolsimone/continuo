package snapshot

import (
	"context"
	"fmt"

	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
)

// RebasePartition is the rebase selector (Feature 2). Given a terminal source
// :Run and the latest topology, it produces a projection that rebases everything
// that didn't succeed (plus its descendants and any new arrivals) and inherits
// the rest. Returns ErrEmptyProjection if the partition produces zero tasks.
type RebasePartition struct{}

func (RebasePartition) SelectTasks(ctx context.Context, r TopologyReader, p Params) ([]TaskProjection, error) {
	if p.SourceRunID == nil {
		return nil, fmt.Errorf("RebasePartition: SourceRunID required")
	}
	if p.ScheduleName == "" {
		return nil, fmt.Errorf("RebasePartition: ScheduleName required")
	}

	// Reject rebase of a TEST run. The source operation is read authoritatively
	// from the reader (not from a caller-populated p.Operation) so this guard is
	// self-contained: it cannot be silently bypassed by a caller that forgets to
	// populate Operation. A test run's projection carries no per-task operation,
	// so rebasing it would re-issue dbt run/test incoherently.
	if op, err := r.SourceRunOperation(ctx, p.SourceRunID.String()); err != nil {
		return nil, fmt.Errorf("RebasePartition: source operation: %w", err)
	} else if op == string(pkgModel.OperationTest) {
		return nil, ErrRerunOfTestUnsupported
	}

	source, err := r.LoadSourceTasks(ctx, p.SourceRunID.String())
	if err != nil {
		return nil, err
	}

	latest, err := r.LoadLatestSourceDAG(ctx, p.ScheduleName)
	if err != nil {
		return nil, err
	}

	// Pass 1: seed rebase_set with non-SUCCEEDED source tasks that still exist in latest.
	rebaseFQNs := map[FQN]struct{}{}
	for f, st := range source {
		if st.Status != "SUCCEEDED" {
			if _, exists := latest[f]; exists {
				rebaseFQNs[f] = struct{}{}
			}
		}
	}
	// Pass 2: for each seeded FQN, add its descendants in LATEST topology.
	// Snapshot the seeds first: one batched reader call resolves every seed's
	// descendants, avoiding the map-mutation-during-range hazard of growing
	// rebaseFQNs while iterating it.
	pass2Seeds := fqnKeys(rebaseFQNs)
	descBySeed, err := r.DescendantsInLatestTopologyBatch(ctx, pass2Seeds)
	if err != nil {
		return nil, err
	}
	for _, seed := range pass2Seeds {
		for _, d := range descBySeed[seed] {
			if _, exists := latest[d]; exists {
				rebaseFQNs[d] = struct{}{}
			}
		}
	}
	// Pass 3: new arrivals (in latest, not in source) join rebase_set.
	for f := range latest {
		if _, exists := source[f]; !exists {
			rebaseFQNs[f] = struct{}{}
		}
	}
	// Pass 3b: compute the dispatch frontier. A rebased node is blocked (left to
	// the run aggregate's NodeUnblocked/cascade-skip) only when it has an
	// IMMEDIATE rebased upstream — i.e. it is a one-hop dependent of another
	// rebased node. Blocking must use immediate, not transitive, edges: the run
	// aggregate only unblocks/cascade-skips along immediate in-run edges, so a
	// node blocked via a transitive-only path (its connecting node absent from
	// the run) would never be reached and would stay PENDING forever. Walking
	// every rebased node (not just the Pass-1 seeds) also catches chains made
	// purely of new arrivals.
	blockedFQNs := map[FQN]struct{}{}
	frontierSeeds := fqnKeys(rebaseFQNs)
	immBySeed, err := r.ImmediateDescendantsInLatestTopologyBatch(ctx, frontierSeeds)
	if err != nil {
		return nil, err
	}
	for _, f := range frontierSeeds {
		for _, d := range immBySeed[f] {
			if _, isRebased := rebaseFQNs[d]; isRebased {
				blockedFQNs[d] = struct{}{}
			}
		}
	}
	// Pass 4: emit projection — iterate latest (drop_set excluded by construction).
	var projection []TaskProjection
	for f, lt := range latest {
		if _, isRebased := rebaseFQNs[f]; isRebased {
			_, blocked := blockedFQNs[f]
			projection = append(projection, TaskProjection{
				TaskID:          uuid.New(),
				ServiceName:     f.Service,
				SchemaName:      f.Schema,
				TableName:       f.Table,
				ScheduleName:    lt.ScheduleName,
				NodeType:        lt.NodeType,
				InitialStatus:   "PENDING",
				ImageTag:        lt.ImageTag,
				ManifestVersion: lt.ManifestVersion,
				TestCount:       lt.TestCount,
				TestCountKnown:  lt.TestCountKnown,
				MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
				ReadyToDispatch: !blocked,
			})
			continue
		}
		if st, ok := source[f]; ok && st.Status == "SUCCEEDED" {
			root := st.TaskID
			if st.InheritedFromRoot != nil {
				root = *st.InheritedFromRoot
			}
			projection = append(projection, TaskProjection{
				TaskID:              uuid.New(),
				ServiceName:         f.Service,
				SchemaName:          f.Schema,
				TableName:           f.Table,
				ScheduleName:        st.ScheduleName,
				NodeType:            st.NodeType,
				InitialStatus:       "SUCCEEDED",
				ImageTag:            st.ImageTag,
				ManifestVersion:     st.ManifestVersion,
				TestCountKnown:      false, // SourceTaskRow carries no test_count; inherited rows never gate on it
				InheritedFromTaskID: &root,
				MaxRetries:          0,
			})
		}
	}
	if len(projection) == 0 {
		return nil, ErrEmptyProjection
	}
	return projection, nil
}
