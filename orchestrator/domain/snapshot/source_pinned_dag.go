package snapshot

import (
	"context"
	"fmt"

	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
)

// SourcePinnedDAG is the rerun selector. It produces a projection bound to the
// source :Run's frozen :EXECUTES set — same topology the source ran against,
// no drift, no new arrivals from latest.
//
// Behaviour:
//   - Every non-SUCCEEDED source task is seeded into the rebase set.
//   - Every descendant of a seeded task (in the source's pinned :EXECUTES set)
//     joins the rebase set, regardless of its source status.
//   - Rebase-set rows → PENDING, fresh task_id, source's pinned
//     (image_tag, manifest_version), InheritedFromTaskID = nil.
//   - Every other source task → InitialStatus = source's stored status,
//     source's pinned metadata, InheritedFromTaskID = root-resolved source
//     task_id (forwards when the source row was itself inherited).
//
// Returns ErrEmptyProjection when the source has zero tasks — defensive only;
// state's TriggerRerun eligibility (≥1 non-SUCCEEDED task) makes this
// unreachable in production.
type SourcePinnedDAG struct{}

func (SourcePinnedDAG) SelectTasks(ctx context.Context, r TopologyReader, p Params) ([]TaskProjection, error) {
	if p.SourceRunID == nil {
		return nil, fmt.Errorf("SourcePinnedDAG: SourceRunID required")
	}

	if p.Operation == string(pkgModel.OperationTest) {
		return nil, ErrRerunOfTestUnsupported
	}

	source, err := r.LoadSourceTasks(ctx, p.SourceRunID.String())
	if err != nil {
		return nil, err
	}

	// Pass 1: seed with all non-SUCCEEDED source tasks.
	rebaseFQNs := map[FQN]struct{}{}
	for f, st := range source {
		if st.Status != "SUCCEEDED" {
			rebaseFQNs[f] = struct{}{}
		}
	}

	// No non-SUCCEEDED tasks means nothing to rerun; guard here so we don't
	// emit a projection that consists entirely of inherited rows.
	if len(rebaseFQNs) == 0 {
		return nil, ErrEmptyProjection
	}

	// Pass 2: grow the rebase set with all transitive descendants WITHIN the
	// source's pinned :EXECUTES set. Snapshot the keys first so the iteration is
	// stable, then resolve every seed's descendants in one batched reader call.
	seeds := fqnKeys(rebaseFQNs)
	descBySeed, err := r.DescendantsInSourceRunBatch(ctx, p.SourceRunID.String(), seeds)
	if err != nil {
		return nil, err
	}
	for _, seed := range seeds {
		for _, d := range descBySeed[seed] {
			if _, ok := source[d]; ok {
				rebaseFQNs[d] = struct{}{}
			}
		}
	}

	// Pass 2b: compute the dispatch frontier. A rebased node is blocked only when
	// it has an IMMEDIATE rebased upstream (it is a one-hop dependent of another
	// rebased node). Blocking must use immediate, not transitive, edges: the run
	// aggregate only unblocks/cascade-skips along immediate in-run edges, so a
	// node blocked via a transitive-only path (its connecting node absent from
	// the run) would never be reached and would stay PENDING forever.
	blockedFQNs := map[FQN]struct{}{}
	frontierSeeds := fqnKeys(rebaseFQNs)
	immBySeed, err := r.ImmediateDescendantsInSourceRunBatch(ctx, p.SourceRunID.String(), frontierSeeds)
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

	// Pass 3: emit projection.
	var projection []TaskProjection
	for f, st := range source {
		if _, isRebased := rebaseFQNs[f]; isRebased {
			_, blocked := blockedFQNs[f]
			projection = append(projection, TaskProjection{
				TaskID:          uuid.New(),
				ServiceName:     f.Service,
				SchemaName:      f.Schema,
				TableName:       f.Table,
				ScheduleName:    st.ScheduleName,
				NodeType:        st.NodeType,
				InitialStatus:   "PENDING",
				ImageTag:        st.ImageTag,
				ManifestVersion: st.ManifestVersion,
				TestCountKnown:  false, // SourceTaskRow carries no test_count; rebased rows never gate on it
				MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
				ReadyToDispatch: !blocked,
			})
			continue
		}
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
			InitialStatus:       st.Status,
			ImageTag:            st.ImageTag,
			ManifestVersion:     st.ManifestVersion,
			TestCountKnown:      false, // SourceTaskRow carries no test_count; inherited rows never gate on it
			InheritedFromTaskID: &root,
			MaxRetries:          0,
		})
	}

	if len(projection) == 0 {
		return nil, ErrEmptyProjection
	}
	return projection, nil
}
