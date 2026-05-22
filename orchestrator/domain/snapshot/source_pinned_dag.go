package snapshot

import (
	"context"
	"fmt"

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

	// Pass 2: add descendants WITHIN the source's pinned :EXECUTES set.
	// Snapshot the keys first so the iteration is stable. blockedFQNs collects
	// every rebased node that has a rebased ancestor — i.e. a still-pending
	// upstream — so it can be excluded from the dispatch frontier below.
	seeds := make([]FQN, 0, len(rebaseFQNs))
	for f := range rebaseFQNs {
		seeds = append(seeds, f)
	}
	blockedFQNs := map[FQN]struct{}{}
	for _, seed := range seeds {
		descendants, err := r.DescendantsInSourceRun(ctx, p.SourceRunID.String(), seed)
		if err != nil {
			return nil, err
		}
		for _, d := range descendants {
			if _, ok := source[d]; ok {
				rebaseFQNs[d] = struct{}{}
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
			InheritedFromTaskID: &root,
			MaxRetries:          0,
		})
	}

	if len(projection) == 0 {
		return nil, ErrEmptyProjection
	}
	return projection, nil
}
