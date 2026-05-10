package snapshot

import (
	"context"
	"fmt"

	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
)

// SourcePinnedDAG is the rerun selector. It produces a full DAG projection
// from the source :Run's frozen :EXECUTES set, with uniform OLD metadata.
//
// Behaviour:
//   - The target node (TargetService/Schema/Table) → InitialStatus=PENDING,
//     fresh task_id, source's pinned (image_tag, manifest_version), no inherit.
//   - Non-SUCCEEDED descendants of the target in source's :EXECUTES set
//     (typically SKIPPED — the cascade-skipped set when target failed) →
//     PENDING, fresh task_id, no inherit.
//   - Every other source task (incl. SUCCEEDED descendants of target and
//     unrelated SUCCEEDED rows) → inherited. InitialStatus = source's
//     stored status. Source's pinned metadata. InheritedFromTaskID points
//     to the root-resolved task_id (forwards when source row was itself
//     inherited).
//
// Returns ErrTargetNotFound if the target FQN is not in the source's :EXECUTES set.
type SourcePinnedDAG struct {
	TargetService string
	TargetSchema  string
	TargetTable   string
}

func (s SourcePinnedDAG) SelectTasks(ctx context.Context, r TopologyReader, p Params) ([]TaskProjection, error) {
	if p.SourceRunID == nil {
		return nil, fmt.Errorf("SourcePinnedDAG: SourceRunID required")
	}
	if s.TargetService == "" || s.TargetSchema == "" || s.TargetTable == "" {
		return nil, fmt.Errorf("SourcePinnedDAG: target identity required")
	}

	source, err := r.LoadSourceTasks(ctx, p.SourceRunID.String())
	if err != nil { return nil, err }

	// P2 fix: source map keys include ScheduleName which the caller doesn't know.
	// Iterate to find the matching entry by 3-tuple; capture the full key so that
	// DescendantsInSourceRun gets the precise 4-field FQN.
	var target FQN
	matched := false
	for f := range source {
		if f.Service == s.TargetService && f.Schema == s.TargetSchema && f.Table == s.TargetTable {
			target = f
			matched = true
			break
		}
	}
	if !matched {
		return nil, ErrTargetNotFound
	}

	rebaseFQNs := map[FQN]struct{}{target: {}}
	descendants, err := r.DescendantsInSourceRun(ctx, p.SourceRunID.String(), target)
	if err != nil { return nil, err }
	for _, d := range descendants {
		st, ok := source[d]
		if !ok { continue }
		if st.Status != "SUCCEEDED" {
			rebaseFQNs[d] = struct{}{}
		}
	}

	var projection []TaskProjection
	for f, st := range source {
		if _, isRebased := rebaseFQNs[f]; isRebased {
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
	return projection, nil
}
