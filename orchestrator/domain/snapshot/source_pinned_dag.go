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
//   - Target node                                       → PENDING, fresh task_id, source's pinned (image_tag, manifest_version).
//   - Non-SUCCEEDED descendants of target in source set → PENDING, fresh task_id (typically the cascade-skipped set).
//   - Every other source task                           → inherited (status carried forward, root-forwarded pointer).
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

	target := FQN{Service: s.TargetService, Schema: s.TargetSchema, Table: s.TargetTable}
	if _, ok := source[target]; !ok {
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
