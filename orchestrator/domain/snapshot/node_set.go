package snapshot

import (
	"context"
	"fmt"
)

// NodeSet produces one TaskProjection per node in an explicit list, in the order
// given. It is the selector for a run whose membership is decided by the caller
// rather than derived from the topology — the promoted-seeds run names the seeds
// a release changed, and this resolves each one's current metadata.
//
// Node metadata comes from the latest :Table topology, except for any field the
// caller pins (see Pinned) — there is no snapshot_of_run mode here. Ordering
// between members carries no meaning; callers must only put nodes in a set
// together when they can run independently of one another.
type NodeSet struct {
	Nodes []FQN
	// Pinned supplies node_type/image_tag per FQN, overriding whatever the
	// topology currently holds. The promoted-seeds run sets it so a release
	// builds its seeds with the image that release promoted, even if a later
	// promotion has already moved the topology on. An FQN absent from the map
	// falls back to the topology row.
	Pinned map[FQN]PinnedNodeMetadata
}

// PinnedNodeMetadata is the release-pinned metadata for one node.
type PinnedNodeMetadata struct {
	NodeType string
	ImageTag string
}

func (s NodeSet) SelectTasks(ctx context.Context, r TopologyReader, p Params) ([]TaskProjection, error) {
	if len(s.Nodes) == 0 {
		return nil, ErrEmptyProjection
	}
	out := make([]TaskProjection, 0, len(s.Nodes))
	for _, fqn := range s.Nodes {
		if fqn.Service == "" || fqn.Schema == "" || fqn.Table == "" {
			return nil, fmt.Errorf("NodeSet: service_name, schema_name, and table_name are required")
		}
		row, ok, err := r.LoadSingleLatestTable(ctx, fqn)
		if err != nil {
			return nil, fmt.Errorf("NodeSet %s.%s.%s: %w", fqn.Service, fqn.Schema, fqn.Table, err)
		}
		// A node named in the set but absent from the topology fails the whole
		// run rather than silently shrinking it: the caller asked for these
		// nodes, and a partial run would report success having skipped work.
		if !ok {
			return nil, ErrTargetNotFound
		}
		// The topology row still supplies schedule_name and test counts; only the
		// fields a later promotion could have moved are overridden.
		if p, ok := s.Pinned[fqn]; ok {
			if p.NodeType != "" {
				row.NodeType = p.NodeType
			}
			if p.ImageTag != "" {
				row.ImageTag = p.ImageTag
			}
		}
		out = append(out, toSingleNodeProjection(fqn, row))
	}
	return out, nil
}
