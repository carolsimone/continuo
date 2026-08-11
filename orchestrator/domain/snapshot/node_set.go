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
// Node metadata always comes from the latest :Table topology: every node in the
// set is built as it exists now, so there is no snapshot_of_run mode here.
// Ordering between members carries no meaning; callers must only put nodes in a
// set together when they can run independently of one another.
type NodeSet struct {
	Nodes []FQN
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
		out = append(out, toSingleNodeProjection(fqn, row))
	}
	return out, nil
}
