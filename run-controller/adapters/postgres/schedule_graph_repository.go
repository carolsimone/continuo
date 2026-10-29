package postgres

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
)

// ScheduleGraphRepository implements repository.ScheduleGraphRepository over
// state's copy of the promoted topologies. Every read starts from the tenant's
// live_release row and reads that release's rows only; a release never
// changes, so a pointer that moves during a read leaves it a consistent view.
type ScheduleGraphRepository struct {
	db   *sqlx.DB
	topo *TopologyRepository
}

// Compile-time assertion that the adapter satisfies the domain port.
var _ repository.ScheduleGraphRepository = (*ScheduleGraphRepository)(nil)

// NewScheduleGraphRepository returns a ScheduleGraphRepository over db.
func NewScheduleGraphRepository(db *sqlx.DB) *ScheduleGraphRepository {
	return &ScheduleGraphRepository{db: db, topo: NewTopologyRepository(db, nil)}
}

// Graph reads the live release's nodes of the schedule through
// idx_topology_nodes_schedule, the edges into them, and the nodes those edges
// name outside the schedule. Each read is an index lookup by key, so the cost
// follows the schedule's size.
func (r *ScheduleGraphRepository) Graph(ctx context.Context, tenantID, scheduleName string) (topology.ScheduleGraph, error) {
	live, err := r.topo.LivePointer(ctx, tenantID)
	if err != nil {
		return topology.ScheduleGraph{}, err
	}
	if live.IsZero() {
		return topology.ScheduleGraph{}, nil
	}
	graph := topology.ScheduleGraph{PromotionSeq: live.PromotionSeq}

	var members []topologyNodeRow
	if err := sqlx.SelectContext(ctx, r.db, &members, `
		SELECT `+topologyNodeColumns+`
		FROM topology_nodes
		WHERE tenant_id = $1 AND release_id = $2 AND schedule_name = $3`,
		tenantID, live.ReleaseID, scheduleName); err != nil {
		return topology.ScheduleGraph{}, fmt.Errorf("read the nodes of schedule %s in release %s: %w", scheduleName, live.ReleaseID, err)
	}
	if len(members) == 0 {
		return graph, nil
	}
	edges, err := r.topo.edgesInto(ctx, tenantID, live.ReleaseID, members)
	if err != nil {
		return topology.ScheduleGraph{}, err
	}

	held := make(map[string]bool, len(members))
	for _, m := range members {
		held[m.NodeKey] = true
	}
	var outside []string
	for _, e := range edges {
		if !held[string(e.Upstream)] {
			held[string(e.Upstream)] = true
			outside = append(outside, string(e.Upstream))
		}
	}
	rows := members
	if len(outside) > 0 {
		var upstreams []topologyNodeRow
		if err := sqlx.SelectContext(ctx, r.db, &upstreams, `
			SELECT `+topologyNodeColumns+`
			FROM topology_nodes
			WHERE tenant_id = $1 AND release_id = $2 AND node_key = ANY($3)`,
			tenantID, live.ReleaseID, pq.Array(outside)); err != nil {
			return topology.ScheduleGraph{}, fmt.Errorf("read the upstreams of schedule %s in release %s: %w", scheduleName, live.ReleaseID, err)
		}
		rows = append(rows, upstreams...)
	}

	graph.Nodes = make([]topology.Node, 0, len(rows))
	for _, row := range rows {
		graph.Nodes = append(graph.Nodes, row.toDomain())
	}
	slices.SortFunc(graph.Nodes, func(a, b topology.Node) int { return cmp.Compare(a.Key, b.Key) })
	graph.Edges = edges
	slices.SortFunc(graph.Edges, func(a, b topology.Edge) int {
		if c := cmp.Compare(a.Upstream, b.Upstream); c != 0 {
			return c
		}
		return cmp.Compare(a.Downstream, b.Downstream)
	})
	return graph, nil
}

// Summaries counts the live release's nodes per schedule with liveScheduleModelCounts,
// the count LiveScheduleModelRepository reads, and stamps each with when the
// release was loaded. A node whose schedule name is empty belongs to no
// schedule and is not counted.
func (r *ScheduleGraphRepository) Summaries(ctx context.Context, tenantID string) ([]topology.ScheduleSummary, error) {
	var rows []struct {
		ScheduleName string    `db:"schedule_name"`
		NodeCount    int       `db:"node_count"`
		LoadedAt     time.Time `db:"loaded_at"`
	}
	if err := sqlx.SelectContext(ctx, r.db, &rows, `
		WITH counts AS (`+liveScheduleModelCounts+`)
		SELECT c.schedule_name, c.models AS node_count, rel.loaded_at
		FROM counts c
		JOIN live_release live ON live.tenant_id = $1
		JOIN topology_releases rel
		  ON rel.tenant_id = live.tenant_id AND rel.release_id = live.release_id
		ORDER BY c.schedule_name COLLATE "C"`, tenantID); err != nil {
		return nil, fmt.Errorf("count the nodes per schedule of tenant %s: %w", tenantID, err)
	}
	out := make([]topology.ScheduleSummary, 0, len(rows))
	for _, row := range rows {
		out = append(out, topology.ScheduleSummary{ScheduleName: row.ScheduleName, NodeCount: row.NodeCount, LastUpdatedAt: row.LoadedAt})
	}
	return out, nil
}
