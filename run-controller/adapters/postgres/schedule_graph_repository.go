package postgres

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
)

// ScheduleGraphRepository implements repository.ScheduleGraphRepository over
// run-controller's copy of the promoted topologies. Every read starts from the tenant's
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
// idx_topology_nodes_schedule, the edges into them, the nodes those edges
// name outside the schedule, and the last run of each dormant node among them
// (lastRunsQuery). Each read is an index lookup by key, so the cost follows
// the schedule's size.
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

	graph.Nodes = make([]topology.ScheduleGraphNode, 0, len(rows))
	for _, row := range rows {
		graph.Nodes = append(graph.Nodes, topology.ScheduleGraphNode{Node: row.toDomain()})
	}
	slices.SortFunc(graph.Nodes, func(a, b topology.ScheduleGraphNode) int { return cmp.Compare(a.Key, b.Key) })
	if err := r.stampLastRuns(ctx, tenantID, graph.Nodes); err != nil {
		return topology.ScheduleGraph{}, fmt.Errorf("read the last runs of the dormant nodes schedule %s reads: %w", scheduleName, err)
	}
	graph.Edges = edges
	slices.SortFunc(graph.Edges, func(a, b topology.Edge) int {
		if c := cmp.Compare(a.Upstream, b.Upstream); c != 0 {
			return c
		}
		return cmp.Compare(a.Downstream, b.Downstream)
	})
	return graph, nil
}

// lastRunsQuery reads, for each node key in $2, the creation time of the run
// of the newest task of that node in tenant $1: one probe of
// idx_tasks_node_key_created per key, then the run by its key. Of two tasks
// of a node created at the same instant, the one of the higher run id is the
// newest. A key no task of the tenant names yields no row.
const lastRunsQuery = `
	SELECT k.node_key, s.created_at
	FROM unnest($2::text[]) AS k(node_key)
	CROSS JOIN LATERAL (
	    SELECT t.run_id
	    FROM tasks t
	    WHERE t.tenant_id = $1 AND t.node_key = k.node_key
	    ORDER BY t.created_at DESC, t.run_id DESC
	    LIMIT 1
	) newest
	JOIN runs s ON s.tenant_id = $1 AND s.run_id = newest.run_id`

// stampLastRuns sets the LastRunAt of each dormant node of nodes from one
// lastRunsQuery over the dormant nodes' keys; a dormant node that never ran
// and every other node keep a nil LastRunAt. Nodes with no dormant node among
// them run no query.
func (r *ScheduleGraphRepository) stampLastRuns(ctx context.Context, tenantID string, nodes []topology.ScheduleGraphNode) error {
	var dormant []string
	for _, n := range nodes {
		if n.IsDormant() {
			dormant = append(dormant, string(n.Key))
		}
	}
	if len(dormant) == 0 {
		return nil
	}
	var rows []struct {
		NodeKey   string    `db:"node_key"`
		CreatedAt time.Time `db:"created_at"`
	}
	if err := sqlx.SelectContext(ctx, r.db, &rows, lastRunsQuery, tenantID, pq.Array(dormant)); err != nil {
		return err
	}
	lastRun := make(map[topology.NodeKey]time.Time, len(rows))
	for _, row := range rows {
		lastRun[topology.NodeKey(row.NodeKey)] = row.CreatedAt
	}
	for i := range nodes {
		if at, ok := lastRun[nodes[i].Key]; ok {
			nodes[i].LastRunAt = &at
		}
	}
	return nil
}

// builtNodeTypeNames returns, as topology_nodes stores them, the node types
// model.BuiltNodeTypes names: the only types a dormant node can have. A row
// of any other type, an unknown-external node under any of its names
// included, is never dormant.
func builtNodeTypeNames() []string {
	types := model.BuiltNodeTypes()
	names := make([]string, 0, len(types))
	for _, t := range types {
		names = append(names, string(t))
	}
	return names
}

// summariesQuery reads tenant $1's live release as schedules in one statement,
// so the schedules and the dormant count are of the same release whatever
// promotion commits during the read. Each row is one schedule, counted by
// liveScheduleModelCounts (the count LiveScheduleModelRepository reads), with
// the time the release was loaded and the release's dormant node count: the
// nodes of no schedule whose type is among $2 (builtNodeTypeNames), the rule
// model.IsDormant states. A live release with no schedule yields one row with
// a NULL schedule name, and no live release yields no row.
const summariesQuery = `
	WITH counts AS (` + liveScheduleModelCounts + `),
	dormant AS (
	    SELECT count(*)::int AS nodes
	    FROM live_release live
	    JOIN topology_nodes n
	      ON n.tenant_id = live.tenant_id AND n.release_id = live.release_id AND n.schedule_name = ''
	    WHERE live.tenant_id = $1 AND n.node_type = ANY($2::text[])
	)
	SELECT c.schedule_name, c.models AS node_count, rel.loaded_at, d.nodes AS dormant_node_count
	FROM live_release live
	JOIN topology_releases rel
	  ON rel.tenant_id = live.tenant_id AND rel.release_id = live.release_id
	CROSS JOIN dormant d
	LEFT JOIN counts c ON true
	WHERE live.tenant_id = $1
	ORDER BY c.schedule_name COLLATE "C"`

// Summaries runs summariesQuery. A node whose schedule name is empty belongs
// to no schedule and is counted in none.
func (r *ScheduleGraphRepository) Summaries(ctx context.Context, tenantID string) (topology.ScheduleSummaries, error) {
	var rows []struct {
		ScheduleName     sql.NullString `db:"schedule_name"`
		NodeCount        sql.NullInt32  `db:"node_count"`
		LoadedAt         time.Time      `db:"loaded_at"`
		DormantNodeCount int            `db:"dormant_node_count"`
	}
	if err := sqlx.SelectContext(ctx, r.db, &rows, summariesQuery, tenantID, pq.Array(builtNodeTypeNames())); err != nil {
		return topology.ScheduleSummaries{}, fmt.Errorf("count the nodes per schedule of tenant %s: %w", tenantID, err)
	}
	var out topology.ScheduleSummaries
	for _, row := range rows {
		out.DormantNodeCount = row.DormantNodeCount
		if !row.ScheduleName.Valid {
			continue
		}
		out.Schedules = append(out.Schedules, topology.ScheduleSummary{
			ScheduleName: row.ScheduleName.String, NodeCount: int(row.NodeCount.Int32), LastUpdatedAt: row.LoadedAt,
		})
	}
	return out, nil
}
