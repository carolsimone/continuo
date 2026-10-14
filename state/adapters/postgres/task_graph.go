package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// readyCandidatesQuery reads the pending, undispatched tasks of run $2 that
// read node $4 directly over release $3's edges, with the statuses of their
// upstream tasks in the run. Each lookup is an index probe: topology_edges by
// its primary key prefix (tenant, release, upstream) or by
// idx_topology_edges_downstream, task_tracker by uq_task_tracker_run_node.
const readyCandidatesQuery = `
	SELECT t.task_id,
	       n.node_key, n.unique_id, n.service_name, n.schema_name, n.table_name, n.node_type, n.test_count,
	       n.content_hash, n.image_tag, n.secret_ref, n.schedule_name,
	       ARRAY(
	           SELECT ut.status::text
	           FROM topology_edges ue
	           JOIN task_tracker ut
	             ON ut.schedule_id = t.schedule_id AND ut.node_key = ue.upstream_key
	           WHERE ue.tenant_id = e.tenant_id AND ue.release_id = e.release_id
	             AND ue.downstream_key = e.downstream_key
	           ORDER BY ue.upstream_key COLLATE "C"
	       ) AS upstream_statuses
	FROM topology_edges e
	JOIN task_tracker t
	  ON t.schedule_id = $2 AND t.node_key = e.downstream_key
	JOIN topology_nodes n
	  ON n.tenant_id = e.tenant_id AND n.release_id = e.release_id AND n.node_key = e.downstream_key
	WHERE e.tenant_id = $1 AND e.release_id = $3 AND e.upstream_key = $4
	  AND t.status = 'pending' AND t.dispatched_at IS NULL
	ORDER BY e.downstream_key COLLATE "C"`

// pendingDescendantsQuery walks release $3's edges down from node $4 through
// the pending, undispatched tasks of run $2. UNION keeps each node once, so
// the walk expands every node once however many paths reach it.
const pendingDescendantsQuery = `
	WITH RECURSIVE blocked(node_key) AS (
	    SELECT t.node_key
	    FROM topology_edges e
	    JOIN task_tracker t
	      ON t.schedule_id = $2 AND t.node_key = e.downstream_key
	    WHERE e.tenant_id = $1 AND e.release_id = $3 AND e.upstream_key = $4
	      AND t.status = 'pending' AND t.dispatched_at IS NULL
	  UNION
	    SELECT t.node_key
	    FROM blocked b
	    JOIN topology_edges e
	      ON e.tenant_id = $1 AND e.release_id = $3 AND e.upstream_key = b.node_key
	    JOIN task_tracker t
	      ON t.schedule_id = $2 AND t.node_key = e.downstream_key
	    WHERE t.status = 'pending' AND t.dispatched_at IS NULL
	)
	SELECT t.task_id
	FROM blocked b
	JOIN task_tracker t
	  ON t.schedule_id = $2 AND t.node_key = b.node_key
	ORDER BY b.node_key COLLATE "C"`

// TaskGraph implements run.TaskGraph over task_tracker and the topology
// tables of one tenant, inside the unit of work's transaction.
type TaskGraph struct {
	tx       *sqlx.Tx
	tenantID string
}

// Compile-time assertion that the adapter satisfies the domain port.
var _ run.TaskGraph = (*TaskGraph)(nil)

// NewTaskGraph binds a TaskGraph to tx and tenantID.
func NewTaskGraph(tx *sqlx.Tx, tenantID string) *TaskGraph {
	return &TaskGraph{tx: tx, tenantID: tenantID}
}

type readyCandidateRow struct {
	TaskID uuid.UUID `db:"task_id"`
	topologyNodeRow
	UpstreamStatuses pq.StringArray `db:"upstream_statuses"`
}

// ReadyCandidates runs readyCandidatesQuery.
func (g *TaskGraph) ReadyCandidates(ctx context.Context, runID uuid.UUID, releaseID string, key topology.NodeKey) ([]run.ReadyCandidate, error) {
	if g.tx == nil {
		return nil, errors.New("ReadyCandidates requires an active transaction")
	}
	var rows []readyCandidateRow
	if err := g.tx.SelectContext(ctx, &rows, readyCandidatesQuery, g.tenantID, runID, releaseID, string(key)); err != nil {
		return nil, fmt.Errorf("read the tasks of run %s that read %s: %w", runID, key, err)
	}
	out := make([]run.ReadyCandidate, 0, len(rows))
	for _, row := range rows {
		statuses := make([]run.TaskStatus, 0, len(row.UpstreamStatuses))
		for _, s := range row.UpstreamStatuses {
			status, err := run.ParseTaskStatus(s)
			if err != nil {
				return nil, fmt.Errorf("task %s reads a task with status %q: %w", row.TaskID, s, err)
			}
			statuses = append(statuses, status)
		}
		out = append(out, run.ReadyCandidate{TaskID: row.TaskID, Node: row.toDomain(), UpstreamStatuses: statuses})
	}
	return out, nil
}

// PendingDescendants runs pendingDescendantsQuery.
func (g *TaskGraph) PendingDescendants(ctx context.Context, runID uuid.UUID, releaseID string, key topology.NodeKey) ([]uuid.UUID, error) {
	if g.tx == nil {
		return nil, errors.New("PendingDescendants requires an active transaction")
	}
	var ids []uuid.UUID
	if err := g.tx.SelectContext(ctx, &ids, pendingDescendantsQuery, g.tenantID, runID, releaseID, string(key)); err != nil {
		return nil, fmt.Errorf("read the pending descendants of %s in run %s: %w", key, runID, err)
	}
	return ids, nil
}
