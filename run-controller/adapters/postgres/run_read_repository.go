package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/projection"
	"github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/lib/pq"
)

// RunReadRepository reads run graphs, run lists and drift from Postgres. A
// run's graph joins its task rows to the nodes and edges of the run's own
// release, so a later promotion never changes a past run's graph, and joins a
// task waiting for its node's lease to the lease held on that node now.
type RunReadRepository struct {
	db *sqlx.DB
}

var _ repository.RunReadRepository = (*RunReadRepository)(nil)

// NewRunReadRepository returns a RunReadRepository over db.
func NewRunReadRepository(db *sqlx.DB) *RunReadRepository {
	return &RunReadRepository{db: db}
}

type runGraphNodeRow struct {
	TaskID         uuid.UUID  `db:"task_id"`
	ServiceName    string     `db:"service_name"`
	SchemaName     string     `db:"schema_name"`
	TableName      string     `db:"table_name"`
	Status         string     `db:"status"`
	ScheduleName   string     `db:"schedule_name"`
	NodeType       string     `db:"node_type"`
	WaitReason     string     `db:"wait_reason"`
	WaitingSince   *time.Time `db:"lease_wait_since"`
	WaitingOnRunID *uuid.UUID `db:"waiting_on_run_id"`
	Operation      string     `db:"operation"`
	PlanReason     string     `db:"plan_reason"`
}

type runGraphEdgeRow struct {
	FromNodeID string `db:"from_node_id"`
	ToNodeID   string `db:"to_node_id"`
}

// GetRunGraph implements repository.RunReadRepository.
func (r *RunReadRepository) GetRunGraph(ctx context.Context, tenantID string, runID uuid.UUID) (projection.RunGraph, error) {
	latest, err := r.livePromotionSeq(ctx, tenantID)
	if err != nil {
		return projection.RunGraph{}, err
	}
	g := projection.RunGraph{LatestPromotionSeq: latest}

	var seq sql.NullInt64
	err = r.db.GetContext(ctx, &seq, `SELECT promotion_seq FROM runs WHERE tenant_id = $1 AND run_id = $2`, tenantID, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return g, nil
	}
	if err != nil {
		return projection.RunGraph{}, fmt.Errorf("read run %s: %w", runID, err)
	}
	g.RunPromotionSeq = seq.Int64

	// A task waiting for its node's lease is joined to the lease another task
	// of the tenant holds on that node, which names the holder's run. A node
	// has at most one lease per tenant, so the join adds at most one row to a
	// task, and a task never reads its own lease as the one it waits for.
	// Every row joined is the tenant's.
	var nodes []runGraphNodeRow
	if err := r.db.SelectContext(ctx, &nodes, `
		SELECT t.task_id, t.service_name, t.schema_name, t.table_name, t.status,
		       COALESCE(n.schedule_name, '') AS schedule_name,
		       COALESCE(n.node_type, '')     AS node_type,
		       COALESCE(t.wait_reason, '')   AS wait_reason,
		       t.lease_wait_since,
		       l.run_id                      AS waiting_on_run_id,
		       t.operation,
		       t.plan_reason
		FROM tasks t
		JOIN runs s ON s.tenant_id = t.tenant_id AND s.run_id = t.run_id
		LEFT JOIN topology_nodes n
		  ON n.tenant_id = t.tenant_id AND n.release_id = s.release_id AND n.node_key = t.node_key
		LEFT JOIN node_leases l
		  ON t.wait_reason = 'node_lease' AND l.tenant_id = t.tenant_id
		 AND l.node_key = t.node_key AND l.task_id <> t.task_id
		WHERE t.tenant_id = $2 AND t.run_id = $1
		ORDER BY t.service_name, t.schema_name, t.table_name`, runID, tenantID); err != nil {
		return projection.RunGraph{}, fmt.Errorf("read tasks of run %s: %w", runID, err)
	}
	for _, n := range nodes {
		g.Nodes = append(g.Nodes, projection.RunGraphNode{
			ServiceName: n.ServiceName, SchemaName: n.SchemaName, TableName: n.TableName,
			ScheduleName: n.ScheduleName, NodeType: n.NodeType,
			Status: run.TaskStatus(n.Status), TaskID: n.TaskID,
			WaitReason: run.WaitReason(n.WaitReason), WaitingSince: n.WaitingSince,
			WaitingOnRunID: n.WaitingOnRunID,
			Operation:      operationFromColumn(n.Operation), PlanReason: n.PlanReason,
		})
	}

	var edges []runGraphEdgeRow
	if err := r.db.SelectContext(ctx, &edges, `
		SELECT d.service_name || '.' || d.schema_name || '.' || d.table_name AS from_node_id,
		       u.service_name || '.' || u.schema_name || '.' || u.table_name AS to_node_id
		FROM runs s
		JOIN topology_edges e ON e.tenant_id = s.tenant_id AND e.release_id = s.release_id
		JOIN tasks u ON u.tenant_id = s.tenant_id AND u.run_id = s.run_id AND u.node_key = e.upstream_key
		JOIN tasks d ON d.tenant_id = s.tenant_id AND d.run_id = s.run_id AND d.node_key = e.downstream_key
		WHERE s.tenant_id = $2 AND s.run_id = $1
		ORDER BY from_node_id, to_node_id`, runID, tenantID); err != nil {
		return projection.RunGraph{}, fmt.Errorf("read edges of run %s: %w", runID, err)
	}
	for _, e := range edges {
		g.Edges = append(g.Edges, projection.RunGraphEdge{FromNodeID: e.FromNodeID, ToNodeID: e.ToNodeID})
	}
	return g, nil
}

type runSummaryRow struct {
	RunID        uuid.UUID  `db:"run_id"`
	ScheduleName string     `db:"schedule_name"`
	Kind         string     `db:"kind"`
	Status       string     `db:"status"`
	ReleaseID    *string    `db:"release_id"`
	FailReason   *string    `db:"failure_reason"`
	CreatedAt    time.Time  `db:"created_at"`
	CompletedAt  *time.Time `db:"completed_at"`
}

const runSummaryColumns = `run_id, schedule_name, kind, status, release_id, failure_reason, created_at, completed_at`

func (row runSummaryRow) summary() projection.RunSummary {
	s := projection.RunSummary{
		RunID: row.RunID, ScheduleName: row.ScheduleName, Kind: run.Kind(row.Kind),
		Status: run.RunStatus(row.Status), ReleaseID: stringOrEmpty(row.ReleaseID),
		CreatedAt: row.CreatedAt, CompletedAt: row.CompletedAt,
	}
	if row.FailReason != nil {
		s.FailureReason = run.RunFailureReason(*row.FailReason)
	}
	return s
}

// ListCompletedRuns implements repository.RunReadRepository.
func (r *RunReadRepository) ListCompletedRuns(ctx context.Context, tenantID, scheduleName string, limit, offset int) ([]projection.RunSummary, int, error) {
	var total int
	if err := r.db.GetContext(ctx, &total, `
		SELECT count(*) FROM runs
		WHERE tenant_id = $2 AND schedule_name = $1 AND completed_at IS NOT NULL`, scheduleName, tenantID); err != nil {
		return nil, 0, fmt.Errorf("count completed runs of %s: %w", scheduleName, err)
	}
	var rows []runSummaryRow
	if err := r.db.SelectContext(ctx, &rows, `
		SELECT `+runSummaryColumns+`
		FROM runs
		WHERE tenant_id = $4 AND schedule_name = $1 AND completed_at IS NOT NULL
		ORDER BY created_at DESC, run_id
		LIMIT $2 OFFSET $3`, scheduleName, limit, offset, tenantID); err != nil {
		return nil, 0, fmt.Errorf("list completed runs of %s: %w", scheduleName, err)
	}
	return summariesOf(rows), total, nil
}

// ListReleaseRuns implements repository.RunReadRepository.
func (r *RunReadRepository) ListReleaseRuns(ctx context.Context, tenantID, releaseID, kind string, limit, offset int) ([]projection.RunSummary, int, error) {
	var total int
	if err := r.db.GetContext(ctx, &total, `
		SELECT count(*) FROM runs
		WHERE tenant_id = $1 AND release_id = $2 AND ($3 = '' OR kind = $3)`, tenantID, releaseID, kind); err != nil {
		return nil, 0, fmt.Errorf("count the runs of release %s: %w", releaseID, err)
	}
	var rows []runSummaryRow
	if err := r.db.SelectContext(ctx, &rows, `
		SELECT `+runSummaryColumns+`
		FROM runs
		WHERE tenant_id = $1 AND release_id = $2 AND ($3 = '' OR kind = $3)
		ORDER BY created_at DESC, run_id
		LIMIT $4 OFFSET $5`, tenantID, releaseID, kind, limit, offset); err != nil {
		return nil, 0, fmt.Errorf("list the runs of release %s: %w", releaseID, err)
	}
	return summariesOf(rows), total, nil
}

func summariesOf(rows []runSummaryRow) []projection.RunSummary {
	runs := make([]projection.RunSummary, 0, len(rows))
	for _, row := range rows {
		runs = append(runs, row.summary())
	}
	return runs
}

type activeRunRow struct {
	ScheduleName string        `db:"schedule_name"`
	RunID        uuid.UUID     `db:"run_id"`
	PromotionSeq sql.NullInt64 `db:"promotion_seq"`
}

// ListActiveRunDrifts implements repository.RunReadRepository.
func (r *RunReadRepository) ListActiveRunDrifts(ctx context.Context, tenantID string) (int64, []projection.ActiveRunDrift, error) {
	latest, err := r.livePromotionSeq(ctx, tenantID)
	if err != nil {
		return 0, nil, err
	}
	var rows []activeRunRow
	if err := r.db.SelectContext(ctx, &rows, `
		SELECT DISTINCT ON (schedule_name) schedule_name, run_id, promotion_seq
		FROM runs
		WHERE tenant_id = $2 AND status = ANY($1::text[])
		ORDER BY schedule_name, created_at DESC`, pq.Array(activeRunStatuses()), tenantID); err != nil {
		return 0, nil, fmt.Errorf("list active runs: %w", err)
	}
	drifts := make([]projection.ActiveRunDrift, 0, len(rows))
	for _, row := range rows {
		drifts = append(drifts, projection.ActiveRunDrift{
			ScheduleName: row.ScheduleName, RunID: row.RunID, RunPromotionSeq: row.PromotionSeq.Int64,
		})
	}
	return latest, drifts, nil
}

// livePromotionSeq returns the tenant's live promotion seq, 0 when no release
// is live.
func (r *RunReadRepository) livePromotionSeq(ctx context.Context, tenantID string) (int64, error) {
	var seq int64
	err := r.db.GetContext(ctx, &seq, `SELECT promotion_seq FROM live_release WHERE tenant_id = $1`, tenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read live promotion seq: %w", err)
	}
	return seq, nil
}
