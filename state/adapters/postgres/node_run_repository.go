// Package postgres provides PostgreSQL-backed repository implementations.
//
// NodeRunRepository is the read-only audit projection of the per-node history
// surface. It joins scheduler_tracker × task_tracker × task_execution filtered
// by node identity and emits a flat NodeRun row per task instance.
//
// Why a separate repo (not a method on TaskTrackerRepository):
//   - The query joins three tables; living on the per-table CRUD repo would
//     pull cross-table concerns into single-table code.
//   - The shape is read-only (no Create/Update/Delete) and audit-shaped (one
//     row per task instance with run-level joins). Distinct enough to deserve
//     its own port.
//   - Future per-node queries (counts, latest-N-failures) can land here without
//     widening unrelated repos.
package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/jmoiron/sqlx"
)

// NodeRunRepository reads per-node run history.
type NodeRunRepository interface {
	List(ctx context.Context, serviceName, schemaName, tableName string, limit int) ([]*projection.NodeRun, error)
	ListNodes(ctx context.Context, search, serviceName string, limit, offset int) ([]*projection.NodeSummary, int, error)
}

type nodeRunRepository struct {
	db     *sqlx.DB
	logger *slog.Logger
}

var _ NodeRunRepository = (*nodeRunRepository)(nil)

// NewNodeRunRepository constructs a NodeRunRepository backed by Postgres.
func NewNodeRunRepository(db *sqlx.DB, logger *slog.Logger) NodeRunRepository {
	return &nodeRunRepository{db: db, logger: logger}
}

// List returns the most recent task instances for the given node identity,
// ordered by scheduler_tracker.created_at DESC.
//
// Each row carries:
//   - run-level: run_id, schedule_name, kind, terminal_status
//   - task-level: task_status, retry_count, image_tag, manifest_version
//   - exec-level (latest execution per task): started_at, completed_at,
//     error_message, log_s3_key
//
// Tasks with no execution row yield rows with nil timings.
// limit is clamped to (0, 50]; non-positive or oversized values default to 50.
func (r *nodeRunRepository) List(
	ctx context.Context,
	serviceName, schemaName, tableName string,
	limit int,
) ([]*projection.NodeRun, error) {
	if limit <= 0 || limit > 50 {
		limit = 50
	}

	const query = `
		WITH target_tasks AS (
			SELECT t.task_id, t.schedule_id, t.status AS task_status,
			       t.retry_count, t.image_tag, t.manifest_version
			FROM task_tracker t
			WHERE t.service_name = $1
			  AND t.schema_name  = $2
			  AND t.table_name   = $3
		),
		latest_exec AS (
			SELECT DISTINCT ON (te.task_id)
			       te.task_id, te.started_at, te.completed_at,
			       te.error_message, te.log_s3_key
			FROM task_execution te
			JOIN target_tasks tt ON tt.task_id = te.task_id
			ORDER BY te.task_id, te.created_at DESC
		)
		SELECT s.schedule_id        AS run_id,
		       s.schedule_name      AS schedule_name,
		       s.kind               AS kind,
		       s.status             AS terminal_status,
		       tt.task_id           AS task_id,
		       tt.task_status       AS task_status,
		       tt.retry_count       AS retry_count,
		       tt.image_tag         AS image_tag,
		       tt.manifest_version  AS manifest_version,
		       s.created_at         AS created_at,
		       le.started_at        AS started_at,
		       le.completed_at      AS completed_at,
		       le.error_message     AS error_message,
		       le.log_s3_key        AS log_s3_key
		FROM target_tasks tt
		JOIN scheduler_tracker s ON s.schedule_id = tt.schedule_id
		LEFT JOIN latest_exec le ON le.task_id = tt.task_id
		ORDER BY s.created_at DESC
		LIMIT $4
	`

	rows := []*projection.NodeRun{}
	if err := r.db.SelectContext(ctx, &rows, query,
		serviceName, schemaName, tableName, limit); err != nil {
		r.logger.Error("Failed to list node runs",
			"service", serviceName, "schema", schemaName, "table", tableName,
			"error", err)
		return nil, fmt.Errorf("failed to list node runs: %w", err)
	}
	return rows, nil
}

type nodeSummaryRow struct {
	ServiceName    string    `db:"service_name"`
	SchemaName     string    `db:"schema_name"`
	TableName      string    `db:"table_name"`
	RunCount       int       `db:"run_count"`
	TerminalCount  int       `db:"terminal_count"`
	SucceededCount int       `db:"succeeded_count"`
	AvgDur         *float64  `db:"avg_dur"`
	P95Dur         *float64  `db:"p95_dur"`
	FlakyCount     int       `db:"flaky_count"`
	LastStatus     string    `db:"last_status"`
	LastRunAt      time.Time `db:"last_run_at"`
}

// ListNodes returns the node catalog: one summary per node that has run, with
// stats aggregated over each node's most recent 50 runs. Filters by exact
// service (when non-empty) and a case-insensitive substring on the fqn (when
// non-empty), ordered by last_run_at DESC with (service, schema, table) identity
// tiebreakers for deterministic paging, and paged by limit/offset. The second
// return value is the total match count before paging, computed independently of
// the page so an empty page still reports the true total.
func (r *nodeRunRepository) ListNodes(
	ctx context.Context,
	search, serviceName string,
	limit, offset int,
) ([]*projection.NodeSummary, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	// rankedWindowedCTE is shared verbatim by the count query and the page query
	// so their filter and windowing can never drift. The search predicate escapes
	// LIKE wildcards (\ % _) in $1 — backslash first — so literal underscores in
	// dbt names (fct_orders) don't match an arbitrary character.
	const rankedWindowedCTE = `
		WITH ranked AS (
			SELECT t.task_id, t.service_name, t.schema_name, t.table_name,
			       t.retry_count, t.status AS run_status, s.created_at,
			       ROW_NUMBER() OVER (
			         PARTITION BY t.service_name, t.schema_name, t.table_name
			         ORDER BY s.created_at DESC
			       ) AS rn
			FROM task_tracker t
			JOIN scheduler_tracker s ON s.schedule_id = t.schedule_id
			WHERE ($1 = '' OR lower(t.service_name||'.'||t.schema_name||'.'||t.table_name)
			                  LIKE '%' || replace(replace(replace(lower($1), '\', '\\'), '%', '\%'), '_', '\_') || '%' ESCAPE '\')
			  AND ($2 = '' OR t.service_name = $2)
		),
		windowed AS ( SELECT * FROM ranked WHERE rn <= 50 )`

	// total_count is computed independently of paging so an empty page (offset
	// past the end) still reports the true match count.
	countQuery := rankedWindowedCTE + `
		SELECT COUNT(*) FROM (
			SELECT 1 FROM windowed GROUP BY service_name, schema_name, table_name
		) g`
	var total int
	if err := r.db.GetContext(ctx, &total, countQuery, search, serviceName); err != nil {
		r.logger.Error("Failed to count nodes", "search", search, "service", serviceName, "error", err)
		return nil, 0, fmt.Errorf("failed to count nodes: %w", err)
	}

	pageQuery := rankedWindowedCTE + `,
		-- latest execution per task; DISTINCT ON is satisfied by idx_task_execution_task_id
		latest_exec AS (
			SELECT DISTINCT ON (te.task_id)
			       te.task_id, te.started_at, te.completed_at
			FROM task_execution te
			JOIN windowed w ON w.task_id = te.task_id
			ORDER BY te.task_id, te.created_at DESC
		),
		agg AS (
			SELECT
			  w.service_name, w.schema_name, w.table_name,
			  COUNT(*) AS run_count,
			  COUNT(*) FILTER (WHERE w.run_status IN ('succeeded','failed','cancelled','skipped')) AS terminal_count,
			  COUNT(*) FILTER (WHERE w.run_status = 'succeeded') AS succeeded_count,
			  AVG(EXTRACT(EPOCH FROM (le.completed_at - le.started_at)))
			     FILTER (WHERE le.completed_at IS NOT NULL AND le.started_at IS NOT NULL) AS avg_dur,
			  PERCENTILE_CONT(0.95) WITHIN GROUP (
			     ORDER BY EXTRACT(EPOCH FROM (le.completed_at - le.started_at)))
			     FILTER (WHERE le.completed_at IS NOT NULL AND le.started_at IS NOT NULL) AS p95_dur,
			  COUNT(*) FILTER (WHERE w.retry_count > 0) AS flaky_count,
			  -- [1] = most-recent run_status
			  (ARRAY_AGG(w.run_status ORDER BY w.created_at DESC))[1] AS last_status,
			  MAX(w.created_at) AS last_run_at
			FROM windowed w
			LEFT JOIN latest_exec le ON le.task_id = w.task_id
			GROUP BY w.service_name, w.schema_name, w.table_name
		)
		SELECT service_name, schema_name, table_name, run_count, terminal_count,
		       succeeded_count, avg_dur, p95_dur, flaky_count, last_status, last_run_at
		FROM agg
		ORDER BY last_run_at DESC, service_name, schema_name, table_name
		LIMIT $3 OFFSET $4`

	rows := []nodeSummaryRow{}
	if err := r.db.SelectContext(ctx, &rows, pageQuery, search, serviceName, limit, offset); err != nil {
		r.logger.Error("Failed to list nodes", "search", search, "service", serviceName, "error", err)
		return nil, 0, fmt.Errorf("failed to list nodes: %w", err)
	}

	out := make([]*projection.NodeSummary, 0, len(rows))
	for _, row := range rows {
		out = append(out, toNodeSummary(row))
	}
	return out, total, nil
}

func toNodeSummary(row nodeSummaryRow) *projection.NodeSummary {
	successPct := -1
	if row.TerminalCount > 0 {
		successPct = int(math.Round(float64(row.SucceededCount) / float64(row.TerminalCount) * 100))
	}
	avgSec := -1
	if row.AvgDur != nil {
		avgSec = int(math.Round(*row.AvgDur))
	}
	p95Sec := -1
	if row.P95Dur != nil {
		p95Sec = int(math.Round(*row.P95Dur))
	}
	flakyPct := 0
	if row.RunCount > 0 {
		flakyPct = int(math.Round(float64(row.FlakyCount) / float64(row.RunCount) * 100))
	}
	return &projection.NodeSummary{
		ServiceName:    row.ServiceName,
		SchemaName:     row.SchemaName,
		TableName:      row.TableName,
		RunCount:       row.RunCount,
		SuccessRatePct: successPct,
		AvgDurationSec: avgSec,
		P95DurationSec: p95Sec,
		FlakyRatePct:   flakyPct,
		LastStatus:     row.LastStatus,
		LastRunAt:      row.LastRunAt,
	}
}
