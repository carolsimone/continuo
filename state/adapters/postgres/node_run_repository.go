// Package postgres provides PostgreSQL-backed repository implementations.
//
// NodeRunRepository is the read-only audit projection of the per-node history
// surface. It joins runs × tasks × task_attempts filtered
// by node identity and emits a flat NodeRun row per task instance.
//
// Why a separate repo (not a method on TaskCollection):
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

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// NodeRunRepository reads per-node run history.
type NodeRunRepository interface {
	List(ctx context.Context, serviceName, schemaName, tableName, operation string, limit int) ([]*projection.NodeRun, error)
	ListNodes(ctx context.Context, search, serviceName, operation string, limit, offset int) ([]*projection.NodeSummary, int, error)
	ListNodeNames(ctx context.Context, serviceName string) ([]string, error)
	ListNodeServices(ctx context.Context) ([]string, error)
}

type nodeRunRepository struct {
	db *sqlx.DB
	// tenantID is the tenant whose runs, tasks, attempts, topology and node
	// leases the reads read.
	tenantID string
	logger   *slog.Logger
}

var _ NodeRunRepository = (*nodeRunRepository)(nil)

// NewNodeRunRepository constructs a NodeRunRepository backed by Postgres that
// reads the runs, tasks, attempts, topology and node leases of tenantID.
func NewNodeRunRepository(db *sqlx.DB, tenantID string, logger *slog.Logger) NodeRunRepository {
	return &nodeRunRepository{db: db, tenantID: tenantID, logger: logger}
}

// List returns the most recent task instances for the given node identity,
// ordered by runs.created_at DESC.
//
// Each row carries:
//   - run-level: run_id, schedule_name, kind, terminal_status
//   - task-level: task_status, attempt, image_tag, and, while the task waits
//     for the node's lease, wait_reason, lease_wait_since and the run whose
//     task holds the lease in the repository's tenant
//   - attempt-level (latest attempt per task): started_at, completed_at
//     (the attempt's finished_at), error_message, log_s3_key, run_results_uri,
//     failure_class, failure_reason
//
// Tasks with no attempt row yield rows with nil timings.
// limit is clamped to (0, 50]; non-positive or oversized values default to 50.
// operation defaults to "run" when empty; the run dimension includes
// full_refresh tasks, since a full refresh materializes the node just as a
// run does.
func (r *nodeRunRepository) List(
	ctx context.Context,
	serviceName, schemaName, tableName, operation string,
	limit int,
) ([]*projection.NodeRun, error) {
	if limit <= 0 || limit > 50 {
		limit = 50
	}
	if operation == "" {
		operation = "run"
	}

	const query = `
		WITH target_tasks AS (
			SELECT t.task_id, t.run_id, t.status AS task_status,
			       t.attempt, t.image_tag, t.operation, t.node_key,
			       t.wait_reason, t.lease_wait_since
			FROM tasks t
			WHERE t.tenant_id    = $6
			  AND t.service_name = $1
			  AND t.schema_name  = $2
			  AND t.table_name   = $3
			  AND (t.operation = $4 OR ($4 = 'run' AND t.operation = 'full_refresh'))
		),
		-- latest attempt per task; DISTINCT ON is satisfied by task_attempts_pkey
		latest_attempt AS (
			SELECT DISTINCT ON (a.task_id)
			       a.task_id, a.started_at, a.finished_at AS completed_at,
			       a.error_message, a.log_s3_key, a.run_results_uri,
			       a.failure_class, a.failure_reason
			FROM task_attempts a
			JOIN target_tasks tt ON a.tenant_id = $6 AND tt.task_id = a.task_id
			ORDER BY a.task_id, a.attempt DESC
		)
		SELECT s.run_id,
		       s.schedule_name      AS schedule_name,
		       s.kind               AS kind,
		       s.status             AS terminal_status,
		       tt.task_id           AS task_id,
		       tt.task_status       AS task_status,
		       tt.attempt           AS attempt,
		       tt.image_tag         AS image_tag,
		       s.created_at         AS created_at,
		       le.started_at        AS started_at,
		       le.completed_at      AS completed_at,
		       le.error_message     AS error_message,
		       le.log_s3_key        AS log_s3_key,
		       le.run_results_uri   AS run_results_uri,
		       le.failure_class     AS failure_class,
		       le.failure_reason    AS failure_reason,
		       tt.operation         AS operation,
		       COALESCE(n.content_hash, '') AS content_hash,
		       COALESCE(tt.wait_reason, '') AS wait_reason,
		       tt.lease_wait_since  AS waiting_since,
		       l.run_id             AS waiting_on_run_id
		FROM target_tasks tt
		JOIN runs s ON s.tenant_id = $6 AND s.run_id = tt.run_id
		LEFT JOIN latest_attempt le ON le.task_id = tt.task_id
		LEFT JOIN topology_nodes n
		  ON n.tenant_id = $6 AND n.release_id = s.release_id AND n.node_key = tt.node_key
		-- the lease another task holds on the node a waiting task waits for: at
		-- most one per tenant and node, and never the task's own
		LEFT JOIN node_leases l
		  ON tt.wait_reason = 'node_lease' AND l.tenant_id = $6
		 AND l.node_key = tt.node_key AND l.task_id <> tt.task_id
		ORDER BY s.created_at DESC
		LIMIT $5
	`

	rows := []nodeRunRow{}
	if err := r.db.SelectContext(ctx, &rows, query,
		serviceName, schemaName, tableName, operation, limit, r.tenantID); err != nil {
		r.logger.Error("Failed to list node runs",
			"service", serviceName, "schema", schemaName, "table", tableName,
			"error", err)
		return nil, fmt.Errorf("failed to list node runs: %w", err)
	}
	out := make([]*projection.NodeRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, toNodeRun(row))
	}
	return out, nil
}

// nodeRunRow is the adapter-side scan carrier for the per-node run-history
// query. The db column tags live here, keeping projection.NodeRun free of
// storage concerns.
type nodeRunRow struct {
	ScheduleID     uuid.UUID      `db:"run_id"`
	ScheduleName   string         `db:"schedule_name"`
	Kind           string         `db:"kind"`
	TerminalStatus string         `db:"terminal_status"`
	TaskID         uuid.UUID      `db:"task_id"`
	TaskStatus     run.TaskStatus `db:"task_status"`
	Attempt        int            `db:"attempt"`
	ImageTag       string         `db:"image_tag"`
	CreatedAt      time.Time      `db:"created_at"`
	StartedAt      *time.Time     `db:"started_at"`
	CompletedAt    *time.Time     `db:"completed_at"`
	ErrorMessage   *string        `db:"error_message"`
	LogS3Key       *string        `db:"log_s3_key"`
	RunResultsURI  *string        `db:"run_results_uri"`
	FailureClass   *string        `db:"failure_class"`
	FailureReason  *string        `db:"failure_reason"`
	Operation      string         `db:"operation"`
	ContentHash    string         `db:"content_hash"`
	WaitReason     string         `db:"wait_reason"`
	WaitingSince   *time.Time     `db:"waiting_since"`
	WaitingOnRunID *uuid.UUID     `db:"waiting_on_run_id"`
}

func toNodeRun(row nodeRunRow) *projection.NodeRun {
	return &projection.NodeRun{
		ScheduleID:     row.ScheduleID,
		ScheduleName:   row.ScheduleName,
		Kind:           row.Kind,
		TerminalStatus: row.TerminalStatus,
		TaskID:         row.TaskID,
		TaskStatus:     row.TaskStatus,
		Attempt:        row.Attempt,
		ImageTag:       row.ImageTag,
		CreatedAt:      row.CreatedAt,
		StartedAt:      row.StartedAt,
		CompletedAt:    row.CompletedAt,
		ErrorMessage:   row.ErrorMessage,
		LogS3Key:       row.LogS3Key,
		RunResultsURI:  row.RunResultsURI,
		FailureClass:   row.FailureClass,
		FailureReason:  row.FailureReason,
		Operation:      row.Operation,
		ContentHash:    row.ContentHash,
		WaitReason:     run.WaitReason(row.WaitReason),
		WaitingSince:   row.WaitingSince,
		WaitingOnRunID: row.WaitingOnRunID,
	}
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
	Operation      string    `db:"operation"`
	// TimeoutSeconds is the live release's declared task timeout for the node;
	// nil when it declares none or the live release does not hold the node.
	TimeoutSeconds *int `db:"timeout_seconds"`
	// TotalCount is COUNT(*) OVER () — the same match count on every page row.
	// Only populated by the ListNodes page query.
	TotalCount int `db:"total_count"`
}

// ListNodes returns the node catalog: one summary per node that has run, with
// stats aggregated over each node's most recent 50 runs. Filters by exact
// service (when non-empty), a case-insensitive exact match on the table name
// (when non-empty), and the run/test/build operation dimension (run includes
// full_refresh) (operation defaults to "run" when empty), ordered by
// last_run_at DESC with (service, schema, table) identity tiebreakers for
// deterministic paging, and paged by limit/offset. The second return value is
// the total match count before paging, computed independently of the page so
// an empty page still reports the true total.
func (r *nodeRunRepository) ListNodes(
	ctx context.Context,
	search, serviceName, operation string,
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
	if operation == "" {
		operation = "run"
	}

	// rankedWindowedCTE filters and caps each node to its most recent 50 runs.
	// $1 is a case-insensitive EXACT match on the table name (not a substring):
	// searching "table_2" returns only "table_2", never "ftable_2" or
	// "table_2_v2". Service is filtered via $2, the run/test/build dimension
	// via $3 (an exact match — always non-empty, since the caller defaults it
	// to "run"), and the tenant via $4. The run dimension includes
	// full_refresh tasks and reports them as operation 'run', so a node keeps
	// one catalog row per dimension. The window-function scan over tasks ⋈
	// runs runs ONCE: the page query layers the per-node aggregation and a
	// COUNT(*) OVER () total on top of this single scan, so the match count no
	// longer requires a second pass.
	const rankedWindowedCTE = `
		WITH ranked AS (
			SELECT t.task_id, t.service_name, t.schema_name, t.table_name,
			       t.attempt, t.status AS run_status, s.created_at,
			       CASE WHEN t.operation = 'full_refresh' THEN 'run' ELSE t.operation END AS operation,
			       ROW_NUMBER() OVER (
			         PARTITION BY t.service_name, t.schema_name, t.table_name
			         ORDER BY s.created_at DESC
			       ) AS rn
			FROM tasks t
			JOIN runs s ON s.tenant_id = t.tenant_id AND s.run_id = t.run_id
			WHERE t.tenant_id = $4
			  AND ($1 = '' OR lower(t.table_name) = lower($1))
			  AND ($2 = '' OR t.service_name = $2)
			  AND (t.operation = $3 OR ($3 = 'run' AND t.operation = 'full_refresh'))
		),
		windowed AS ( SELECT * FROM ranked WHERE rn <= 50 )`

	// total_count is COUNT(*) OVER () the full agg set (one row per node) before
	// paging, so each returned row carries the true match count and a single
	// scan covers both the page and the total.
	pageQuery := rankedWindowedCTE + `,
		-- each windowed task's succeeded attempt, probed through task_attempts_pkey;
		-- only a succeeded attempt contributes a duration
		succeeded_attempt AS (
			SELECT DISTINCT ON (a.task_id)
			       a.task_id, a.started_at, a.finished_at
			FROM task_attempts a
			JOIN windowed w ON w.task_id = a.task_id
			WHERE a.tenant_id = $4 AND a.status = 'succeeded'
			ORDER BY a.task_id, a.attempt DESC
		),
		agg AS (
			SELECT
			  w.service_name, w.schema_name, w.table_name,
			  COUNT(*) AS run_count,
			  COUNT(*) FILTER (WHERE w.run_status IN ('succeeded','failed','cancelled','skipped')) AS terminal_count,
			  COUNT(*) FILTER (WHERE w.run_status = 'succeeded') AS succeeded_count,
			  AVG(EXTRACT(EPOCH FROM (sa.finished_at - sa.started_at)))
			     FILTER (WHERE sa.finished_at IS NOT NULL AND sa.started_at IS NOT NULL) AS avg_dur,
			  PERCENTILE_CONT(0.95) WITHIN GROUP (
			     ORDER BY EXTRACT(EPOCH FROM (sa.finished_at - sa.started_at)))
			     FILTER (WHERE sa.finished_at IS NOT NULL AND sa.started_at IS NOT NULL) AS p95_dur,
			  COUNT(*) FILTER (WHERE w.attempt > 0) AS flaky_count,
			  -- [1] = most-recent run_status
			  (ARRAY_AGG(w.run_status ORDER BY w.created_at DESC))[1] AS last_status,
			  MAX(w.created_at) AS last_run_at,
			  w.operation
			FROM windowed w
			LEFT JOIN succeeded_attempt sa ON sa.task_id = w.task_id
			GROUP BY w.service_name, w.schema_name, w.table_name, w.operation
		),
		-- the task timeout each node declares in the live release, one row per
		-- service, schema and table so the join never multiplies catalog rows
		live_timeouts AS (
			SELECT n.service_name, n.schema_name, n.table_name,
			       MAX(n.timeout_seconds) AS timeout_seconds
			FROM live_release l
			JOIN topology_nodes n
			  ON n.tenant_id = l.tenant_id AND n.release_id = l.release_id
			WHERE l.tenant_id = $4
			GROUP BY n.service_name, n.schema_name, n.table_name
		)
		SELECT agg.service_name, agg.schema_name, agg.table_name, agg.run_count,
		       agg.terminal_count, agg.succeeded_count, agg.avg_dur, agg.p95_dur,
		       agg.flaky_count, agg.last_status, agg.last_run_at, agg.operation,
		       lt.timeout_seconds,
		       COUNT(*) OVER () AS total_count
		FROM agg
		LEFT JOIN live_timeouts lt
		  ON lt.service_name = agg.service_name
		 AND lt.schema_name = agg.schema_name
		 AND lt.table_name = agg.table_name
		ORDER BY agg.last_run_at DESC, agg.service_name, agg.schema_name, agg.table_name
		LIMIT $5 OFFSET $6`

	rows := []nodeSummaryRow{}
	if err := r.db.SelectContext(ctx, &rows, pageQuery, search, serviceName, operation, r.tenantID, limit, offset); err != nil {
		r.logger.Error("Failed to list nodes", "search", search, "service", serviceName, "error", err)
		return nil, 0, fmt.Errorf("failed to list nodes: %w", err)
	}

	out := make([]*projection.NodeSummary, 0, len(rows))
	total := 0
	for _, row := range rows {
		out = append(out, toNodeSummary(row))
		total = row.TotalCount
	}

	// COUNT(*) OVER () travels on the page rows, so an empty page (offset past
	// the end, or zero matches) yields no rows and no total. Recover the true
	// match count with a cheap count-only pass — this second scan happens only
	// on empty pages, not on the common path.
	if len(rows) == 0 {
		countQuery := rankedWindowedCTE + `
			SELECT COUNT(*) FROM (
				SELECT 1 FROM windowed GROUP BY service_name, schema_name, table_name
			) g`
		if err := r.db.GetContext(ctx, &total, countQuery, search, serviceName, operation, r.tenantID); err != nil {
			r.logger.Error("Failed to count nodes", "search", search, "service", serviceName, "error", err)
			return nil, 0, fmt.Errorf("failed to count nodes: %w", err)
		}
	}

	return out, total, nil
}

// ListNodeNames returns the distinct table names of the tenant's nodes that
// have run, filtered by exact service when serviceName is non-empty, sorted
// ascending. A cheap DISTINCT (no window/aggregation) — it feeds the search
// autocomplete.
func (r *nodeRunRepository) ListNodeNames(ctx context.Context, serviceName string) ([]string, error) {
	const query = `
		SELECT DISTINCT table_name
		FROM tasks
		WHERE tenant_id = $2 AND ($1 = '' OR service_name = $1)
		ORDER BY table_name`
	names := []string{}
	if err := r.db.SelectContext(ctx, &names, query, serviceName, r.tenantID); err != nil {
		r.logger.Error("Failed to list node names", "service", serviceName, "error", err)
		return nil, fmt.Errorf("failed to list node names: %w", err)
	}
	return names, nil
}

// ListNodeServices returns every service name of the tenant's tasks in byte
// order. COLLATE "C" keeps the order independent of the database's locale
// (en_US, for one, ignores '-'), so every install lists services the same way.
func (r *nodeRunRepository) ListNodeServices(ctx context.Context) ([]string, error) {
	const query = `
		SELECT DISTINCT service_name COLLATE "C" AS service_name
		FROM tasks
		WHERE tenant_id = $1
		ORDER BY service_name`
	services := []string{}
	if err := r.db.SelectContext(ctx, &services, query, r.tenantID); err != nil {
		r.logger.Error("Failed to list node services", "error", err)
		return nil, fmt.Errorf("failed to list node services: %w", err)
	}
	return services, nil
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
		Operation:      row.Operation,
		TimeoutSeconds: row.TimeoutSeconds,
	}
}
