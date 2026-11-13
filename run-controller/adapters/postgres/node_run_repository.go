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

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/projection"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// NodeRunRepository reads per-node run history.
type NodeRunRepository interface {
	List(ctx context.Context, serviceName, schemaName, tableName string, operation model.Operation, limit int) ([]*projection.NodeRun, error)
	ListNodes(ctx context.Context, search, serviceName string, operation model.Operation, dormantOnly bool, limit, offset int) ([]*projection.NodeSummary, int, error)
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
// operation is matched as stored (operationWithDefault); the run dimension
// includes full_refresh tasks, since a full refresh materializes the node just
// as a run does.
func (r *nodeRunRepository) List(
	ctx context.Context,
	serviceName, schemaName, tableName string,
	op model.Operation,
	limit int,
) ([]*projection.NodeRun, error) {
	if limit <= 0 || limit > 50 {
		limit = 50
	}
	operation := operationWithDefault(string(op))

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
			       a.failure_class, a.failure_reason, a.seed_preflight_findings
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
		       le.seed_preflight_findings AS seed_preflight_findings,
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
		nr, err := toNodeRun(row)
		if err != nil {
			return nil, fmt.Errorf("failed to list node runs: %w", err)
		}
		out = append(out, nr)
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
	SeedFindings   *string        `db:"seed_preflight_findings"`
	Operation      string         `db:"operation"`
	ContentHash    string         `db:"content_hash"`
	WaitReason     string         `db:"wait_reason"`
	WaitingSince   *time.Time     `db:"waiting_since"`
	WaitingOnRunID *uuid.UUID     `db:"waiting_on_run_id"`
}

// toNodeRun maps a scanned row. A seed task whose latest attempt failed
// unowned_dependents lists the dependents the attempt's preflight found and did
// not allow as its blocking objects.
func toNodeRun(row nodeRunRow) (*projection.NodeRun, error) {
	var blocking []string
	if row.FailureReason != nil && *row.FailureReason == string(model.FailureReasonUnownedDependents) {
		findings, err := seedPreflightFindingsFromColumn(row.SeedFindings)
		if err != nil {
			return nil, fmt.Errorf("task %s: %w", row.TaskID, err)
		}
		if findings != nil {
			for _, d := range findings.Outside() {
				blocking = append(blocking, d.Object())
			}
		}
	}
	return &projection.NodeRun{
		ScheduleID:      row.ScheduleID,
		ScheduleName:    row.ScheduleName,
		Kind:            row.Kind,
		TerminalStatus:  row.TerminalStatus,
		TaskID:          row.TaskID,
		TaskStatus:      row.TaskStatus,
		Attempt:         row.Attempt,
		ImageTag:        row.ImageTag,
		CreatedAt:       row.CreatedAt,
		StartedAt:       row.StartedAt,
		CompletedAt:     row.CompletedAt,
		ErrorMessage:    row.ErrorMessage,
		LogS3Key:        row.LogS3Key,
		RunResultsURI:   row.RunResultsURI,
		FailureClass:    row.FailureClass,
		FailureReason:   row.FailureReason,
		Operation:       row.Operation,
		ContentHash:     row.ContentHash,
		WaitReason:      run.WaitReason(row.WaitReason),
		WaitingSince:    row.WaitingSince,
		WaitingOnRunID:  row.WaitingOnRunID,
		BlockingObjects: blocking,
	}, nil
}

// nodeSummaryRow is the adapter-side scan carrier of the node-list query. A
// row with Listed false is the one row the query returns when the page holds
// no node: it carries only TotalCount.
type nodeSummaryRow struct {
	Listed         bool     `db:"listed"`
	ServiceName    string   `db:"service_name"`
	SchemaName     string   `db:"schema_name"`
	TableName      string   `db:"table_name"`
	NodeType       string   `db:"node_type"`
	ScheduleName   string   `db:"schedule_name"`
	RunCount       int      `db:"run_count"`
	TerminalCount  int      `db:"terminal_count"`
	SucceededCount int      `db:"succeeded_count"`
	AvgDur         *float64 `db:"avg_dur"`
	P95Dur         *float64 `db:"p95_dur"`
	FlakyCount     int      `db:"flaky_count"`
	LastStatus     string   `db:"last_status"`
	// LastRunAt is the creation of the run of the node's newest task; nil
	// when the node never ran.
	LastRunAt *time.Time `db:"last_run_at"`
	Operation string     `db:"operation"`
	// TimeoutSeconds is the live release's declared task timeout for the node;
	// nil when it declares none.
	TimeoutSeconds *int `db:"timeout_seconds"`
	// TotalCount is the number of nodes the filters match before paging, the
	// same on every row.
	TotalCount int `db:"total_count"`
}

// listNodesQuery reads one page of tenant $4's node list in one statement, so
// the page and the total are of the same live release. Its parameters:
// $1 the table name, matched case-insensitively and exactly (searching
// "table_2" finds "table_2", never "ftable_2" or "table_2_v2"), empty for
// every table; $2 the service, exact, empty for every service; $3 the
// run/test/build dimension as stored (the run dimension includes
// full_refresh tasks and reports them as 'run'); $5 and $6 the limit and the
// offset; $7 the node types a release builds (builtNodeTypeNames); $8 true to
// keep only nodes of no schedule.
//
//   - nodes: the live release's nodes of the types in $7 that match $1, $2 and
//     $8, one row per service, schema and table (two nodes sharing a table
//     make one row, with the type and schedule of the first node key and the
//     larger declared timeout). The test dimension keeps only the nodes with
//     a known, positive data-test count.
//   - last_runs: each node's newest task of the dimension, by its run's
//     creation; a node that never ran has none. A search names one table, so
//     its nodes' tasks are probed through the node identity; without one, the
//     tenant's tasks (of $2's service when given) are read once and grouped,
//     which costs less than one probe per node of a large list. The
//     condition on $1 leaves exactly one of the two branches to run.
//   - page: the nodes ordered by that time, newest first and never-run nodes
//     last, then by service, schema and table, sliced by $5 and $6.
//   - ranked, windowed, agg: the stats of each page node over its most recent
//     50 tasks of the dimension; a duration comes from a task's latest
//     succeeded attempt only.
//
// The count of nodes is joined to the page, so the statement returns one row
// even when the page is empty (offset past the end, or no node matches):
// that row has no node (listed is false) and still carries the total.
// Every read of tasks keeps the dimension through listedOperation, so the
// last run, the page order and the stats count the same tasks.
const listNodesQuery = `
	WITH nodes AS (
		SELECT n.service_name, n.schema_name, n.table_name,
		       (ARRAY_AGG(n.node_type ORDER BY n.node_key))[1] AS node_type,
		       (ARRAY_AGG(n.schedule_name ORDER BY n.node_key))[1] AS schedule_name,
		       MAX(n.timeout_seconds) AS timeout_seconds
		FROM live_release l
		JOIN topology_nodes n
		  ON n.tenant_id = l.tenant_id AND n.release_id = l.release_id
		WHERE l.tenant_id = $4
		  AND n.node_type = ANY($7::text[])
		  AND ($1 = '' OR lower(n.table_name) = lower($1))
		  AND ($2 = '' OR n.service_name = $2)
		  AND (NOT $8::boolean OR n.schedule_name = '')
		  AND ($3 <> 'test' OR n.test_count > 0)
		GROUP BY n.service_name, n.schema_name, n.table_name
	),
	last_runs AS (
		SELECT t.service_name, t.schema_name, t.table_name, MAX(s.created_at) AS last_run_at
		FROM nodes n
		JOIN tasks t
		  ON t.tenant_id = $4 AND t.service_name = n.service_name
		 AND t.schema_name = n.schema_name AND t.table_name = n.table_name
		JOIN runs s ON s.tenant_id = t.tenant_id AND s.run_id = t.run_id
		WHERE $1 <> ''
		  AND ` + listedOperation + `
		GROUP BY t.service_name, t.schema_name, t.table_name
		UNION ALL
		SELECT t.service_name, t.schema_name, t.table_name, MAX(s.created_at) AS last_run_at
		FROM tasks t
		JOIN runs s ON s.tenant_id = t.tenant_id AND s.run_id = t.run_id
		WHERE $1 = '' AND t.tenant_id = $4
		  AND ($2 = '' OR t.service_name = $2)
		  AND ` + listedOperation + `
		GROUP BY t.service_name, t.schema_name, t.table_name
	),
	page AS (
		SELECT n.service_name, n.schema_name, n.table_name, n.node_type, n.schedule_name,
		       n.timeout_seconds, lr.last_run_at
		FROM nodes n
		LEFT JOIN last_runs lr
		  ON lr.service_name = n.service_name AND lr.schema_name = n.schema_name
		 AND lr.table_name = n.table_name
		ORDER BY lr.last_run_at DESC NULLS LAST, n.service_name, n.schema_name, n.table_name
		LIMIT $5 OFFSET $6
	),
	ranked AS (
		SELECT t.task_id, t.service_name, t.schema_name, t.table_name,
		       t.attempt, t.status AS run_status, s.created_at,
		       CASE WHEN t.operation = 'full_refresh' THEN 'run' ELSE t.operation END AS operation,
		       ROW_NUMBER() OVER (
		         PARTITION BY t.service_name, t.schema_name, t.table_name
		         ORDER BY s.created_at DESC
		       ) AS rn
		FROM page p
		JOIN tasks t
		  ON t.tenant_id = $4 AND t.service_name = p.service_name
		 AND t.schema_name = p.schema_name AND t.table_name = p.table_name
		JOIN runs s ON s.tenant_id = t.tenant_id AND s.run_id = t.run_id
		WHERE ` + listedOperation + `
	),
	windowed AS ( SELECT * FROM ranked WHERE rn <= 50 ),
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
		  w.operation
		FROM windowed w
		-- the task's latest succeeded attempt, probed through task_attempts_pkey;
		-- only a succeeded attempt contributes a duration
		LEFT JOIN LATERAL (
			SELECT a.started_at, a.finished_at
			FROM task_attempts a
			WHERE a.tenant_id = $4 AND a.task_id = w.task_id AND a.status = 'succeeded'
			ORDER BY a.attempt DESC
			LIMIT 1
		) sa ON true
		GROUP BY w.service_name, w.schema_name, w.table_name, w.operation
	)
	SELECT p.service_name IS NOT NULL AS listed,
	       COALESCE(p.service_name, '') AS service_name,
	       COALESCE(p.schema_name, '') AS schema_name,
	       COALESCE(p.table_name, '') AS table_name,
	       COALESCE(p.node_type, '') AS node_type,
	       COALESCE(p.schedule_name, '') AS schedule_name,
	       COALESCE(a.run_count, 0) AS run_count,
	       COALESCE(a.terminal_count, 0) AS terminal_count,
	       COALESCE(a.succeeded_count, 0) AS succeeded_count,
	       a.avg_dur, a.p95_dur,
	       COALESCE(a.flaky_count, 0) AS flaky_count,
	       COALESCE(a.last_status, '') AS last_status,
	       p.last_run_at,
	       COALESCE(a.operation, CASE WHEN $3 = 'full_refresh' THEN 'run' ELSE $3 END) AS operation,
	       p.timeout_seconds,
	       total.total_count
	FROM (SELECT COUNT(*)::int AS total_count FROM nodes) total
	LEFT JOIN page p ON true
	LEFT JOIN agg a
	  ON a.service_name = p.service_name AND a.schema_name = p.schema_name
	 AND a.table_name = p.table_name
	ORDER BY p.last_run_at DESC NULLS LAST, p.service_name, p.schema_name, p.table_name`

// listedOperation keeps the tasks t of listNodesQuery's dimension $3, as
// stored: the run dimension includes full_refresh tasks.
const listedOperation = `(t.operation = $3 OR ($3 = 'run' AND t.operation = 'full_refresh'))`

// ListNodes returns one page of the node list and how many nodes match before
// paging. The list holds every node of the tenant's live release whose type a
// release builds (model.BuiltNodeTypes): a table no node builds, under any of
// its type names, and a dbt test are never listed, and neither is a node the
// live release no longer holds, whatever runs it had. Each row carries the
// node's type, whether it is dormant (model.IsDormant), the timeout the live
// release declares for it and, when it ran, stats over its most recent 50
// tasks of the operation; a node that never ran has no task, no last status
// and no last run. Filters: search, a case-insensitive exact match on the
// table name (when non-empty); serviceName, exact (when non-empty);
// dormantOnly, only the nodes of no schedule; and the run/test/build
// operation (run includes full_refresh; matched as stored, see
// operationWithDefault), whose test dimension lists only the nodes with
// data tests. Rows are ordered by last run, newest first and never-run nodes
// last, with (service, schema, table) tiebreakers for deterministic paging.
func (r *nodeRunRepository) ListNodes(
	ctx context.Context,
	search, serviceName string,
	op model.Operation,
	dormantOnly bool,
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
	operation := operationWithDefault(string(op))

	rows := []nodeSummaryRow{}
	if err := r.db.SelectContext(ctx, &rows, listNodesQuery, search, serviceName, operation, r.tenantID,
		limit, offset, pq.Array(builtNodeTypeNames()), dormantOnly); err != nil {
		r.logger.Error("Failed to list nodes", "search", search, "service", serviceName, "error", err)
		return nil, 0, fmt.Errorf("failed to list nodes: %w", err)
	}

	out := make([]*projection.NodeSummary, 0, len(rows))
	total := 0
	for _, row := range rows {
		total = row.TotalCount
		if row.Listed {
			out = append(out, toNodeSummary(row))
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
	nodeType := model.CanonicalNodeType(row.NodeType)
	return &projection.NodeSummary{
		ServiceName:    row.ServiceName,
		SchemaName:     row.SchemaName,
		TableName:      row.TableName,
		NodeType:       string(nodeType),
		Dormant:        model.IsDormant(nodeType, row.ScheduleName),
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
