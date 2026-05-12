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

	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/jmoiron/sqlx"
)

// NodeRunRepository reads per-node run history.
type NodeRunRepository interface {
	List(ctx context.Context, serviceName, schemaName, tableName string, limit int) ([]*model.NodeRun, error)
}

type nodeRunRepository struct {
	db     *sqlx.DB
	logger *slog.Logger
}

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
) ([]*model.NodeRun, error) {
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

	rows := []*model.NodeRun{}
	if err := r.db.SelectContext(ctx, &rows, query,
		serviceName, schemaName, tableName, limit); err != nil {
		r.logger.Error("Failed to list node runs",
			"service", serviceName, "schema", schemaName, "table", tableName,
			"error", err)
		return nil, fmt.Errorf("failed to list node runs: %w", err)
	}
	return rows, nil
}
