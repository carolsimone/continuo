package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/seeds"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// MaterializationRepository implements repository.MaterializationRepository
// over node_materializations. Statements run in the unit of work's
// transaction when one is open.
type MaterializationRepository struct {
	db *sqlx.DB
	tx *sqlx.Tx
}

// Compile-time assertion that the adapter satisfies the domain port.
var _ repository.MaterializationRepository = (*MaterializationRepository)(nil)

// NewMaterializationRepository binds a MaterializationRepository to tx; a nil
// tx runs every statement on the autocommit db.
func NewMaterializationRepository(db *sqlx.DB, tx *sqlx.Tx) *MaterializationRepository {
	return &MaterializationRepository{db: db, tx: tx}
}

func (r *MaterializationRepository) conn() sqlx.ExtContext {
	if r.tx != nil {
		return r.tx
	}
	return r.db
}

type materializationRow struct {
	NodeKey            string         `db:"node_key"`
	ContentHash        string         `db:"content_hash"`
	AttemptContentHash string         `db:"attempt_content_hash"`
	AttemptRunID       uuid.NullUUID  `db:"attempt_run_id"`
	AttemptRunStatus   sql.NullString `db:"attempt_run_status"`
}

// List reads the tenant's rows with the status of each request's run.
func (r *MaterializationRepository) List(ctx context.Context, tenantID string) (map[topology.NodeKey]seeds.Materialization, error) {
	var rows []materializationRow
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, `
		SELECT m.node_key, m.content_hash, m.attempt_content_hash, m.attempt_run_id, s.status AS attempt_run_status
		FROM node_materializations m
		LEFT JOIN scheduler_tracker s ON s.schedule_id = m.attempt_run_id
		WHERE m.tenant_id = $1`, tenantID); err != nil {
		return nil, fmt.Errorf("read the materializations of tenant %s: %w", tenantID, err)
	}
	out := make(map[topology.NodeKey]seeds.Materialization, len(rows))
	for _, row := range rows {
		key := topology.NodeKey(row.NodeKey)
		out[key] = seeds.Materialization{
			Key:                key,
			ContentHash:        row.ContentHash,
			AttemptContentHash: row.AttemptContentHash,
			AttemptRunID:       row.AttemptRunID.UUID,
			AttemptState:       attemptState(row.AttemptRunID, row.AttemptRunStatus),
		}
	}
	return out, nil
}

// attemptState maps a request's run onto the seed rules: pending or running is
// in flight, succeeded is succeeded, and every other ending is failed. No
// request, or a run that no longer exists, is none.
func attemptState(runID uuid.NullUUID, status sql.NullString) seeds.AttemptState {
	if !runID.Valid || !status.Valid {
		return seeds.AttemptNone
	}
	switch run.SchedulerStatus(status.String) {
	case run.SchedulerStatusSucceeded:
		return seeds.AttemptSucceeded
	case run.SchedulerStatusFailed, run.SchedulerStatusCancelled, run.SchedulerStatusSkipped:
		return seeds.AttemptFailed
	default:
		return seeds.AttemptInFlight
	}
}

// RecordAttempt upserts the request columns of the node's row.
func (r *MaterializationRepository) RecordAttempt(ctx context.Context, tenantID string, key topology.NodeKey, contentHash string, runID uuid.UUID, at time.Time) error {
	if _, err := r.conn().ExecContext(ctx, `
		INSERT INTO node_materializations (tenant_id, node_key, attempt_content_hash, attempt_run_id, attempted_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tenant_id, node_key) DO UPDATE
		SET attempt_content_hash = EXCLUDED.attempt_content_hash,
		    attempt_run_id = EXCLUDED.attempt_run_id,
		    attempted_at = EXCLUDED.attempted_at`,
		tenantID, string(key), contentHash, nullableUUID(runID), at); err != nil {
		return fmt.Errorf("record the build request of %s: %w", key, err)
	}
	return nil
}

// RecordBuilt upserts the version columns of the node's row.
func (r *MaterializationRepository) RecordBuilt(ctx context.Context, tenantID string, b seeds.Built) error {
	if _, err := r.conn().ExecContext(ctx, `
		INSERT INTO node_materializations
		    (tenant_id, node_key, content_hash, image_tag, release_id, run_id, task_id, materialized_at, source)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (tenant_id, node_key) DO UPDATE
		SET content_hash = EXCLUDED.content_hash,
		    image_tag = EXCLUDED.image_tag,
		    release_id = EXCLUDED.release_id,
		    run_id = EXCLUDED.run_id,
		    task_id = EXCLUDED.task_id,
		    materialized_at = EXCLUDED.materialized_at,
		    source = EXCLUDED.source`,
		tenantID, string(b.Key), b.ContentHash, b.ImageTag, b.ReleaseID,
		nullableUUID(b.RunID), nullableUUID(b.TaskID), b.At, string(b.Source)); err != nil {
		return fmt.Errorf("record the loaded version of %s: %w", b.Key, err)
	}
	return nil
}

// DeleteAbsentFrom deletes the tenant's rows whose node the release lacks.
func (r *MaterializationRepository) DeleteAbsentFrom(ctx context.Context, tenantID, releaseID string) (int64, error) {
	res, err := r.conn().ExecContext(ctx, `
		DELETE FROM node_materializations m
		WHERE m.tenant_id = $1
		  AND NOT EXISTS (
		      SELECT 1 FROM topology_nodes n
		      WHERE n.tenant_id = m.tenant_id AND n.release_id = $2 AND n.node_key = m.node_key)`,
		tenantID, releaseID)
	if err != nil {
		return 0, fmt.Errorf("delete materializations absent from release %s: %w", releaseID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("delete materializations absent from release %s: %w", releaseID, err)
	}
	return n, nil
}

// nullableUUID stores uuid.Nil as NULL.
func nullableUUID(id uuid.UUID) uuid.NullUUID {
	return uuid.NullUUID{UUID: id, Valid: id != uuid.Nil}
}
