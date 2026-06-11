package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/jmoiron/sqlx"
)

// ScheduleCatalogRow is a single row from schedule_catalog, including rows
// that have been soft-deleted (RemovedAt non-nil). Used by ListAll for
// hydrating the ScheduleCatalog aggregate.
type ScheduleCatalogRow struct {
	ScheduleName    string
	RemovedAt       *time.Time
	ServiceMetadata map[string]run.ServiceMetadata
}

// ScheduleCatalogRepository manages the schedule_catalog table.
type ScheduleCatalogRepository interface {
	// UpsertAllTx inserts or reactivates all names in the list within the
	// caller's transaction. On conflict: sets last_seen_at=now(),
	// removed_at=NULL. serviceMetadata is keyed by schedule_name; each inner map
	// holds service_name → ServiceMetadata for that schedule.
	UpsertAllTx(ctx context.Context, tx *sqlx.Tx, names []string, serviceMetadata map[string]map[string]run.ServiceMetadata) error
	// SoftDeleteAbsentTx soft-deletes any active row whose name is not in names,
	// within the caller's transaction.
	SoftDeleteAbsentTx(ctx context.Context, tx *sqlx.Tx, names []string) error
	// ListActive returns all schedule_names with removed_at IS NULL.
	ListActive(ctx context.Context) ([]string, error)
	// ExistsActive returns true if schedule_name is active in the catalog.
	ExistsActive(ctx context.Context, scheduleName string) (bool, error)
	GetServiceMetadata(ctx context.Context, scheduleName string) (map[string]run.ServiceMetadata, error)
	// ListAll returns every row in schedule_catalog, including soft-deleted
	// rows, for aggregate hydration.
	ListAll(ctx context.Context) ([]ScheduleCatalogRow, error)
	// ListAllForUpdateTx returns every row in schedule_catalog within the
	// caller's transaction, taking a SELECT ... FOR UPDATE row lock so a
	// reconcile cycle's read-modify-write is serialised against concurrent
	// reconcilers. Like ListAll it includes soft-deleted rows.
	ListAllForUpdateTx(ctx context.Context, tx *sqlx.Tx) ([]ScheduleCatalogRow, error)
}

type scheduleCatalogRepository struct {
	db     *sqlx.DB
	logger *slog.Logger
}

// NewScheduleCatalogRepository creates a new ScheduleCatalogRepository.
func NewScheduleCatalogRepository(db *sqlx.DB, logger *slog.Logger) ScheduleCatalogRepository {
	return &scheduleCatalogRepository{db: db, logger: logger}
}

func (r *scheduleCatalogRepository) ListActive(ctx context.Context) ([]string, error) {
	var names []string
	err := r.db.SelectContext(ctx, &names,
		`SELECT schedule_name FROM schedule_catalog WHERE removed_at IS NULL ORDER BY schedule_name`,
	)
	if err != nil {
		return nil, fmt.Errorf("list active schedules: %w", err)
	}
	return names, nil
}

func (r *scheduleCatalogRepository) ExistsActive(ctx context.Context, scheduleName string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM schedule_catalog WHERE schedule_name = $1 AND removed_at IS NULL)`,
		scheduleName,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("exists active schedule %q: %w", scheduleName, err)
	}
	return exists, nil
}

func (r *scheduleCatalogRepository) GetServiceMetadata(ctx context.Context, scheduleName string) (map[string]run.ServiceMetadata, error) {
	var raw []byte
	err := r.db.QueryRowContext(ctx,
		`SELECT service_metadata FROM schedule_catalog WHERE schedule_name = $1`,
		scheduleName,
	).Scan(&raw)
	if err != nil {
		return nil, fmt.Errorf("get service_metadata for %q: %w", scheduleName, err)
	}

	meta, err := unmarshalServiceMetadata(raw)
	if err != nil {
		return nil, fmt.Errorf("unmarshal service_metadata: %w", err)
	}
	return meta, nil
}

// UpsertAllTx is the tx-bound variant of UpsertAll. Each schedule in names
// is upserted with its own per-schedule service metadata from the
// serviceMetadata map (keyed by schedule_name).
func (r *scheduleCatalogRepository) UpsertAllTx(ctx context.Context, tx *sqlx.Tx, names []string, serviceMetadata map[string]map[string]run.ServiceMetadata) error {
	if len(names) == 0 {
		return nil
	}
	for _, name := range names {
		perScheduleMeta := serviceMetadata[name]
		metaJSON, err := marshalServiceMetadata(perScheduleMeta)
		if err != nil {
			return fmt.Errorf("marshal service_metadata for %q: %w", name, err)
		}
		// first_seen_at / last_seen_at use the DB clock (NOW()) rather than a
		// Go wall-clock value, keeping persisted catalog timestamps on a single
		// authority and immune to host/DB clock skew.
		_, err = tx.ExecContext(ctx, `
			INSERT INTO schedule_catalog (schedule_name, first_seen_at, last_seen_at, removed_at, service_metadata)
			VALUES ($1, NOW(), NOW(), NULL, $2)
			ON CONFLICT (schedule_name) DO UPDATE
			  SET last_seen_at = NOW(),
			      removed_at = NULL,
			      service_metadata = $2
		`, name, metaJSON)
		if err != nil {
			return fmt.Errorf("upsert schedule_catalog %q: %w", name, err)
		}
	}
	return nil
}

// SoftDeleteAbsentTx is the tx-bound variant of SoftDeleteAbsent.
func (r *scheduleCatalogRepository) SoftDeleteAbsentTx(ctx context.Context, tx *sqlx.Tx, names []string) error {
	// removed_at uses the DB clock (NOW()) so the soft-delete stamp shares the
	// same time authority as the rest of the reconcile cycle.
	if len(names) == 0 {
		_, err := tx.ExecContext(ctx,
			`UPDATE schedule_catalog SET removed_at = NOW() WHERE removed_at IS NULL`,
		)
		if err != nil {
			return fmt.Errorf("soft-delete all active schedules: %w", err)
		}
		return nil
	}
	query, args, err := sqlx.In(
		`UPDATE schedule_catalog SET removed_at = NOW() WHERE removed_at IS NULL AND schedule_name NOT IN (?)`,
		names,
	)
	if err != nil {
		return fmt.Errorf("build SoftDeleteAbsentTx query: %w", err)
	}
	query = r.db.Rebind(query)
	_, err = tx.ExecContext(ctx, query, args...)
	return err
}

// ListAll returns every row in schedule_catalog, including soft-deleted rows.
// Used to hydrate the ScheduleCatalog aggregate.
func (r *scheduleCatalogRepository) ListAll(ctx context.Context) ([]ScheduleCatalogRow, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT schedule_name, removed_at, service_metadata FROM schedule_catalog ORDER BY schedule_name`,
	)
	if err != nil {
		return nil, fmt.Errorf("list all schedule_catalog rows: %w", err)
	}
	defer rows.Close()
	return scanScheduleCatalogRows(rows)
}

// ListAllForUpdateTx returns every schedule_catalog row within tx, locking the
// scanned rows with FOR UPDATE so a reconcile cycle's read-modify-write is
// serialised against any concurrent reconciler.
func (r *scheduleCatalogRepository) ListAllForUpdateTx(ctx context.Context, tx *sqlx.Tx) ([]ScheduleCatalogRow, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT schedule_name, removed_at, service_metadata FROM schedule_catalog ORDER BY schedule_name FOR UPDATE`,
	)
	if err != nil {
		return nil, fmt.Errorf("list all schedule_catalog rows for update: %w", err)
	}
	defer rows.Close()
	return scanScheduleCatalogRows(rows)
}

// scanScheduleCatalogRows materialises a schedule_catalog result set into
// ScheduleCatalogRow values, decoding each service_metadata blob.
func scanScheduleCatalogRows(rows *sql.Rows) ([]ScheduleCatalogRow, error) {
	var out []ScheduleCatalogRow
	for rows.Next() {
		var name string
		var removedAt *time.Time
		var rawMeta []byte
		if err := rows.Scan(&name, &removedAt, &rawMeta); err != nil {
			return nil, fmt.Errorf("scan schedule_catalog row: %w", err)
		}
		meta, err := unmarshalServiceMetadata(rawMeta)
		if err != nil {
			return nil, fmt.Errorf("unmarshal service_metadata for %q: %w", name, err)
		}
		out = append(out, ScheduleCatalogRow{
			ScheduleName:    name,
			RemovedAt:       removedAt,
			ServiceMetadata: meta,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schedule_catalog rows: %w", err)
	}
	return out, nil
}
