package postgres

import (
	"context"
	"encoding/json"
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

	var meta map[string]run.ServiceMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal service_metadata: %w", err)
	}
	if meta == nil {
		meta = map[string]run.ServiceMetadata{}
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
	now := time.Now()
	for _, name := range names {
		perScheduleMeta := serviceMetadata[name]
		metaJSON, err := json.Marshal(perScheduleMeta)
		if err != nil {
			return fmt.Errorf("marshal service_metadata for %q: %w", name, err)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO schedule_catalog (schedule_name, first_seen_at, last_seen_at, removed_at, service_metadata)
			VALUES ($1, $2, $2, NULL, $3)
			ON CONFLICT (schedule_name) DO UPDATE
			  SET last_seen_at = $2,
			      removed_at = NULL,
			      service_metadata = $3
		`, name, now, metaJSON)
		if err != nil {
			return fmt.Errorf("upsert schedule_catalog %q: %w", name, err)
		}
	}
	return nil
}

// SoftDeleteAbsentTx is the tx-bound variant of SoftDeleteAbsent.
func (r *scheduleCatalogRepository) SoftDeleteAbsentTx(ctx context.Context, tx *sqlx.Tx, names []string) error {
	now := time.Now()
	if len(names) == 0 {
		_, err := tx.ExecContext(ctx,
			`UPDATE schedule_catalog SET removed_at = $1 WHERE removed_at IS NULL`,
			now,
		)
		if err != nil {
			return fmt.Errorf("soft-delete all active schedules: %w", err)
		}
		return nil
	}
	query, args, err := sqlx.In(
		`UPDATE schedule_catalog SET removed_at = ? WHERE removed_at IS NULL AND schedule_name NOT IN (?)`,
		now, names,
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

	var out []ScheduleCatalogRow
	for rows.Next() {
		var name string
		var removedAt *time.Time
		var rawMeta []byte
		if err := rows.Scan(&name, &removedAt, &rawMeta); err != nil {
			return nil, fmt.Errorf("scan schedule_catalog row: %w", err)
		}
		var meta map[string]run.ServiceMetadata
		if len(rawMeta) > 0 {
			if err := json.Unmarshal(rawMeta, &meta); err != nil {
				return nil, fmt.Errorf("unmarshal service_metadata for %q: %w", name, err)
			}
		}
		if meta == nil {
			meta = map[string]run.ServiceMetadata{}
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
