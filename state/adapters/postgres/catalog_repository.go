package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/catalog"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/jmoiron/sqlx"
)

// ScheduleCatalogRow is a single row from schedule_catalog, including rows
// that have been soft-deleted (RemovedAt non-nil). Used to
// hydrate the ScheduleCatalog aggregate.
type ScheduleCatalogRow struct {
	ScheduleName    string
	RemovedAt       *time.Time
	ServiceMetadata map[string]run.ServiceMetadata
}

// CatalogRepository implements repository.ScheduleCatalogRepository over the
// schedule_catalog and schedule_catalog_state tables. Every method reads or
// writes the rows of one tenant, the catalog of that tenant. Reads use the
// autocommit db; SaveCatalog and LoadCatalogForUpdate run inside the
// transaction bound by WithTx, and return an error when none is bound.
type CatalogRepository struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	logger *slog.Logger
}

// NewCatalogRepository creates a CatalogRepository over db, bound to no
// transaction.
func NewCatalogRepository(db *sqlx.DB, logger *slog.Logger) *CatalogRepository {
	return &CatalogRepository{db: db, logger: logger}
}

// WithTx returns a CatalogRepository over the same db that runs its
// transactional methods in tx.
func (r *CatalogRepository) WithTx(tx *sqlx.Tx) *CatalogRepository {
	return &CatalogRepository{db: r.db, tx: tx, logger: r.logger}
}

// ListActive returns all of the tenant's schedule names that are currently
// active (removed_at IS NULL).
func (r *CatalogRepository) ListActive(ctx context.Context, tenantID string) ([]string, error) {
	var names []string
	err := r.db.SelectContext(ctx, &names,
		`SELECT schedule_name FROM schedule_catalog WHERE tenant_id = $1 AND removed_at IS NULL ORDER BY schedule_name`,
		tenantID,
	)
	if err != nil {
		return nil, fmt.Errorf("list active schedules: %w", err)
	}
	return names, nil
}

// ExistsActive returns true when the tenant's named schedule has an active row.
func (r *CatalogRepository) ExistsActive(ctx context.Context, tenantID, scheduleName string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM schedule_catalog WHERE tenant_id = $2 AND schedule_name = $1 AND removed_at IS NULL)`,
		scheduleName, tenantID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("exists active schedule %q: %w", scheduleName, err)
	}
	return exists, nil
}

// GetServiceMetadata returns the service_metadata snapshot for a single
// schedule of the tenant.
func (r *CatalogRepository) GetServiceMetadata(ctx context.Context, tenantID, scheduleName string) (map[string]run.ServiceMetadata, error) {
	var raw []byte
	err := r.db.QueryRowContext(ctx,
		`SELECT service_metadata FROM schedule_catalog WHERE tenant_id = $2 AND schedule_name = $1`,
		scheduleName, tenantID,
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

// UpsertAllTx inserts or reactivates every name in names within tx. On
// conflict it sets last_seen_at=now() and removed_at=NULL. Each schedule is
// upserted with its own per-schedule service metadata from the serviceMetadata
// map (keyed by schedule_name; each inner map holds service_name to
// ServiceMetadata for that schedule).
func (r *CatalogRepository) UpsertAllTx(ctx context.Context, tx *sqlx.Tx, tenantID string, names []string, serviceMetadata map[string]map[string]run.ServiceMetadata) error {
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
			INSERT INTO schedule_catalog (tenant_id, schedule_name, first_seen_at, last_seen_at, removed_at, service_metadata)
			VALUES ($3, $1, NOW(), NOW(), NULL, $2)
			ON CONFLICT (tenant_id, schedule_name) DO UPDATE
			  SET last_seen_at = NOW(),
			      removed_at = NULL,
			      service_metadata = $2
		`, name, metaJSON, tenantID)
		if err != nil {
			return fmt.Errorf("upsert schedule_catalog %q: %w", name, err)
		}
	}
	return nil
}

// SoftDeleteAbsentTx soft-deletes, within tx, every active row of the tenant
// whose name is not in names.
func (r *CatalogRepository) SoftDeleteAbsentTx(ctx context.Context, tx *sqlx.Tx, tenantID string, names []string) error {
	// removed_at uses the DB clock (NOW()) so the soft-delete stamp shares the
	// same time authority as the rest of the reconcile cycle.
	if len(names) == 0 {
		_, err := tx.ExecContext(ctx,
			`UPDATE schedule_catalog SET removed_at = NOW() WHERE tenant_id = $1 AND removed_at IS NULL`,
			tenantID,
		)
		if err != nil {
			return fmt.Errorf("soft-delete all active schedules: %w", err)
		}
		return nil
	}
	query, args, err := sqlx.In(
		`UPDATE schedule_catalog SET removed_at = NOW() WHERE tenant_id = ? AND removed_at IS NULL AND schedule_name NOT IN (?)`,
		tenantID, names,
	)
	if err != nil {
		return fmt.Errorf("build SoftDeleteAbsentTx query: %w", err)
	}
	query = r.db.Rebind(query)
	_, err = tx.ExecContext(ctx, query, args...)
	return err
}

// listAll returns every row of the tenant's catalog, including soft-deleted
// rows. Used to hydrate the ScheduleCatalog aggregate.
func (r *CatalogRepository) listAll(ctx context.Context, tenantID string) ([]ScheduleCatalogRow, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT schedule_name, removed_at, service_metadata FROM schedule_catalog WHERE tenant_id = $1 ORDER BY schedule_name`,
		tenantID,
	)
	if err != nil {
		return nil, fmt.Errorf("list all schedule_catalog rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanScheduleCatalogRows(rows)
}

// listAllForUpdate returns every row of the tenant's catalog within tx,
// locking the scanned rows with FOR UPDATE so a reconcile cycle's
// read-modify-write is serialised against any concurrent reconciler. Like
// listAll it includes soft-deleted rows.
func (r *CatalogRepository) listAllForUpdate(ctx context.Context, tx *sqlx.Tx, tenantID string) ([]ScheduleCatalogRow, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT schedule_name, removed_at, service_metadata FROM schedule_catalog WHERE tenant_id = $1 ORDER BY schedule_name FOR UPDATE`,
		tenantID,
	)
	if err != nil {
		return nil, fmt.Errorf("list all schedule_catalog rows for update: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanScheduleCatalogRows(rows)
}

// GetCatalog loads the tenant's full schedule catalog (active and removed
// rows) and the promotion seq it last applied into a ScheduleCatalog aggregate
// without acquiring any row locks.
func (r *CatalogRepository) GetCatalog(ctx context.Context, tenantID string) (*catalog.ScheduleCatalog, error) {
	rows, err := r.listAll(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	seq, err := catalogPromotionSeq(ctx, r.db, tenantID, false)
	if err != nil {
		return nil, err
	}
	return hydrateCatalog(tenantID, rows, seq), nil
}

// scheduleCatalogReconcileLockKey is the fixed advisory-lock key that
// serialises schedule_catalog reconcile cycles. It is an arbitrary constant
// scoped to this one reconcile domain; no other advisory lock uses it.
const scheduleCatalogReconcileLockKey int64 = 0x5343_4154 // "SCAT"

// LoadCatalogForUpdate loads the tenant's full schedule catalog for a
// reconcile cycle under a lock that serialises the read-modify-write
// (LoadCatalogForUpdate → mutate → SaveCatalog) against any concurrent
// reconciler, honouring the Load* contract.
//
// It takes a transaction-scoped advisory lock first, then SELECT … FOR UPDATE
// on the existing rows. The advisory lock — not the row locks — is what makes
// this correct on the very first reconcile: an empty schedule_catalog has no
// rows for FOR UPDATE to lock, so without it two replicas could both read the
// empty table and concurrently upsert, leaving the union of two snapshots.
// pg_advisory_xact_lock blocks the second reconciler regardless of row count
// and releases automatically when the transaction ends. The lock is one for
// every tenant, so reconciles of different tenants' catalogs also take turns.
func (r *CatalogRepository) LoadCatalogForUpdate(ctx context.Context, tenantID string) (*catalog.ScheduleCatalog, error) {
	if r.tx == nil {
		return nil, fmt.Errorf("LoadCatalogForUpdate requires an active transaction")
	}
	if _, err := r.tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, scheduleCatalogReconcileLockKey); err != nil {
		return nil, fmt.Errorf("acquire schedule_catalog reconcile lock: %w", err)
	}
	rows, err := r.listAllForUpdate(ctx, r.tx, tenantID)
	if err != nil {
		return nil, err
	}
	seq, err := catalogPromotionSeq(ctx, r.tx, tenantID, true)
	if err != nil {
		return nil, err
	}
	return hydrateCatalog(tenantID, rows, seq), nil
}

// SaveCatalog persists the full active set of the catalog under the catalog's
// tenant inside the bound transaction: every active entry is upserted with its
// per-schedule service metadata, any row of the tenant not in that active set
// is soft-deleted, and the tenant's schedule_catalog_state row records the
// catalog's promotion seq.
func (r *CatalogRepository) SaveCatalog(ctx context.Context, c *catalog.ScheduleCatalog) error {
	if r.tx == nil {
		return fmt.Errorf("SaveCatalog requires an active transaction")
	}
	var present []string
	perScheduleMeta := make(map[string]map[string]run.ServiceMetadata)

	for _, name := range c.Names() {
		e, _ := c.Entry(name)
		if e.IsActive() {
			present = append(present, name)
			inner := make(map[string]run.ServiceMetadata, len(e.ServiceMetadata))
			for svc, m := range e.ServiceMetadata {
				inner[svc] = run.ServiceMetadata{ImageTag: m.ImageTag}
			}
			perScheduleMeta[name] = inner
		}
	}

	if c.TenantID() == "" {
		return fmt.Errorf("save schedule catalog: %w", ErrNoTenant)
	}
	if err := r.UpsertAllTx(ctx, r.tx, c.TenantID(), present, perScheduleMeta); err != nil {
		return err
	}
	if err := r.SoftDeleteAbsentTx(ctx, r.tx, c.TenantID(), present); err != nil {
		return err
	}
	if _, err := r.tx.ExecContext(ctx, `
		INSERT INTO schedule_catalog_state (tenant_id, promotion_seq, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (tenant_id) DO UPDATE SET promotion_seq = EXCLUDED.promotion_seq, updated_at = EXCLUDED.updated_at`,
		c.TenantID(), c.PromotionSeq(),
	); err != nil {
		return fmt.Errorf("save schedule_catalog_state: %w", err)
	}
	return nil
}

// hydrateCatalog constructs the tenant's ScheduleCatalog aggregate from
// persisted rows and the promotion seq the catalog last applied, translating
// from the postgres model type to the catalog domain types.
func hydrateCatalog(tenantID string, rows []ScheduleCatalogRow, promotionSeq int64) *catalog.ScheduleCatalog {
	entries := make(map[string]catalog.Entry, len(rows))
	for _, r := range rows {
		meta := make(map[string]run.ServiceMetadata, len(r.ServiceMetadata))
		for svc, sm := range r.ServiceMetadata {
			meta[svc] = run.ServiceMetadata{ImageTag: sm.ImageTag}
		}
		entries[r.ScheduleName] = catalog.Entry{
			ScheduleName:    r.ScheduleName,
			RemovedAt:       r.RemovedAt,
			ServiceMetadata: meta,
		}
	}
	return catalog.Hydrate(tenantID, entries, promotionSeq)
}

// catalogPromotionSeq reads the promotion seq the tenant's schedule catalog
// last applied, 0 when the tenant's catalog has never applied one. q is the
// reconcile transaction (forUpdate locks the row inside the advisory-locked
// cycle) or the pool for a lock-free read.
func catalogPromotionSeq(ctx context.Context, q sqlx.QueryerContext, tenantID string, forUpdate bool) (int64, error) {
	query := `SELECT promotion_seq FROM schedule_catalog_state WHERE tenant_id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var seq int64
	err := sqlx.GetContext(ctx, q, &seq, query, tenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read schedule_catalog_state: %w", err)
	}
	return seq, nil
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

var _ repository.ScheduleCatalogRepository = (*CatalogRepository)(nil)
