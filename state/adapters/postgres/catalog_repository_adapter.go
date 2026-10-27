package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/aggregate/catalog"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/jmoiron/sqlx"
)

// CatalogRepositoryAdapter implements repository.ScheduleCatalogRepository by
// wrapping the lower-level ScheduleCatalogRepository. SaveCatalog runs
// UpsertAllTx and SoftDeleteAbsentTx inside the caller's transaction so the
// catalog write is atomic with any surrounding domain logic.
type CatalogRepositoryAdapter struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	repo   ScheduleCatalogRepository
	logger *slog.Logger
}

// NewCatalogRepositoryAdapter constructs a CatalogRepositoryAdapter bound to
// tx. Reads use db; SaveCatalog runs inside tx (which may be nil outside a
// transaction, in which case SaveCatalog returns an error).
func NewCatalogRepositoryAdapter(db *sqlx.DB, tx *sqlx.Tx, repo ScheduleCatalogRepository, logger *slog.Logger) *CatalogRepositoryAdapter {
	return &CatalogRepositoryAdapter{db: db, tx: tx, repo: repo, logger: logger}
}

// GetCatalog loads the tenant's full schedule catalog (active and removed
// rows) and the promotion seq it last applied into a ScheduleCatalog aggregate
// without acquiring any row locks.
func (a *CatalogRepositoryAdapter) GetCatalog(ctx context.Context, tenantID string) (*catalog.ScheduleCatalog, error) {
	rows, err := a.repo.ListAll(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	seq, err := catalogPromotionSeq(ctx, a.db, tenantID, false)
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
func (a *CatalogRepositoryAdapter) LoadCatalogForUpdate(ctx context.Context, tenantID string) (*catalog.ScheduleCatalog, error) {
	if a.tx == nil {
		return nil, fmt.Errorf("LoadCatalogForUpdate requires an active transaction")
	}
	if _, err := a.tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, scheduleCatalogReconcileLockKey); err != nil {
		return nil, fmt.Errorf("acquire schedule_catalog reconcile lock: %w", err)
	}
	rows, err := a.repo.ListAllForUpdateTx(ctx, a.tx, tenantID)
	if err != nil {
		return nil, err
	}
	seq, err := catalogPromotionSeq(ctx, a.tx, tenantID, true)
	if err != nil {
		return nil, err
	}
	return hydrateCatalog(tenantID, rows, seq), nil
}

// SaveCatalog persists the full active set of the catalog under the catalog's
// tenant inside the caller's transaction: every active entry is upserted with
// its per-schedule service metadata, any row of the tenant not in that active
// set is soft-deleted, and the tenant's schedule_catalog_state row records the
// catalog's promotion seq.
func (a *CatalogRepositoryAdapter) SaveCatalog(ctx context.Context, c *catalog.ScheduleCatalog) error {
	if a.tx == nil {
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
	if err := a.repo.UpsertAllTx(ctx, a.tx, c.TenantID(), present, perScheduleMeta); err != nil {
		return err
	}
	if err := a.repo.SoftDeleteAbsentTx(ctx, a.tx, c.TenantID(), present); err != nil {
		return err
	}
	if _, err := a.tx.ExecContext(ctx, `
		INSERT INTO schedule_catalog_state (tenant_id, promotion_seq, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (tenant_id) DO UPDATE SET promotion_seq = EXCLUDED.promotion_seq, updated_at = EXCLUDED.updated_at`,
		c.TenantID(), c.PromotionSeq(),
	); err != nil {
		return fmt.Errorf("save schedule_catalog_state: %w", err)
	}
	return nil
}

// ExistsActive returns true when the tenant's named schedule has an active row.
func (a *CatalogRepositoryAdapter) ExistsActive(ctx context.Context, tenantID, name string) (bool, error) {
	return a.repo.ExistsActive(ctx, tenantID, name)
}

// ListActive returns all of the tenant's schedule names that are currently
// active.
func (a *CatalogRepositoryAdapter) ListActive(ctx context.Context, tenantID string) ([]string, error) {
	return a.repo.ListActive(ctx, tenantID)
}

// GetServiceMetadata returns the service_metadata snapshot for a single
// schedule of the tenant, translating from the postgres model type to the run
// domain type.
func (a *CatalogRepositoryAdapter) GetServiceMetadata(ctx context.Context, tenantID, name string) (map[string]run.ServiceMetadata, error) {
	raw, err := a.repo.GetServiceMetadata(ctx, tenantID, name)
	if err != nil {
		return nil, err
	}
	out := make(map[string]run.ServiceMetadata, len(raw))
	for svc, m := range raw {
		out[svc] = run.ServiceMetadata{ImageTag: m.ImageTag}
	}
	return out, nil
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

var _ repository.ScheduleCatalogRepository = (*CatalogRepositoryAdapter)(nil)
