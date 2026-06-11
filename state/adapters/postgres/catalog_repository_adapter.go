package postgres

import (
	"context"
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

// GetCatalog loads the full schedule_catalog (active and removed rows) into a
// ScheduleCatalog aggregate without acquiring any row locks.
func (a *CatalogRepositoryAdapter) GetCatalog(ctx context.Context) (*catalog.ScheduleCatalog, error) {
	rows, err := a.repo.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	return hydrateCatalog(rows), nil
}

// scheduleCatalogReconcileLockKey is the fixed advisory-lock key that
// serialises schedule_catalog reconcile cycles. It is an arbitrary constant
// scoped to this one reconcile domain; no other advisory lock uses it.
const scheduleCatalogReconcileLockKey int64 = 0x5343_4154 // "SCAT"

// LoadCatalogForUpdate loads the full schedule_catalog for a reconcile cycle
// under a lock that serialises the read-modify-write (LoadCatalogForUpdate →
// mutate → SaveCatalog) against any concurrent reconciler, honouring the Load*
// contract.
//
// It takes a transaction-scoped advisory lock first, then SELECT … FOR UPDATE
// on the existing rows. The advisory lock — not the row locks — is what makes
// this correct on the very first reconcile: an empty schedule_catalog has no
// rows for FOR UPDATE to lock, so without it two replicas could both read the
// empty table and concurrently upsert, leaving the union of two snapshots.
// pg_advisory_xact_lock blocks the second reconciler regardless of row count
// and releases automatically when the transaction ends.
func (a *CatalogRepositoryAdapter) LoadCatalogForUpdate(ctx context.Context) (*catalog.ScheduleCatalog, error) {
	if a.tx == nil {
		return nil, fmt.Errorf("LoadCatalogForUpdate requires an active transaction")
	}
	if _, err := a.tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, scheduleCatalogReconcileLockKey); err != nil {
		return nil, fmt.Errorf("acquire schedule_catalog reconcile lock: %w", err)
	}
	rows, err := a.repo.ListAllForUpdateTx(ctx, a.tx)
	if err != nil {
		return nil, err
	}
	return hydrateCatalog(rows), nil
}

// SaveCatalog persists the full active set of the catalog inside the caller's
// transaction: every active entry is upserted with its per-schedule service
// metadata, and any row not in that active set is soft-deleted.
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
				inner[svc] = run.ServiceMetadata{ManifestVersion: m.ManifestVersion, ImageTag: m.ImageTag}
			}
			perScheduleMeta[name] = inner
		}
	}

	if err := a.repo.UpsertAllTx(ctx, a.tx, present, perScheduleMeta); err != nil {
		return err
	}
	if err := a.repo.SoftDeleteAbsentTx(ctx, a.tx, present); err != nil {
		return err
	}
	return nil
}

// ExistsActive returns true when the named schedule has an active row.
func (a *CatalogRepositoryAdapter) ExistsActive(ctx context.Context, name string) (bool, error) {
	return a.repo.ExistsActive(ctx, name)
}

// ListActive returns all schedule names that are currently active.
func (a *CatalogRepositoryAdapter) ListActive(ctx context.Context) ([]string, error) {
	return a.repo.ListActive(ctx)
}

// GetServiceMetadata returns the service_metadata snapshot for a single
// schedule, translating from the postgres model type to the run domain type.
func (a *CatalogRepositoryAdapter) GetServiceMetadata(ctx context.Context, name string) (map[string]run.ServiceMetadata, error) {
	raw, err := a.repo.GetServiceMetadata(ctx, name)
	if err != nil {
		return nil, err
	}
	out := make(map[string]run.ServiceMetadata, len(raw))
	for svc, m := range raw {
		out[svc] = run.ServiceMetadata{ManifestVersion: m.ManifestVersion, ImageTag: m.ImageTag}
	}
	return out, nil
}

// hydrateCatalog constructs a ScheduleCatalog aggregate from persisted rows,
// translating from the postgres model type to the catalog domain types.
func hydrateCatalog(rows []ScheduleCatalogRow) *catalog.ScheduleCatalog {
	entries := make(map[string]catalog.Entry, len(rows))
	for _, r := range rows {
		meta := make(map[string]run.ServiceMetadata, len(r.ServiceMetadata))
		for svc, sm := range r.ServiceMetadata {
			meta[svc] = run.ServiceMetadata{ManifestVersion: sm.ManifestVersion, ImageTag: sm.ImageTag}
		}
		entries[r.ScheduleName] = catalog.Entry{
			ScheduleName:    r.ScheduleName,
			RemovedAt:       r.RemovedAt,
			ServiceMetadata: meta,
		}
	}
	return catalog.Hydrate(entries)
}

var _ repository.ScheduleCatalogRepository = (*CatalogRepositoryAdapter)(nil)
