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

// LoadCatalogForUpdate loads the full schedule_catalog for a reconcile cycle.
// schedule_catalog reconciliation is driven by a single consumer of
// schedules.loaded:v1, so a plain ListAll suffices. If contention increases,
// add a ListAllForUpdate (SELECT … FOR UPDATE) to the underlying repository.
func (a *CatalogRepositoryAdapter) LoadCatalogForUpdate(ctx context.Context) (*catalog.ScheduleCatalog, error) {
	return a.GetCatalog(ctx)
}

// SaveCatalog persists the catalog's changeset inside the caller's
// transaction: active entries are upserted with per-schedule service metadata,
// and entries that are no longer active are soft-deleted. ResetChanges is
// called after a successful write.
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
	c.ResetChanges()
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
