package repository

import (
	"context"

	"github.com/carolsimone/continuo/state/domain/aggregate/catalog"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
)

// ScheduleCatalogRepository is the port for ScheduleCatalog persistence. Each
// tenant has its own catalog: every read names the tenant it reads, and a save
// writes the catalog under the catalog's own tenant.
type ScheduleCatalogRepository interface {
	GetCatalog(ctx context.Context, tenantID string) (*catalog.ScheduleCatalog, error)
	LoadCatalogForUpdate(ctx context.Context, tenantID string) (*catalog.ScheduleCatalog, error)
	SaveCatalog(ctx context.Context, c *catalog.ScheduleCatalog) error

	// ExistsActive returns true when the tenant's named schedule has an
	// active row. Cross-aggregate query used by SchedulePolicy.
	ExistsActive(ctx context.Context, tenantID, name string) (bool, error)

	// ListActive returns the tenant's active schedule names.
	ListActive(ctx context.Context, tenantID string) ([]string, error)

	// GetServiceMetadata returns the service_metadata snapshot for a single
	// active schedule of the tenant. Used at activation time to pin the run's
	// metadata.
	GetServiceMetadata(ctx context.Context, tenantID, name string) (map[string]run.ServiceMetadata, error)
}
