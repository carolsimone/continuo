package repository

import (
	"context"

	"github.com/carolsimone/continuo/state/domain/aggregate/catalog"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
)

// ScheduleCatalogRepository is the port for ScheduleCatalog persistence.
type ScheduleCatalogRepository interface {
	GetCatalog(ctx context.Context) (*catalog.ScheduleCatalog, error)
	LoadCatalogForUpdate(ctx context.Context) (*catalog.ScheduleCatalog, error)
	SaveCatalog(ctx context.Context, c *catalog.ScheduleCatalog) error

	// ExistsActive returns true when the named schedule has an active row.
	// Cross-aggregate query used by SchedulePolicy.
	ExistsActive(ctx context.Context, name string) (bool, error)

	// ListActive returns the active schedule names.
	ListActive(ctx context.Context) ([]string, error)

	// GetServiceMetadata returns the service_metadata snapshot for a single
	// active schedule. Used at activation time to pin the run's metadata.
	GetServiceMetadata(ctx context.Context, name string) (map[string]run.ServiceMetadata, error)
}
