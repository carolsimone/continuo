package repository

import (
	"context"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// ServiceProdRepository owns the per-service production pointer rows.
// Each row records the manifest S3 key and image tag currently live for one
// dbt service. The full production topology is assembled from these rows at
// release time.
type ServiceProdRepository interface {
	List(ctx context.Context) ([]*release.ServiceProd, error)
	Get(ctx context.Context, serviceName string) (*release.ServiceProd, error)
	Upsert(ctx context.Context, sp *release.ServiceProd) error
}
