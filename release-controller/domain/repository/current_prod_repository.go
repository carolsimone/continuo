package repository

import (
	"context"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// CurrentProdRepository owns the singleton CurrentProd aggregate.
type CurrentProdRepository interface {
	Get(ctx context.Context) (*release.CurrentProd, error)
	Upsert(ctx context.Context, cp *release.CurrentProd) error
}
