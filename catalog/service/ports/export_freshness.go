package ports

import (
	"context"
	"time"
)

// ExportFreshness says how recent the stored exports of the catalog's
// tenants are.
type ExportFreshness interface {
	// OldestNewestExport returns the oldest of the tenants' newest exports,
	// the time the stored object was last modified. It is the zero time when
	// a tenant has no export, and known is false when there is no tenant.
	OldestNewestExport(ctx context.Context) (at time.Time, known bool, err error)
}
