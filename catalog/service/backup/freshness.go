package backup

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
)

// Freshness reads how recent the stored backups are.
type Freshness struct {
	tenants ports.GraphTenants
	store   ports.BackupStore
}

var _ ports.BackupFreshness = (*Freshness)(nil)

// NewFreshness returns a Freshness over the backups store holds for the
// tenants tenants lists.
func NewFreshness(tenants ports.GraphTenants, store ports.BackupStore) *Freshness {
	return &Freshness{tenants: tenants, store: store}
}

// OldestNewestBackup returns the oldest of the tenants' newest backups, the
// time the stored object was last modified. It is the zero time when a tenant
// has no backup, and known is false when there is no tenant. It reads the
// store, so it holds every backup, whichever process took it.
func (f *Freshness) OldestNewestBackup(ctx context.Context) (time.Time, bool, error) {
	tenants, err := f.tenants.Tenants(ctx)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("list the tenants: %w", err)
	}
	if len(tenants) == 0 {
		return time.Time{}, false, nil
	}
	var oldest time.Time
	for i, tenantID := range tenants {
		objects, err := f.store.List(ctx, tenantID)
		if err != nil {
			return time.Time{}, false, fmt.Errorf("list the backups of tenant %q: %w", tenantID, err)
		}
		var newest time.Time
		if len(objects) > 0 {
			newest = objects[0].LastModified
		}
		if i == 0 || newest.Before(oldest) {
			oldest = newest
		}
	}
	return oldest, true, nil
}
