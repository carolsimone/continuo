package ports

import (
	"context"
	"time"
)

// BackupFreshness says how recent the stored backups of the catalog's
// tenants are.
type BackupFreshness interface {
	// OldestNewestBackup returns the oldest of the tenants' newest backups,
	// the time the stored object was last modified. It is the zero time when
	// a tenant has no backup, and known is false when there is no tenant.
	OldestNewestBackup(ctx context.Context) (at time.Time, known bool, err error)
}
