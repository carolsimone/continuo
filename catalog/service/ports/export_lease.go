package ports

import (
	"context"
	"errors"
	"time"
)

// LeaseKind says what a tenant's export lease is held for.
type LeaseKind string

const (
	// LeaseExport is held while catalog exports a tenant's graph.
	LeaseExport LeaseKind = "export"
	// LeaseRestore is held while catalog restores a tenant's graph.
	LeaseRestore LeaseKind = "restore"
)

// ErrLeaseHeld means another export or restore holds the tenant's lease.
var ErrLeaseHeld = errors.New("another export or restore holds the lease")

// ErrLeaseLost means the caller no longer holds the tenant's lease: another
// holder owns it, or it expired.
var ErrLeaseLost = errors.New("the lease is no longer held by this holder")

// ExportLease is the per-tenant lease an export or a restore holds. While it
// is held (its until lies ahead on the database clock) every catalog write of
// the tenant backs off, so the tenant's graph does not change under the
// export or restore. A lease nobody renews expires, so a holder that crashed
// pauses the tenant's writes for at most one ttl.
type ExportLease interface {
	// Acquire takes the tenant's lease for holder for ttl. It waits for the
	// tenant's in-flight writes to commit, and returns ErrLeaseHeld while
	// another live lease exists, whoever holds it.
	Acquire(ctx context.Context, tenantID string, kind LeaseKind, holder string, ttl time.Duration) error
	// Renew extends holder's live lease to ttl from now. It returns
	// ErrLeaseLost when holder is not the owner of a live lease.
	Renew(ctx context.Context, tenantID, holder string, ttl time.Duration) error
	// Release ends holder's lease. A lease another holder owns is left as it
	// is.
	Release(ctx context.Context, tenantID, holder string) error
}
