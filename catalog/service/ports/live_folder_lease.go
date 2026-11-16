package ports

import (
	"context"
	"time"
)

// LiveFolderLease is the per-tenant lease a rewrite of the live contracts
// folder holds, so two rewrites of one tenant never interleave their writes.
// It is not the export lease: holding it pauses no graph write, and an export
// does not wait for it. A lease nobody renews expires, so a holder that
// crashed blocks the folder for at most one ttl.
type LiveFolderLease interface {
	// Acquire takes the tenant's lease for holder for ttl. acquired is false,
	// and nothing is taken, while another live lease exists.
	Acquire(ctx context.Context, tenantID, holder string, ttl time.Duration) (acquired bool, err error)
	// Renew extends holder's live lease to ttl from now. It returns
	// ErrLeaseLost when holder is not the owner of a live lease.
	Renew(ctx context.Context, tenantID, holder string, ttl time.Duration) error
	// Release ends holder's lease. A lease another holder owns is left as it
	// is.
	Release(ctx context.Context, tenantID, holder string) error
}

// LiveFolderState is what the graph records about a tenant's live contracts
// folder: the largest promotion seq a rewrite finished for (the folder's
// watermark) and the fingerprint of the publishing settings it was written
// with.
type LiveFolderState interface {
	// Read returns the tenant's watermark and settings fingerprint: 0 and ""
	// when the folder was never written.
	Read(ctx context.Context, tenantID string) (promotionSeq int64, settingsSHA256 string, err error)
	// RecordSettings stores the fingerprint of the settings a rewrite of the
	// whole folder used, and leaves the watermark as it is.
	RecordSettings(ctx context.Context, tenantID, settingsSHA256 string) error
}
