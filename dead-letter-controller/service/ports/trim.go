package ports

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/trim"
)

// StreamInspector reads and trims Redis streams on the trimmer's behalf.
type StreamInspector interface {
	// Snapshot returns the consumer state of stream restricted to contractGroups,
	// plus the names of the groups Redis holds that the contract does not list.
	// exists is false when the stream does not exist.
	Snapshot(ctx context.Context, stream string, contractGroups []string) (snap trim.Snapshot, unknown []string, exists bool, err error)
	// NeededEntries returns, in id order and at most limit long, the existing
	// entries below cutoff that group has pending or has not been delivered (id
	// after lastDelivered).
	NeededEntries(ctx context.Context, stream, group string, lastDelivered, cutoff trim.StreamID, limit int) ([]trim.Entry, error)
	// TrimBefore removes the entries of stream with an id below minID and returns
	// how many it removed.
	TrimBefore(ctx context.Context, stream string, minID trim.StreamID) (int64, error)
	// DeleteIfExists deletes stream and reports whether it existed.
	DeleteIfExists(ctx context.Context, stream string) (bool, error)
}

// TrimLock lets one replica run the trim loop at a time.
type TrimLock interface {
	// TryAcquire returns ok=false, without an error, when another process holds
	// the lock. The returned release function must be called once the run ends.
	TryAcquire(ctx context.Context) (release func(), ok bool, err error)
}

// TrimObserver is told what each trim run did, for metrics.
type TrimObserver interface {
	// Quarantined reports n entries newly stored for group on stream.
	Quarantined(stream, group string, n int)
	// Trimmed reports n entries removed from stream.
	Trimmed(stream string, n int64)
	// TrimSucceeded reports a run that completed without error, at time at.
	TrimSucceeded(at time.Time)
}
