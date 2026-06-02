package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// ListCursor is the keyset position for paginating releases newest-first.
type ListCursor struct {
	CreatedAt time.Time
	ReleaseID string
}

// ListFilter parameterises a paginated release history query.
type ListFilter struct {
	Status *string
	Limit  int
	Cursor *ListCursor
}

// ReleaseRepository is the collection-like abstraction over the Release
// aggregate. Concrete implementations live in adapters/postgres.
type ReleaseRepository interface {
	Get(ctx context.Context, id string) (*release.Release, error)
	Save(ctx context.Context, r *release.Release) error
	NextQueuedRelease(ctx context.Context) (*release.Release, error)                                // oldest StatusReceived; nil if none
	ActiveRelease(ctx context.Context) (*release.Release, error)                                    // single Parsing or Validating; nil if none
	List(ctx context.Context, f ListFilter) (items []*release.Release, next *ListCursor, err error) // paginated history newest-first
	DeleteResolvedBefore(ctx context.Context, cutoff time.Time, keepReleaseID string) (int, error)  // retention prune
}
