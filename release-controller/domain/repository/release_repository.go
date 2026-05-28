package repository

import (
	"context"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// ReleaseRepository is the collection-like abstraction over the Release
// aggregate. Concrete implementations live in adapters/postgres.
type ReleaseRepository interface {
	Get(ctx context.Context, id string) (*release.Release, error)
	Save(ctx context.Context, r *release.Release) error
	NextQueuedRelease(ctx context.Context) (*release.Release, error) // oldest StatusReceived; nil if none
	ActiveRelease(ctx context.Context) (*release.Release, error)     // single Parsing or Validating; nil if none
}
