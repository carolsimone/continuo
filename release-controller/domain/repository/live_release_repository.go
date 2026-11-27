package repository

import (
	"context"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// LiveReleaseRepository owns the install's one LiveRelease.
type LiveReleaseRepository interface {
	Get(ctx context.Context) (*release.LiveRelease, error)
	Upsert(ctx context.Context, live *release.LiveRelease) error
}
