package handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPruneResolvedReleases_PassesCutoffAndKeepIDs(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	// Build deps via the shared helper so Clock/Telemetry/Logger are set
	// consistently with other handler tests.
	deps, _ := newDeps(now)

	// Use a dedicated store for this test so we can seed current_prod and
	// inspect the fakeReleaseRepo fields after the call.
	store := newFakeStore()
	store.SeedCurrentProd(release.RehydrateCurrentProd("live-1", release.Topology{}, now))

	// Capture the fakeReleaseRepo that the handler will obtain so we can
	// assert on the recorded cutoff and keepReleaseIDs.
	var captured *fakeReleaseRepo
	deps.NewUoW = func() uow.UnitOfWork {
		u := newFakeUoW(store)
		u.releases.deletedCount = 3
		captured = u.releases
		return u
	}

	// now is supplied via deps' fakeClock (set by newDeps), so the handler
	// derives the cutoff from d.Clock.Now() rather than a caller-supplied time.
	n, err := handlers.PruneResolvedReleases(context.Background(), deps, 90)
	require.NoError(t, err)
	require.NotNil(t, captured, "NewUoW factory was never called")
	assert.Equal(t, 3, n)
	assert.Contains(t, captured.lastKeepReleaseIDs, "live-1")
	assert.True(t, captured.lastCutoff.Equal(now.AddDate(0, 0, -90)))
}

func TestPruneResolvedReleases_IncludesServiceProdReleaseIDs(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	deps, _ := newDeps(now)

	store := newFakeStore()
	// current_prod points at cp-1
	store.SeedCurrentProd(release.RehydrateCurrentProd("cp-1", release.Topology{}, now))
	// service_prod rows point at different releases for each service
	store.SeedServiceProd(release.NewServiceProd("svc-a", "sp-a", "s3://a", "t1", now))
	store.SeedServiceProd(release.NewServiceProd("svc-b", "sp-b", "s3://b", "t2", now))

	var captured *fakeReleaseRepo
	deps.NewUoW = func() uow.UnitOfWork {
		u := newFakeUoW(store)
		u.releases.deletedCount = 0
		captured = u.releases
		return u
	}

	_, err := handlers.PruneResolvedReleases(context.Background(), deps, 90)
	require.NoError(t, err)
	require.NotNil(t, captured)

	// All three release IDs — current_prod and both service_prod refs — must be kept.
	assert.ElementsMatch(t, []string{"cp-1", "sp-a", "sp-b"}, captured.lastKeepReleaseIDs)
}

func TestPruneResolvedReleases_EmptyKeepSetWhenNoProdPointers(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	deps, _ := newDeps(now)

	// No current_prod, no service_prod rows.
	store := newFakeStore()

	var captured *fakeReleaseRepo
	deps.NewUoW = func() uow.UnitOfWork {
		u := newFakeUoW(store)
		captured = u.releases
		return u
	}

	_, err := handlers.PruneResolvedReleases(context.Background(), deps, 90)
	require.NoError(t, err)
	require.NotNil(t, captured)

	// With no pointers, keepReleaseIDs should be empty — prune everything old.
	assert.Empty(t, captured.lastKeepReleaseIDs)
}

func TestPruneResolvedReleases_DeduplicatesOverlappingIDs(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	deps, _ := newDeps(now)

	store := newFakeStore()
	// current_prod and a service_prod row both reference the same release.
	store.SeedCurrentProd(release.RehydrateCurrentProd("shared-1", release.Topology{}, now))
	store.SeedServiceProd(release.NewServiceProd("svc-a", "shared-1", "s3://a", "t1", now))

	var captured *fakeReleaseRepo
	deps.NewUoW = func() uow.UnitOfWork {
		u := newFakeUoW(store)
		captured = u.releases
		return u
	}

	_, err := handlers.PruneResolvedReleases(context.Background(), deps, 90)
	require.NoError(t, err)
	require.NotNil(t, captured)

	// Deduplicated: only one entry for shared-1.
	assert.Equal(t, []string{"shared-1"}, captured.lastKeepReleaseIDs)
}
