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

func TestPruneFinishedChecks_PassesCutoffAndKeepIDs(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	// Build deps via the shared helper so Clock/Telemetry/Logger are set
	// consistently with other handler tests.
	deps, _ := newDeps(now)

	// Use a dedicated store for this test so we can seed the live release and
	// inspect the fakeReleaseCheckRepo fields after the call.
	store := newFakeStore()
	store.SeedLiveRelease(release.RehydrateLiveRelease("live-1", release.TopologyRef{}, 0, now))

	// Capture the fakeReleaseCheckRepo that the handler will obtain so we can
	// assert on the recorded cutoff and keepReleaseIDs.
	var captured *fakeReleaseCheckRepo
	deps.NewUoW = func() uow.UnitOfWork {
		u := newFakeUoW(store)
		u.releases.deletedCount = 3
		captured = u.releases
		return u
	}

	// now is supplied via deps' fakeClock (set by newDeps), so the handler
	// derives the cutoff from d.Clock.Now() rather than a caller-supplied time.
	n, err := handlers.PruneFinishedChecks(context.Background(), deps, 90)
	require.NoError(t, err)
	require.NotNil(t, captured, "NewUoW factory was never called")
	assert.Equal(t, 3, n)
	assert.Contains(t, captured.lastKeepReleaseIDs, "live-1")
	assert.True(t, captured.lastCutoff.Equal(now.AddDate(0, 0, -90)))
}

func TestPruneFinishedChecks_IncludesProductionPointerReleaseIDs(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	deps, _ := newDeps(now)

	store := newFakeStore()
	// the live release is rel-live
	store.SeedLiveRelease(release.RehydrateLiveRelease("rel-live", release.TopologyRef{}, 0, now))
	// production pointers point at different releases for each project
	store.SeedProductionPointer(release.NewProductionPointer("svc-a", "rel-a", "s3://a", "t1", release.ManifestKindDbt, now))
	store.SeedProductionPointer(release.NewProductionPointer("svc-b", "rel-b", "s3://b", "t2", release.ManifestKindDbt, now))

	var captured *fakeReleaseCheckRepo
	deps.NewUoW = func() uow.UnitOfWork {
		u := newFakeUoW(store)
		u.releases.deletedCount = 0
		captured = u.releases
		return u
	}

	_, err := handlers.PruneFinishedChecks(context.Background(), deps, 90)
	require.NoError(t, err)
	require.NotNil(t, captured)

	// All three release IDs — the live release and the release each production pointer names — must be kept.
	assert.ElementsMatch(t, []string{"rel-live", "rel-a", "rel-b"}, captured.lastKeepReleaseIDs)
}

func TestPruneFinishedChecks_EmptyKeepSetWithNoLiveReleaseOrProductionPointers(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	deps, _ := newDeps(now)

	// No live release, no production pointers.
	store := newFakeStore()

	var captured *fakeReleaseCheckRepo
	deps.NewUoW = func() uow.UnitOfWork {
		u := newFakeUoW(store)
		captured = u.releases
		return u
	}

	_, err := handlers.PruneFinishedChecks(context.Background(), deps, 90)
	require.NoError(t, err)
	require.NotNil(t, captured)

	// With no pointers, keepReleaseIDs should be empty — prune everything old.
	assert.Empty(t, captured.lastKeepReleaseIDs)
}

func TestPruneFinishedChecks_DeduplicatesOverlappingIDs(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	deps, _ := newDeps(now)

	store := newFakeStore()
	// The live release and a production pointer name the same release.
	store.SeedLiveRelease(release.RehydrateLiveRelease("shared-1", release.TopologyRef{}, 0, now))
	store.SeedProductionPointer(release.NewProductionPointer("svc-a", "shared-1", "s3://a", "t1", release.ManifestKindDbt, now))

	var captured *fakeReleaseCheckRepo
	deps.NewUoW = func() uow.UnitOfWork {
		u := newFakeUoW(store)
		captured = u.releases
		return u
	}

	_, err := handlers.PruneFinishedChecks(context.Background(), deps, 90)
	require.NoError(t, err)
	require.NotNil(t, captured)

	// Deduplicated: only one entry for shared-1.
	assert.Equal(t, []string{"shared-1"}, captured.lastKeepReleaseIDs)
}
