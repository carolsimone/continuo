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

func TestPruneResolvedReleases_PassesCutoffAndKeepID(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	// Build deps via the shared helper so Clock/Telemetry/Logger are set
	// consistently with other handler tests.
	deps, _ := newDeps(now)

	// Use a dedicated store for this test so we can seed current_prod and
	// inspect the fakeReleaseRepo fields after the call.
	store := newFakeStore()
	store.SeedCurrentProd(release.RehydrateCurrentProd("live-1", "s3://bucket/live-1/", release.Topology{}, now))

	// Capture the fakeReleaseRepo that the handler will obtain so we can
	// assert on the recorded cutoff and keepReleaseID.
	var captured *fakeReleaseRepo
	deps.NewUoW = func() uow.UnitOfWork {
		u := newFakeUoW(store)
		u.releases.deletedCount = 3
		captured = u.releases
		return u
	}

	n, err := handlers.PruneResolvedReleases(context.Background(), deps, 90, now)
	require.NoError(t, err)
	require.NotNil(t, captured, "NewUoW factory was never called")
	assert.Equal(t, 3, n)
	assert.Equal(t, "live-1", captured.lastKeepReleaseID)
	assert.True(t, captured.lastCutoff.Equal(now.AddDate(0, 0, -90)))
}
