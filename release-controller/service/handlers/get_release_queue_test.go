package handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetReleaseQueue_NothingActive(t *testing.T) {
	deps, _ := newDeps(time.Unix(1_700_000_000, 0).UTC())
	got, err := handlers.GetReleaseQueue(context.Background(), deps)
	require.NoError(t, err)
	assert.Nil(t, got.Active)
}

func TestGetReleaseQueue_ReportsTheActiveFix(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, store := newDeps(now)
	v := releasecheck.NewFix("verify-rel-1-core-a2", "core", "img", "rel-1", 2, "", release.ManifestKindDbt, now)
	require.NoError(t, v.TransitionToCompiling(now.Add(time.Minute)))
	store.SeedRelease(v)
	store.SeedRelease(releasecheck.NewCandidate("rel-2", "core", "img", false, false, "org/r", "sha", release.ManifestKindDbt, now.Add(2*time.Minute)))

	got, err := handlers.GetReleaseQueue(context.Background(), deps)
	require.NoError(t, err)
	require.NotNil(t, got.Active)
	assert.Equal(t, "verify-rel-1-core-a2", got.Active.CheckID)
	assert.Equal(t, releasecheck.KindFix, got.Active.Kind)
	assert.Equal(t, releasecheck.StatusCompiling, got.Active.Status)
	assert.Equal(t, now.Add(time.Minute), got.Active.Since)
	assert.Equal(t, "rel-1", got.Active.VerifiesReleaseID)
	assert.Equal(t, 2, got.Active.Attempt)
}
