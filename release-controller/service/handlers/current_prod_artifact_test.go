package handlers_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func legacyLiveTopology() release.Topology {
	return release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", NodeType: "dbt-model", ContentHash: "h", ImageTag: "t", UpstreamUniqueIDs: []string{}},
		{UniqueID: "test.p.not_null_a.1", ServiceName: "svc-a", NodeType: "dbt-test", UpstreamUniqueIDs: []string{"a"}},
	}
}

// A current_prod that names a release but references no topology artifact gets
// its artifact from the legacy snapshot and is re-announced under the next seq,
// so every consumer learns the seq the live topology carries.
func TestBackfillCurrentProdArtifact_WritesTheArtifactAndReannounces(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	store.SeedLegacyCurrentProd("rLive", legacyLiveTopology())

	require.NoError(t, handlers.BackfillCurrentProdArtifact(context.Background(), deps))

	cp := store.GetCurrentProd()
	assert.Equal(t, "rLive", cp.ReleaseID())
	require.False(t, cp.Topology().IsZero())
	assert.Equal(t, 2, cp.Topology().NodeCount)
	assert.Equal(t, int64(1), cp.PromotionSeq())
	assert.Equal(t, time.Unix(1, 0).UTC(), cp.UpdatedAt().UTC(), "a re-announcement does not move current_prod")

	got, err := deps.Topologies.Load(context.Background(), cp.Topology())
	require.NoError(t, err)
	assert.Equal(t, legacyLiveTopology(), got)

	p := promotedEvent(t, findEntry(t, store, streams.ReleasePromotedV2))
	assert.Equal(t, "rLive", p.ReleaseID)
	assert.Equal(t, int64(1), p.PromotionSeq)
	assert.Equal(t, cp.Topology().URI, p.TopologyURI)
	assert.Equal(t, cp.Topology().SHA256, p.TopologySHA256)
	assert.Empty(t, p.ChangedNodeIDs)
	assert.Empty(t, p.CandidateSchema, "no candidate schema to tear down")
	assert.Empty(t, p.CodeBundleURI, "the versions are already recorded")
	assert.False(t, p.Bootstrap)
}

func TestBackfillCurrentProdArtifact_RunsOnce(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	store.SeedLegacyCurrentProd("rLive", legacyLiveTopology())

	require.NoError(t, handlers.BackfillCurrentProdArtifact(context.Background(), deps))
	require.NoError(t, handlers.BackfillCurrentProdArtifact(context.Background(), deps))

	assert.Len(t, outboxEntries(store), 1)
	assert.Equal(t, int64(1), store.LastPromotionSeq())
}

func TestBackfillCurrentProdArtifact_NothingToDo(t *testing.T) {
	t.Run("fresh install", func(t *testing.T) {
		deps, store := newDeps(time.Unix(100, 0).UTC())
		require.NoError(t, handlers.BackfillCurrentProdArtifact(context.Background(), deps))
		assert.Empty(t, outboxEntries(store))
		assert.Equal(t, int64(0), store.LastPromotionSeq())
	})
	t.Run("current_prod already references an artifact", func(t *testing.T) {
		deps, store := newDeps(time.Unix(100, 0).UTC())
		seedProd(t, deps, store, "rLive", legacyLiveTopology(), time.Unix(1, 0).UTC())
		require.NoError(t, handlers.BackfillCurrentProdArtifact(context.Background(), deps))
		assert.Empty(t, outboxEntries(store))
		assert.Equal(t, int64(0), store.LastPromotionSeq())
	})
}

// failingWrites is an artifact store whose writes fail, as S3 does while it is
// unreachable.
type failingWrites struct {
	ports.TopologyArtifactStore
	err error
}

func (f failingWrites) Write(context.Context, string, release.Topology) (release.TopologyRef, error) {
	return release.TopologyRef{}, f.err
}

// A failed artifact write leaves current_prod and the sequence untouched, so
// the next start retries from the same state.
func TestBackfillCurrentProdArtifact_WriteFailureChangesNothing(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	store.SeedLegacyCurrentProd("rLive", legacyLiveTopology())
	unreachable := errors.New("connection refused")
	deps.Topologies = failingWrites{TopologyArtifactStore: deps.Topologies, err: unreachable}

	err := handlers.BackfillCurrentProdArtifact(context.Background(), deps)
	require.ErrorIs(t, err, unreachable)
	assert.True(t, store.GetCurrentProd().Topology().IsZero())
	assert.Equal(t, int64(0), store.LastPromotionSeq())
	assert.Empty(t, outboxEntries(store))
}
