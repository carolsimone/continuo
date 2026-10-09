package handlers_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func benchTopology() release.Topology {
	return release.Topology{
		{UniqueID: "bench.n1", ServiceName: "bench", NodeType: "dbt-model", UpstreamUniqueIDs: []string{}},
		{UniqueID: "bench.n2", ServiceName: "bench", NodeType: "dbt-model", UpstreamUniqueIDs: []string{"bench.n1"}},
	}
}

// An announcement writes the artifact, takes the next seq and queues
// release.promoted:v2 — and leaves current_prod alone, even when none exists.
func TestAnnounceTopology_AnnouncesWithoutMovingCurrentProd(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())

	first, err := handlers.AnnounceTopology(context.Background(), deps, "bench-1", benchTopology())
	require.NoError(t, err)
	second, err := handlers.AnnounceTopology(context.Background(), deps, "bench-2", benchTopology())
	require.NoError(t, err)

	assert.Equal(t, int64(1), first.PromotionSeq)
	assert.Equal(t, int64(2), second.PromotionSeq)
	assert.Equal(t, "", store.GetCurrentProd().ReleaseID(), "an announcement never moves current_prod")

	got, err := deps.Topologies.Load(context.Background(), release.TopologyRef{URI: first.TopologyURI, SHA256: first.TopologySHA256, NodeCount: 2})
	require.NoError(t, err)
	assert.Equal(t, benchTopology(), got)

	p := promotedEvent(t, lastEntryOn(store, streams.ReleasePromotedV2))
	assert.Equal(t, "bench-2", p.ReleaseID)
	assert.Equal(t, int64(2), p.PromotionSeq)
	assert.Empty(t, p.ChangedNodeIDs)
	assert.Empty(t, p.CandidateSchema, "no drop-schema Job for an announcement")
	assert.Empty(t, p.CodeBundleURI)
	assert.False(t, p.Bootstrap)
}

// Announcing under a release id that names a run or current_prod would
// replace that release's artifact, so it is refused before anything is written.
func TestAnnounceTopology_RefusesATakenReleaseID(t *testing.T) {
	t.Run("a pipeline run", func(t *testing.T) {
		deps, store := newDeps(time.Unix(100, 0).UTC())
		store.SeedRelease(pipeline.NewCandidate("rA", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(1, 0)))
		_, err := handlers.AnnounceTopology(context.Background(), deps, "rA", benchTopology())
		require.ErrorIs(t, err, handlers.ErrReleaseIDTaken)
		assert.Equal(t, int64(0), store.LastPromotionSeq())
		assert.Empty(t, outboxEntries(store))
	})
	t.Run("current_prod", func(t *testing.T) {
		deps, store := newDeps(time.Unix(100, 0).UTC())
		seedProd(t, deps, store, "rLive", benchTopology(), time.Unix(1, 0).UTC())
		_, err := handlers.AnnounceTopology(context.Background(), deps, "rLive", benchTopology())
		require.ErrorIs(t, err, handlers.ErrReleaseIDTaken)
		assert.Empty(t, outboxEntries(store))
	})
}

// A release id becomes part of the artifact's object key, so an announcement
// takes only the ids the public release API accepts: no id can leave its
// prefix, and nothing is written or announced for a refused one.
func TestAnnounceTopology_RefusesAnIDTheReleaseAPIWouldRefuse(t *testing.T) {
	for _, id := range []string{"", "a/b", "../x", " x", "-x", ".x", strings.Repeat("a", 129)} {
		deps, store := newDeps(time.Unix(100, 0).UTC())
		_, err := handlers.AnnounceTopology(context.Background(), deps, id, benchTopology())
		require.ErrorIs(t, err, handlers.ErrInvalidReleaseID, "%q", id)
		assert.Empty(t, outboxEntries(store), "%q", id)
		assert.Equal(t, int64(0), store.LastPromotionSeq(), "%q", id)
	}
	deps, _ := newDeps(time.Unix(100, 0).UTC())
	_, err := handlers.AnnounceTopology(context.Background(), deps, "bench-dag_2000.rep1", benchTopology())
	require.NoError(t, err, "letters, digits, '.', '_' and '-' are accepted")
}

func TestAnnounceTopology_RefusesAnEmptyTopology(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	_, err := handlers.AnnounceTopology(context.Background(), deps, "bench-1", release.Topology{})
	require.Error(t, err)
	assert.Empty(t, outboxEntries(store))
}

// A topology of only dbt-test nodes survives the len(topo)==0 guard but
// orchestrator strips every dbt-test when it applies the promotion, leaving an
// empty swap set that retires every live node. Announcing it is refused, and a
// topology with at least one non-test node is accepted.
func TestAnnounceTopology_RefusesAnAllDbtTestTopology(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	onlyTests := release.Topology{
		{UniqueID: "test.p.not_null_a.1", ServiceName: "svc", NodeType: "dbt-test", UpstreamUniqueIDs: []string{"a"}},
	}
	_, err := handlers.AnnounceTopology(context.Background(), deps, "bench-1", onlyTests)
	require.Error(t, err)
	assert.Empty(t, outboxEntries(store))
	assert.Equal(t, int64(0), store.LastPromotionSeq())

	_, err = handlers.AnnounceTopology(context.Background(), deps, "bench-2", release.Topology{
		{UniqueID: "a", ServiceName: "svc", NodeType: "dbt-model", UpstreamUniqueIDs: []string{}},
		{UniqueID: "test.p.not_null_a.1", ServiceName: "svc", NodeType: "dbt-test", UpstreamUniqueIDs: []string{"a"}},
	})
	require.NoError(t, err)
}

func TestReannounceCurrentProd(t *testing.T) {
	t.Run("nothing promoted", func(t *testing.T) {
		deps, store := newDeps(time.Unix(100, 0).UTC())
		_, err := handlers.ReannounceCurrentProd(context.Background(), deps)
		require.ErrorIs(t, err, handlers.ErrNoCurrentProd)
		assert.Empty(t, outboxEntries(store))
	})
	t.Run("re-announces current_prod's artifact under a fresh seq", func(t *testing.T) {
		deps, store := newDeps(time.Unix(100, 0).UTC())
		seedProd(t, deps, store, "rLive", benchTopology(), time.Unix(1, 0).UTC())
		_, err := handlers.AnnounceTopology(context.Background(), deps, "bench-1", benchTopology())
		require.NoError(t, err)

		res, err := handlers.ReannounceCurrentProd(context.Background(), deps)
		require.NoError(t, err)

		cp := store.GetCurrentProd()
		assert.Equal(t, "rLive", res.ReleaseID)
		assert.Equal(t, int64(2), res.PromotionSeq)
		assert.Equal(t, cp.Topology().URI, res.TopologyURI)
		assert.Equal(t, int64(2), cp.PromotionSeq())
		p := promotedEvent(t, lastEntryOn(store, streams.ReleasePromotedV2))
		assert.Equal(t, "rLive", p.ReleaseID)
		assert.Equal(t, int64(2), p.PromotionSeq)
	})
}

func TestCurrentProdTopology(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	_, err := handlers.CurrentProdTopology(context.Background(), deps)
	require.ErrorIs(t, err, handlers.ErrNoCurrentProd)

	seedProd(t, deps, store, "rLive", benchTopology(), time.Unix(1, 0).UTC())
	got, err := handlers.CurrentProdTopology(context.Background(), deps)
	require.NoError(t, err)
	assert.Equal(t, benchTopology(), got)
}
