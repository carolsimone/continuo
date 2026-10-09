package handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// promoteCandidate drives a candidate of svc-a through parse and an all-ok
// validation, so it promotes over whatever current_prod holds.
func promoteCandidate(t *testing.T, deps *handlers.Deps, releaseID string, topo release.Topology) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, handlers.ReceiveCandidate(ctx, deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: releaseID, ImageTag: "sha-" + releaseID, Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(ctx, deps))
	require.NoError(t, handlers.HandleCompileResult(ctx, deps, handlers.HandleCompileResultInput{ReleaseID: releaseID, Status: "ok"}))
	ref, err := deps.Topologies.Write(ctx, releaseID, topo)
	require.NoError(t, err)
	require.NoError(t, handlers.HandleParsedManifest(ctx, deps, handlers.HandleParsedManifestInput{
		ReleaseID: releaseID, Status: "ok", TopologyRef: ref,
	}))
	results := make([]handlers.NodeResult, 0, len(topo))
	for _, n := range topo {
		results = append(results, handlers.NodeResult{NodeID: n.UniqueID, Status: "ok"})
	}
	seedValidationNodes(t, deps, releaseID, results)
	require.NoError(t, handlers.HandleValidationResult(ctx, deps, handlers.HandleValidationResultInput{ReleaseID: releaseID, AggregateStatus: "ok"}))
}

// Each promotion takes the next seq, records it on current_prod and carries it
// on release.promoted:v2 with the run's own topology reference.
func TestPromote_EachPromotionTakesTheNextSeq(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"
	topo := release.Topology{{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h1", UpstreamUniqueIDs: []string{}}}

	promoteCandidate(t, deps, "rA", topo)
	topo[0].ContentHash = "h2"
	promoteCandidate(t, deps, "rB", topo)

	var seqs []int64
	for _, e := range outboxEntries(store) {
		if e.StreamName == streams.ReleasePromotedV2 {
			seqs = append(seqs, promotedEvent(t, e).PromotionSeq)
		}
	}
	assert.Equal(t, []int64{1, 2}, seqs)

	rB, err := store.GetRelease("rB")
	require.NoError(t, err)
	last := promotedEvent(t, lastEntryOn(store, streams.ReleasePromotedV2))
	assert.Equal(t, "rB", last.ReleaseID)
	assert.Equal(t, rB.CandidateTopologyRef().URI, last.TopologyURI)
	assert.Equal(t, rB.CandidateTopologyRef().SHA256, last.TopologySHA256)
	assert.Equal(t, int64(2), store.GetCurrentProd().PromotionSeq())
	assert.Equal(t, rB.CandidateTopologyRef(), store.GetCurrentProd().Topology())
}

// changed_node_ids lists, sorted, the non-test nodes whose content_hash differs
// from or is absent in the production artifact being replaced.
func TestPromote_ChangedNodeIDsAreSortedAndNeverNameATest(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"
	seedProd(t, deps, store, "prev", release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", ContentHash: "same", UpstreamUniqueIDs: []string{}},
		{UniqueID: "c", ServiceName: "svc-a", ContentHash: "old", UpstreamUniqueIDs: []string{}},
	}, time.Unix(50, 0).UTC())

	promoteCandidate(t, deps, "rA", release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", ContentHash: "same", UpstreamUniqueIDs: []string{}},
		{UniqueID: "c", ServiceName: "svc-a", ContentHash: "new", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-a", ContentHash: "new", UpstreamUniqueIDs: []string{}},
		{UniqueID: "test.p.not_null_b.1", ServiceName: "svc-a", NodeType: "dbt-test", ContentHash: "t", UpstreamUniqueIDs: []string{"b"}},
	})

	p := promotedEvent(t, findEntry(t, store, streams.ReleasePromotedV2))
	assert.Equal(t, []string{"b", "c"}, p.ChangedNodeIDs)
	assert.False(t, p.Bootstrap)
}

// A bootstrap promotion onto an empty production lists every non-test node as
// changed, exactly like any other promotion: the diff, not the bootstrap flag,
// decides which seeds are rebuilt.
func TestPromote_BootstrapListsTheExactDiff(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"
	ctx := context.Background()
	require.NoError(t, handlers.ReceiveCandidate(ctx, deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rBoot", ImageTag: "sha-a", Repo: "acme/demo", CommitSHA: "deadbeef", Bootstrap: true,
	}))
	require.NoError(t, handlers.AdvanceQueue(ctx, deps))
	require.NoError(t, handlers.HandleCompileResult(ctx, deps, handlers.HandleCompileResultInput{ReleaseID: "rBoot", Status: "ok"}))
	ref, err := deps.Topologies.Write(ctx, "rBoot", release.Topology{{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}}})
	require.NoError(t, err)
	require.NoError(t, handlers.HandleParsedManifest(ctx, deps, handlers.HandleParsedManifestInput{ReleaseID: "rBoot", Status: "ok", TopologyRef: ref}))

	p := promotedEvent(t, findEntry(t, store, streams.ReleasePromotedV2))
	assert.True(t, p.Bootstrap)
	assert.Equal(t, []string{"a"}, p.ChangedNodeIDs, "no production yet: every non-test node is changed")
	assert.Equal(t, int64(1), p.PromotionSeq)
}

// A verification run passes without touching the promotion sequence.
func TestPromote_VerificationNeverTakesASeq(t *testing.T) {
	deps, store := seedToValidatingVerification(t, "rVerify")
	seedValidationNodes(t, deps, "rVerify", []handlers.NodeResult{{NodeID: "a", Status: "ok"}, {NodeID: "b", Status: "ok"}})
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rVerify", AggregateStatus: "ok",
	}))
	assert.Equal(t, int64(0), store.LastPromotionSeq())
	assert.Nil(t, lastEntryOn(store, streams.ReleasePromotedV2))
}
