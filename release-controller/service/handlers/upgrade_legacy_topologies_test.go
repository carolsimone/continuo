package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpgradeLegacyTopologies_MovesInlineTopologiesIntoArtifacts(t *testing.T) {
	deps, store := newTestDeps(t)
	topo := release.Topology{{UniqueID: "a", ServiceName: "svc-a", NodeType: "dbt-model", ImageTag: "img", UpstreamUniqueIDs: []string{}}}
	store.SeedLegacyTopology("rOld", topo)

	require.NoError(t, handlers.UpgradeLegacyTopologies(context.Background(), deps))

	ref, ok := store.LegacyRef("rOld")
	require.True(t, ok, "the inline topology moved into an artifact")
	assert.Equal(t, 1, ref.NodeCount)
	got, err := deps.Topologies.Load(context.Background(), ref)
	require.NoError(t, err)
	assert.Equal(t, topo, got)

	// A second start finds nothing left to move.
	store.topologies.failWrites(errors.New("nothing must be written twice"))
	require.NoError(t, handlers.UpgradeLegacyTopologies(context.Background(), deps))
}

func TestUpgradeLegacyTopologies_FailsACandidateLeftParsingAndAdvancesTheQueue(t *testing.T) {
	deps, store := newTestDeps(t)
	seedReleaseInParsing(store, "rStuck", "svc-a", false, "")
	store.MarkParsingAtUpgrade("rStuck")
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-b", ReleaseID: "rNext", ImageTag: "img-b", Repo: "acme/demo", CommitSHA: "cafe",
	}))
	queued, err := store.GetRelease("rNext")
	require.NoError(t, err)
	require.Equal(t, pipeline.StatusReceived, queued.Status(), "the parsing run holds the queue's single active slot")

	require.NoError(t, handlers.UpgradeLegacyTopologies(context.Background(), deps))

	stuck, err := store.GetRelease("rStuck")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusRejected, stuck.Status())
	assert.Equal(t, string(pkg_model.RejectReasonUpgradeInterrupted), stuck.FailReason())
	assert.NotEmpty(t, stuck.FailDetail(), "the rejection explains itself to an operator")
	var rejection struct {
		Reason string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(lastEntryOn(store, streams.ReleaseRejectedV1).Payload, &rejection))
	assert.Equal(t, "upgrade_interrupted", rejection.Reason)
	assert.Equal(t, "rejected", outcomeOf(t, lastEntryOn(store, streams.PipelineRunFinishedV1)))

	next, err := store.GetRelease("rNext")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusCompiling, next.Status(), "the queue advances past the failed run")

	// The mark is cleared, so a second start fails nothing.
	before := len(store.OutboxEntries())
	require.NoError(t, handlers.UpgradeLegacyTopologies(context.Background(), deps))
	assert.Len(t, store.OutboxEntries(), before)
}

func TestUpgradeLegacyTopologies_FailsAVerificationLeftParsingWithoutARejection(t *testing.T) {
	deps, store := newTestDeps(t)
	seedReleaseInParsing(store, "vStuck", "svc-a", true, "rOrig")
	store.MarkParsingAtUpgrade("vStuck")

	require.NoError(t, handlers.UpgradeLegacyTopologies(context.Background(), deps))

	v, err := store.GetRelease("vStuck")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusFailed, v.Status())
	assert.Nil(t, lastEntryOn(store, streams.ReleaseRejectedV1), "a verification's failure is not a release rejection")
	assert.Equal(t, "failed", outcomeOf(t, lastEntryOn(store, streams.PipelineRunFinishedV1)))
}

// A run marked at upgrade that has moved on since (its parse result was
// already consumed) is left alone.
func TestUpgradeLegacyTopologies_LeavesAMarkedRunThatMovedOnAlone(t *testing.T) {
	deps, store := newTestDeps(t)
	r := pipeline.NewCandidate("rMoved", "svc-a", "img", false, "acme/demo", "cafe", release.ManifestKindDbt, deps.Clock.Now())
	require.NoError(t, r.TransitionToParsing(deps.Clock.Now()))
	require.NoError(t, r.TransitionToValidating(release.TopologyRef{URI: "s3://continuo/tenants/default/topologies/rMoved/topology.json.gz", SHA256: "ab", NodeCount: 1}, []string{"a"}, deps.Clock.Now()))
	store.SeedRelease(r)
	store.MarkParsingAtUpgrade("rMoved")

	require.NoError(t, handlers.UpgradeLegacyTopologies(context.Background(), deps))

	got, err := store.GetRelease("rMoved")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusValidating, got.Status())
	assert.Empty(t, store.OutboxEntries())
}

// An unreachable object store moves nothing and is returned with its status
// code, so the startup step retries it.
func TestUpgradeLegacyTopologies_UnreachableStoreMovesNothing(t *testing.T) {
	deps, store := newTestDeps(t)
	store.SeedLegacyTopology("rOld", release.Topology{{UniqueID: "a"}})
	store.topologies.failWrites(storeOutage{})

	err := handlers.UpgradeLegacyTopologies(context.Background(), deps)
	require.Error(t, err)
	var status interface{ HTTPStatusCode() int }
	assert.True(t, errors.As(err, &status))
	_, moved := store.LegacyRef("rOld")
	assert.False(t, moved)
}
