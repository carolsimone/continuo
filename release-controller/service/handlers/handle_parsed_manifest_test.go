package handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedToParsing advances a release from Received to Parsing via ReceiveCandidate + AdvanceQueue.
// Returns the deps and UoW for further assertions or handler calls.
func seedToParsing(t *testing.T, releaseID string, changedNodeIDs []string, imageTags map[string]string) (*handlers.Deps, *fakeUoW) {
	t.Helper()
	deps, u := newDeps(time.Unix(100, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID:      releaseID,
		ChangedNodeIDs: changedNodeIDs,
		ImageTags:      imageTags,
		ManifestsURI:   "s3://continuo/releases/" + releaseID + "/manifests/",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	return deps, u
}

func TestHandleParsedManifest_OK_TransitionsToValidating(t *testing.T) {
	deps, u := seedToParsing(t, "rA", []string{"a", "b"}, map[string]string{"svc-a": "sha-a", "svc-b": "sha-b"})

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-b", UpstreamUniqueIDs: []string{"a"}},
	}
	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	})
	require.NoError(t, err)

	r, err := u.ReleaseRepo().Get(context.Background(), "rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusValidating, r.Status())

	validIDs := r.ValidationNodeIDs()
	assert.Contains(t, validIDs, "a")
	assert.Contains(t, validIDs, "b")

	entries := outboxEntries(u)
	require.Len(t, entries, 2) // ReleaseRequested + ValidationRequested

	second := entries[1]
	assert.Equal(t, streams.ValidationRequestedV1, second.StreamName)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(second.Payload, &payload))
	assert.Equal(t, "rA", payload["release_id"])
	assert.Equal(t, "validation", payload["mode"])
}

func TestHandleParsedManifest_Failed_TransitionsToRejected(t *testing.T) {
	deps, u := seedToParsing(t, "rA", []string{"a"}, map[string]string{"svc-a": "sha-a"})

	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:   "rA",
		Status:      "failed",
		ErrorClass:  "UnresolvedReference",
		ErrorDetail: "ref('missing') unresolved in service_1.table_a",
	})
	require.NoError(t, err)

	r, err := u.ReleaseRepo().Get(context.Background(), "rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "parse_failed", r.RejectReason())

	entries := outboxEntries(u)
	require.Len(t, entries, 2) // ReleaseRequested + ReleaseRejected

	second := entries[1]
	assert.Equal(t, streams.ReleaseRejectedV1, second.StreamName)
}

func TestHandleParsedManifest_ImageTagJoinedIntoTopology(t *testing.T) {
	imageTags := map[string]string{
		"svc-a": "tag-alpha",
		"svc-b": "tag-beta",
	}
	deps, u := seedToParsing(t, "rA", []string{"a", "b"}, imageTags)

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-b", UpstreamUniqueIDs: []string{"a"}},
	}
	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	})
	require.NoError(t, err)

	r, err := u.ReleaseRepo().Get(context.Background(), "rA")
	require.NoError(t, err)

	stored := r.CandidateTopology()
	require.Len(t, stored, 2)

	nodeByID := map[string]release.Node{}
	for _, n := range stored {
		nodeByID[n.UniqueID] = n
	}
	assert.Equal(t, "tag-alpha", nodeByID["a"].ImageTag)
	assert.Equal(t, "tag-beta", nodeByID["b"].ImageTag)
}
