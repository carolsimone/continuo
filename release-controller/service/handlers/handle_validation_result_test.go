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

// seedToValidating advances a release from Received through Parsing to Validating.
// It uses ReceiveCandidate → AdvanceQueue → HandleParsedManifest(ok) with a
// two-node topology (a → b). Returns the shared deps and fakeUoW.
func seedToValidating(t *testing.T, releaseID string) (*handlers.Deps, *fakeUoW) {
	t.Helper()
	deps, u := newDeps(time.Unix(100, 0).UTC())

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID:      releaseID,
		ChangedNodeIDs: []string{"a", "b"},
		ImageTags:      map[string]string{"svc-a": "sha-a", "svc-b": "sha-b"},
		ManifestsURI:   "s3://continuo/releases/" + releaseID + "/manifests/",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-b", UpstreamUniqueIDs: []string{"a"}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: releaseID,
		Status:    "ok",
		Topology:  topo,
	}))
	return deps, u
}

func TestHandleValidationResult_AllOK_Promotes(t *testing.T) {
	deps, u := seedToValidating(t, "rA")

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "ok"},
		},
		AggregateStatus: "ok",
	})
	require.NoError(t, err)

	r, err := u.ReleaseRepo().Get(context.Background(), "rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusPromoted, r.Status())

	cp, err := u.CurrentProdRepo().Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "rA", cp.ReleaseID())

	entries := outboxEntries(u)
	require.Len(t, entries, 3) // ReleaseRequested + ValidationRequested + ReleasePromoted

	third := entries[2]
	assert.Equal(t, streams.ReleasePromotedV1, third.StreamName)
}

func TestHandleValidationResult_AnyFail_Rejects(t *testing.T) {
	deps, u := seedToValidating(t, "rA")

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/b.log"},
		},
		AggregateStatus: "failed",
	})
	require.NoError(t, err)

	r, err := u.ReleaseRepo().Get(context.Background(), "rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.RejectReason())
	assert.Equal(t, []string{"b"}, r.FailingNodes())

	// CurrentProd must remain empty since validation failed.
	cp, err := u.CurrentProdRepo().Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "", cp.ReleaseID())

	entries := outboxEntries(u)
	require.Len(t, entries, 3) // ReleaseRequested + ValidationRequested + ReleaseRejected

	third := entries[2]
	assert.Equal(t, streams.ReleaseRejectedV1, third.StreamName)
}
