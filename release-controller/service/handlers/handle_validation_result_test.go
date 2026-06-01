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

// rejectedPayload is the JSON shape released into release.rejected:v1.
type rejectedPayload struct {
	ReleaseID       string   `json:"release_id"`
	Reason          string   `json:"reason"`
	FailingNodes    []string `json:"failing_nodes"`
	MissingNodes    []string `json:"missing_nodes"`
	AggregateStatus string   `json:"aggregate_status"`
}

// seedToValidating advances a release from Received through Parsing to Validating.
// It uses ReceiveCandidate → AdvanceQueue → HandleParsedManifest(ok) with a
// two-node single-service topology (a → b, both svc-a) so that bootstrap with
// no prod snapshot does not trigger the cross-service upstream rejection.
// Returns the shared deps and fakeStore.
func seedToValidating(t *testing.T, releaseID string) (*handlers.Deps, *fakeStore) {
	t.Helper()
	deps, store := newDeps(time.Unix(100, 0).UTC())

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID:    releaseID,
		ImageTags:    map[string]string{"svc-a": "sha-a"},
		ManifestsURI: "s3://continuo/releases/" + releaseID + "/manifests/",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-a", UpstreamUniqueIDs: []string{"a"}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: releaseID,
		Status:    "ok",
		Topology:  topo,
	}))
	return deps, store
}

// TestHandleValidationResult_UnknownRelease_DropsWithoutPanic guards against a
// stale or duplicate validation.completed:v1 message whose release row no longer
// exists (e.g. it was pruned, or the message was reclaimed from a previous
// consumer for a deleted release). ReleaseRepo.Get returns (nil, nil) for a
// missing release; the handler must ack and drop rather than dereference a nil
// aggregate and crash the consumer on reclaim.
func TestHandleValidationResult_UnknownRelease_DropsWithoutPanic(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "does-not-exist",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
		},
		AggregateStatus: "ok",
	})
	require.NoError(t, err, "unknown release must be dropped, not error")

	// Nothing was written: no promotion, no rejection, no outbox rows.
	require.Empty(t, outboxEntries(store))
	assert.Equal(t, "", store.GetCurrentProd().ReleaseID())
}

func TestHandleValidationResult_AllOK_Promotes(t *testing.T) {
	deps, store := seedToValidating(t, "rA")

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "ok"},
		},
		AggregateStatus: "ok",
	})
	require.NoError(t, err)

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusPromoted, r.Status())

	cp := store.GetCurrentProd()
	assert.Equal(t, "rA", cp.ReleaseID())

	entries := outboxEntries(store)
	require.Len(t, entries, 3) // ReleaseRequested + ValidationRequested + ReleasePromoted

	third := entries[2]
	assert.Equal(t, streams.ReleasePromotedV1, third.StreamName)
}

func TestHandleValidationResult_AnyFail_Rejects(t *testing.T) {
	deps, store := seedToValidating(t, "rA")

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/b.log"},
		},
		AggregateStatus: "failed",
	})
	require.NoError(t, err)

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.RejectReason())
	assert.Equal(t, []string{"b"}, r.FailingNodes())

	// CurrentProd must remain empty since validation failed.
	cp := store.GetCurrentProd()
	assert.Equal(t, "", cp.ReleaseID())

	entries := outboxEntries(store)
	require.Len(t, entries, 3) // ReleaseRequested + ValidationRequested + ReleaseRejected

	third := entries[2]
	assert.Equal(t, streams.ReleaseRejectedV1, third.StreamName)
}

// TestHandleValidationResult_EmptyResults_Rejects ensures that a result with no
// per_node_results does not promote a release whose validation nodes were never run.
func TestHandleValidationResult_EmptyResults_Rejects(t *testing.T) {
	deps, store := seedToValidating(t, "rA")

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
		PerNodeResults:  nil,
		AggregateStatus: "ok",
	})
	require.NoError(t, err)

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.RejectReason())
}

// TestHandleValidationResult_MissingNodeInResults_Rejects ensures that a result
// that omits one of the required validation node IDs does not promote the
// release, and that the outbox payload surfaces missing_nodes distinctly from
// failing_nodes so operators can tell the two failure modes apart.
func TestHandleValidationResult_MissingNodeInResults_Rejects(t *testing.T) {
	deps, store := seedToValidating(t, "rA")

	// Report only node "a"; node "b" is missing from the results.
	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
		PerNodeResults:  []handlers.NodeResult{{NodeID: "a", Status: "ok"}},
		AggregateStatus: "ok",
	})
	require.NoError(t, err)

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Contains(t, r.FailingNodes(), "b")

	entries := outboxEntries(store)
	require.Len(t, entries, 3)
	var payload rejectedPayload
	require.NoError(t, json.Unmarshal(entries[2].Payload, &payload))
	assert.Empty(t, payload.FailingNodes, "no explicitly-failed nodes in this scenario")
	assert.Equal(t, []string{"b"}, payload.MissingNodes, "b was expected but never reported")
	assert.Equal(t, "ok", payload.AggregateStatus, "aggregate_status passed through unchanged")
}

// TestHandleValidationResult_AggregateStatusFailed_Rejects covers the edge case
// where every reported node passed but the aggregate_status is not "ok".
// Without the audit-trail fix the rejected event would carry an empty
// failing_nodes slice and no signal about why the release was rejected;
// this test guards that the outbox payload preserves aggregate_status so
// operators can diagnose the rejection.
func TestHandleValidationResult_AggregateStatusFailed_Rejects(t *testing.T) {
	deps, store := seedToValidating(t, "rA")

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "ok"},
		},
		AggregateStatus: "partial_failed",
	})
	require.NoError(t, err)

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.RejectReason())
	assert.Empty(t, r.FailingNodes(), "no explicitly-failed or missing nodes when only aggregate is bad")

	entries := outboxEntries(store)
	require.Len(t, entries, 3)
	var payload rejectedPayload
	require.NoError(t, json.Unmarshal(entries[2].Payload, &payload))
	assert.Empty(t, payload.FailingNodes)
	assert.Empty(t, payload.MissingNodes)
	assert.Equal(t, "partial_failed", payload.AggregateStatus,
		"aggregate_status must be surfaced so operators can diagnose a rejection with no per-node signal")
}
