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
	Repo            string   `json:"repo"`
	CommitSHA       string   `json:"commit_sha"`
}

// seedToValidating advances a release from Received through Parsing to Validating.
// It uses ReceiveCandidate → AdvanceQueue → HandleParsedManifest(ok) with a
// two-node single-service topology (a → b, both svc-a) so that bootstrap with
// no prod snapshot does not trigger the cross-service upstream rejection.
// Returns the shared deps and fakeStore.
func seedToValidating(t *testing.T, releaseID string) (*handlers.Deps, *fakeStore) {
	t.Helper()
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   "svc-a",
		ReleaseID: releaseID,
		ImageTag:  "sha-a",
		Repo:      "acme/demo",
		CommitSHA: "deadbeef",
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

	// Assert the changed service's service_prod row carries the correct values.
	sp := store.GetServiceProd("svc-a")
	require.NotNil(t, sp)
	assert.Equal(t, "rA", sp.ReleaseID())
	assert.Equal(t, "s3://continuo/svc-a/rA/manifest.json", sp.ManifestS3Key())
	assert.Equal(t, "sha-a", sp.ImageTag())
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

// seedToValidatingWithURIs is like seedToValidating but uses a two-node topology
// where each node carries a CandidateSQLURI so the rejected payload enrichment
// can be verified. Node "b" (svc-a) fails validation; node "a" passes.
func seedToValidatingWithURIs(t *testing.T, releaseID string) (*handlers.Deps, *fakeStore) {
	t.Helper()
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   "svc-a",
		ReleaseID: releaseID,
		ImageTag:  "sha-a",
		Repo:      "acme/demo",
		CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{},
			CandidateSQLURI: "s3://continuo/svc-a/" + releaseID + "/candidate_a.sql"},
		{UniqueID: "b", ServiceName: "svc-a", UpstreamUniqueIDs: []string{"a"},
			CandidateSQLURI: "s3://continuo/svc-a/" + releaseID + "/candidate_b.sql"},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: releaseID,
		Status:    "ok",
		Topology:  topo,
	}))
	return deps, store
}

// TestHandleValidationResult_Rejected_CarriesCandidateSQLURIAndProvenance asserts
// that the release.rejected:v1 outbox payload emitted for a validation failure
// carries:
//   - per_node[*].candidate_sql_uri  — S3 URI pointer to the candidate SQL for each node
//   - top-level "repo" and "commit_sha" — provenance fields from the release aggregate
//
// This allows the consumer to fetch the exact SQL that was validated when
// investigating a failure without inline SQL bloat in the event.
func TestHandleValidationResult_Rejected_CarriesCandidateSQLURIAndProvenance(t *testing.T) {
	deps, store := seedToValidatingWithURIs(t, "rA")

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/rA/b.log", RunResultsURI: "run-results/rA/b.json"},
		},
		AggregateStatus: "failed",
	})
	require.NoError(t, err)

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusRejected, r.Status())

	entries := outboxEntries(store)
	require.Len(t, entries, 3) // ReleaseRequested + ValidationRequested + ReleaseRejected
	rejEntry := entries[2]
	assert.Equal(t, streams.ReleaseRejectedV1, rejEntry.StreamName)

	// Decode top-level payload
	var topLevel map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rejEntry.Payload, &topLevel))

	// Assert top-level provenance fields
	var repo string
	require.NoError(t, json.Unmarshal(topLevel["repo"], &repo))
	assert.Equal(t, "acme/demo", repo, "top-level repo must come from release aggregate")

	var commitSHA string
	require.NoError(t, json.Unmarshal(topLevel["commit_sha"], &commitSHA))
	assert.Equal(t, "deadbeef", commitSHA, "top-level commit_sha must come from release aggregate")

	// Decode per_node and check candidate_sql_uri per entry
	var perNode []struct {
		NodeID          string `json:"node_id"`
		Status          string `json:"status"`
		DBTLogURI       string `json:"dbt_log_uri,omitempty"`
		RunResultsURI   string `json:"run_results_uri,omitempty"`
		CandidateSQLURI string `json:"candidate_sql_uri,omitempty"`
	}
	require.NoError(t, json.Unmarshal(topLevel["per_node"], &perNode))
	require.Len(t, perNode, 2)

	byID := map[string]string{}
	runResultsByID := map[string]string{}
	for _, pn := range perNode {
		byID[pn.NodeID] = pn.CandidateSQLURI
		runResultsByID[pn.NodeID] = pn.RunResultsURI
	}
	assert.Equal(t, "s3://continuo/svc-a/rA/candidate_a.sql", byID["a"],
		"ok nodes must also carry candidate_sql_uri (pointer, not inline SQL)")
	assert.Equal(t, "s3://continuo/svc-a/rA/candidate_b.sql", byID["b"],
		"failing node must carry candidate_sql_uri")
	assert.Equal(t, "run-results/rA/b.json", runResultsByID["b"],
		"failing node must carry run_results_uri through to release.rejected:v1")
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

// promotedNodeWire mirrors the per-node shape of release.promoted:v1, including
// the new `changed` flag and all other per-node fields to catch regressions
// if any field is dropped or renamed.
type promotedNodeWire struct {
	UniqueID          string   `json:"unique_id"`
	SchemaName        string   `json:"schema_name"`
	TableName         string   `json:"table_name"`
	ServiceName       string   `json:"service_name"`
	NodeType          string   `json:"node_type"`
	ContentHash       string   `json:"content_hash"`
	ImageTag          string   `json:"image_tag"`
	UpstreamUniqueIDs []string `json:"upstream_unique_ids"`
	Schedule          string   `json:"schedule"`
	Changed           bool     `json:"changed"`
	OriginalFilePath  string   `json:"original_file_path"`
}

// promotedPayload is the JSON shape released into release.promoted:v1.
type promotedPayload struct {
	ReleaseID  string             `json:"release_id"`
	Repo       string             `json:"repo"`
	CommitSHA  string             `json:"commit_sha"`
	PromotedAt time.Time          `json:"promoted_at"`
	Topology   []promotedNodeWire `json:"topology"`
}

// TestHandleValidationResult_Promote_StampsChangedAndProvenance verifies that a
// promotion emits release-level provenance (repo, commit_sha, promoted_at) and
// flags exactly the nodes whose content_hash differs from the prior prod: node
// "b" changed (hash differs), node "a" unchanged (hash matches). Node "a" is
// validated because it is "b"'s ancestor, but it must carry changed=false.
func TestHandleValidationResult_Promote_StampsChangedAndProvenance(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	// Prior prod: a@"h", b@"old". Both in svc-a; b depends on a.
	store.SeedCurrentProd(release.RehydrateCurrentProd("r0", release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-a", ContentHash: "old", UpstreamUniqueIDs: []string{"a"}},
	}, time.Unix(50, 0).UTC()))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   "svc-a",
		ReleaseID: "rA",
		ImageTag:  "sha-a",
		Repo:      "acme/demo",
		CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	// Candidate: a unchanged (hash "h"), b changed (hash "new").
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h", UpstreamUniqueIDs: []string{}},
			{UniqueID: "b", ServiceName: "svc-a", ContentHash: "new", UpstreamUniqueIDs: []string{"a"}},
		},
	}))

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "ok"},
		},
		AggregateStatus: "ok",
	}))

	entries := outboxEntries(store)
	require.Len(t, entries, 3) // ReleaseRequested + ValidationRequested + ReleasePromoted
	promotedEntry := entries[2]
	require.Equal(t, streams.ReleasePromotedV1, promotedEntry.StreamName)

	var p promotedPayload
	require.NoError(t, json.Unmarshal(promotedEntry.Payload, &p))
	assert.Equal(t, "acme/demo", p.Repo)
	assert.Equal(t, "deadbeef", p.CommitSHA)
	assert.Equal(t, time.Unix(100, 0).UTC(), p.PromotedAt.UTC())

	changedByID := map[string]bool{}
	contentHashByID := map[string]string{}
	for _, n := range p.Topology {
		changedByID[n.UniqueID] = n.Changed
		contentHashByID[n.UniqueID] = n.ContentHash
	}
	assert.False(t, changedByID["a"], "a is unchanged (hash matches prior prod)")
	assert.True(t, changedByID["b"], "b changed (hash differs from prior prod)")
	assert.Equal(t, "new", contentHashByID["b"], "b's content_hash must match candidate (verifies field is emitted)")
	assert.Equal(t, "h", contentHashByID["a"], "a's content_hash must match candidate")
}

// TestHandleValidationResult_Promote_EmitsOriginalFilePath verifies that promotion
// carries the original_file_path field from the candidate topology through to the
// release.promoted:v1 event, allowing the orchestrator to persist ancestry metadata.
func TestHandleValidationResult_Promote_EmitsOriginalFilePath(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "sha-a", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA", Status: "ok",
		Topology: release.Topology{
			{UniqueID: "a", ServiceName: "svc-a", OriginalFilePath: "models/a.sql", UpstreamUniqueIDs: []string{}},
		},
	}))
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA", PerNodeResults: []handlers.NodeResult{{NodeID: "a", Status: "ok"}}, AggregateStatus: "ok",
	}))

	entries := outboxEntries(store)
	last := entries[len(entries)-1]
	require.Equal(t, streams.ReleasePromotedV1, last.StreamName)
	var p promotedPayload
	require.NoError(t, json.Unmarshal(last.Payload, &p))
	require.Len(t, p.Topology, 1)
	assert.Equal(t, "models/a.sql", p.Topology[0].OriginalFilePath)
}
