package handlers_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
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
	// Simulate the compile leg completing (Compiling → Parsing).
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: releaseID, Status: "ok",
	}))

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
	require.Len(t, entries, 4) // CompileRequested + ReleaseRequested + ValidationRequested + ReleasePromoted

	third := entries[3]
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
	require.Len(t, entries, 4) // CompileRequested + ReleaseRequested + ValidationRequested + ReleaseRejected

	third := entries[3]
	assert.Equal(t, streams.ReleaseRejectedV1, third.StreamName)

	// The outbox payload must carry stage="validation" so consumers can distinguish
	// compile-leg rejections from validation-leg rejections.
	var topLevel map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(third.Payload, &topLevel))
	var stage string
	require.NoError(t, json.Unmarshal(topLevel["stage"], &stage))
	assert.Equal(t, "validation", stage)
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
	require.Len(t, entries, 4)
	var payload rejectedPayload
	require.NoError(t, json.Unmarshal(entries[3].Payload, &payload))
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
	// Simulate the compile leg completing (Compiling → Parsing).
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: releaseID, Status: "ok",
	}))

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
	require.Len(t, entries, 4) // CompileRequested + ReleaseRequested + ValidationRequested + ReleaseRejected
	rejEntry := entries[3]
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
	require.Len(t, entries, 4)
	var payload rejectedPayload
	require.NoError(t, json.Unmarshal(entries[3].Payload, &payload))
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
	TestCount         int      `json:"test_count"`
	ImageTag          string   `json:"image_tag"`
	UpstreamUniqueIDs []string `json:"upstream_unique_ids"`
	Schedule          string   `json:"schedule"`
	Changed           bool     `json:"changed"`
	OriginalFilePath  string   `json:"original_file_path"`

	DBTUniqueID                       string `json:"dbt_unique_id"`
	RuntimeManifestURI                string `json:"runtime_manifest_uri"`
	RuntimeManifestSHA256             string `json:"runtime_manifest_sha256"`
	RuntimeManifestDBTVersion         string `json:"runtime_manifest_dbt_version"`
	RuntimeManifestParseContextSHA256 string `json:"runtime_manifest_parse_context_sha256"`
}

// promotedPayload is the JSON shape released into release.promoted:v1.
type promotedPayload struct {
	ReleaseID  string             `json:"release_id"`
	Repo       string             `json:"repo"`
	CommitSHA  string             `json:"commit_sha"`
	PromotedAt time.Time          `json:"promoted_at"`
	Topology   []promotedNodeWire `json:"topology"`
}

// runtimeRef builds a complete reference distinguished by seed, so a test can
// tell one service's artifact from another's.
func runtimeRef(seed string) pkgmodel.RuntimeManifestRef {
	return pkgmodel.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://continuo/" + seed + "/manifest.msgpack",
		RuntimeManifestSHA256:             seed + strings.Repeat("0", 64-len(seed)),
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: seed + strings.Repeat("1", 64-len(seed)),
	}
}

// TestHandleValidationResult_Promote_UnchangedServiceKeepsItsOwnRuntimeManifest
// is the carry-forward guarantee. An incremental release changes svc-a only.
// svc-b's nodes and its service_prod pointer must still name svc-b's original
// artifact afterwards — never svc-a's. Repointing a stable service at another
// service's manifest would silently execute it against a project that never
// described it.
func TestHandleValidationResult_Promote_UnchangedServiceKeepsItsOwnRuntimeManifest(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	refAOld, refANew, refB := runtimeRef("a1"), runtimeRef("a2"), runtimeRef("bb")

	// svc-b was promoted by an earlier release and pins its own artifact. Its
	// manifest key is carried into this release untouched, so the parse resolves
	// that same release directory's descriptor and reports refB again.
	store.SeedServiceProd(release.NewServiceProdWithRuntime(
		"svc-b", "r0", "s3://continuo/svc-b/r0/manifest.json", "sha-b", refB, time.Unix(50, 0).UTC()))

	// Prior prod holds both services; a@"h-a-old" is what this release changes.
	store.SeedCurrentProd(release.RehydrateCurrentProd("r0", release.Topology{
		{UniqueID: "public.a", ServiceName: "svc-a", ContentHash: "h-a-old",
			UpstreamUniqueIDs: []string{}, RuntimeManifestRef: refAOld},
		{UniqueID: "public.b", ServiceName: "svc-b", ContentHash: "h-b",
			UpstreamUniqueIDs: []string{}, RuntimeManifestRef: refB},
	}, time.Unix(50, 0).UTC()))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rB", ImageTag: "sha-a2", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: "rB", Status: "ok",
	}))

	// The parse reports a fresh artifact for the changed service and svc-b's
	// unchanged one, each keyed by its own service.
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rB",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "public.a", DBTUniqueID: "model.svc_a.a", ServiceName: "svc-a",
				ContentHash: "h-a-new", UpstreamUniqueIDs: []string{}},
			{UniqueID: "public.b", DBTUniqueID: "model.svc_b.b", ServiceName: "svc-b",
				ContentHash: "h-b", UpstreamUniqueIDs: []string{}},
		},
		RuntimeManifests: map[string]pkgmodel.RuntimeManifestRef{"svc-a": refANew, "svc-b": refB},
	}))

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rB",
		PerNodeResults:  []handlers.NodeResult{{NodeID: "public.a", Status: "ok"}},
		AggregateStatus: "ok",
	}))

	entries := outboxEntries(store)
	promotedEntry := entries[len(entries)-1]
	require.Equal(t, streams.ReleasePromotedV1, promotedEntry.StreamName)

	var p promotedPayload
	require.NoError(t, json.Unmarshal(promotedEntry.Payload, &p))
	byID := map[string]promotedNodeWire{}
	for _, n := range p.Topology {
		byID[n.UniqueID] = n
	}
	require.Len(t, byID, 2)

	// The changed service's nodes carry its new artifact.
	assert.Equal(t, refANew.RuntimeManifestSHA256, byID["public.a"].RuntimeManifestSHA256)
	assert.Equal(t, refANew.RuntimeManifestURI, byID["public.a"].RuntimeManifestURI)
	assert.Equal(t, "model.svc_a.a", byID["public.a"].DBTUniqueID)

	// The unchanged service's node still carries its OWN old artifact.
	assert.Equal(t, refB.RuntimeManifestSHA256, byID["public.b"].RuntimeManifestSHA256,
		"svc-b's node must keep svc-b's artifact")
	assert.NotEqual(t, refANew.RuntimeManifestSHA256, byID["public.b"].RuntimeManifestSHA256,
		"svc-b's node must never be repointed at svc-a's artifact")
	assert.Equal(t, refB.RuntimeManifestParseContextSHA256, byID["public.b"].RuntimeManifestParseContextSHA256)

	// Graph identity is untouched by any of this.
	assert.Equal(t, "public.b", byID["public.b"].UniqueID)
	assert.Equal(t, "model.svc_b.b", byID["public.b"].DBTUniqueID)

	// The changed service's pointer is re-pinned to its new artifact...
	spA := store.GetServiceProd("svc-a")
	require.NotNil(t, spA)
	assert.Equal(t, refANew, spA.RuntimeManifest())
	assert.Equal(t, "rB", spA.ReleaseID())

	// ...while the unchanged service's persisted pointer is left exactly as it
	// was: same release, same key, same artifact.
	spB := store.GetServiceProd("svc-b")
	require.NotNil(t, spB)
	assert.Equal(t, refB, spB.RuntimeManifest(), "svc-b's pointer must keep its own artifact")
	assert.Equal(t, "r0", spB.ReleaseID(), "svc-b's pointer must still name its own release")
	assert.Equal(t, "s3://continuo/svc-b/r0/manifest.json", spB.ManifestS3Key())
}

// TestHandleValidationResult_Promote_LegacyReleaseCarriesNoRuntimeManifest
// verifies a release whose parse reported no artifacts still promotes: its nodes
// and its service_prod pointer simply pin nothing, and their Jobs keep parsing
// the project themselves.
func TestHandleValidationResult_Promote_LegacyReleaseCarriesNoRuntimeManifest(t *testing.T) {
	deps, store := seedToValidating(t, "rLegacy")

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rLegacy",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "ok"},
		},
		AggregateStatus: "ok",
	}))

	entries := outboxEntries(store)
	promotedEntry := entries[len(entries)-1]
	require.Equal(t, streams.ReleasePromotedV1, promotedEntry.StreamName)

	// The published nodes must omit the runtime keys entirely rather than emit
	// empty ones, which a consumer could read as a partial reference.
	var raw struct {
		Topology []map[string]any `json:"topology"`
	}
	require.NoError(t, json.Unmarshal(promotedEntry.Payload, &raw))
	require.NotEmpty(t, raw.Topology)
	for _, n := range raw.Topology {
		assert.NotContains(t, n, "runtime_manifest_uri")
		assert.NotContains(t, n, "runtime_manifest_sha256")
		assert.NotContains(t, n, "dbt_unique_id")
	}

	sp := store.GetServiceProd("svc-a")
	require.NotNil(t, sp)
	assert.Equal(t, pkgmodel.RuntimeManifestRef{}, sp.RuntimeManifest())
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
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: "rA", Status: "ok",
	}))

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
	require.Len(t, entries, 4) // CompileRequested + ReleaseRequested + ValidationRequested + ReleasePromoted
	promotedEntry := entries[3]
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

// TestHandleValidationResult_Promote_CarriesCandidateSchema verifies that the
// release.promoted:v1 payload includes candidate_schema so the executor-controller's
// release.promoted teardown consumer can drop the schema when present (idempotent
// no-op if validation.completed already cleaned it up).
func TestHandleValidationResult_Promote_CarriesCandidateSchema(t *testing.T) {
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

	entries := outboxEntries(store)
	promotedEntry := entries[len(entries)-1]
	require.Equal(t, streams.ReleasePromotedV1, promotedEntry.StreamName)

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(promotedEntry.Payload, &payload))

	var candidateSchema string
	require.NoError(t, json.Unmarshal(payload["candidate_schema"], &candidateSchema))
	assert.Equal(t, "_candidate_rA", candidateSchema,
		"release.promoted:v1 must carry candidate_schema for executor teardown")
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
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: "rA", Status: "ok",
	}))
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

// TestHandleValidationResult_Promote_EmitsTestCount verifies that promotion
// carries the per-node test_count from the candidate topology through to the
// release.promoted:v1 event, allowing the orchestrator to persist it onto the
// Neo4j node.
func TestHandleValidationResult_Promote_EmitsTestCount(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "sha-a", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: "rA", Status: "ok",
	}))
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA", Status: "ok",
		Topology: release.Topology{
			{UniqueID: "a", ServiceName: "svc-a", TestCount: 3, UpstreamUniqueIDs: []string{}},
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
	assert.Equal(t, 3, p.Topology[0].TestCount, "release.promoted:v1 must carry per-node test_count")
}
