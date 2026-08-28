package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
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

// seedValidationNodes projects each per-node validation result into the release
// read model through HandleNodeValidationResult, exactly as the
// validation.result:v1 (kind=node) rows do at runtime. The slim
// validation.result:v1 (kind=complete) terminal event no longer carries per-node content, so
// the results its decision reads must already be stored before it is invoked.
func seedValidationNodes(t *testing.T, deps *handlers.Deps, releaseID string, nodes []handlers.NodeResult) {
	t.Helper()
	for _, n := range nodes {
		require.NoError(t, handlers.HandleNodeValidationResult(context.Background(), deps, handlers.NodeValidationResultInput{
			ReleaseID:     releaseID,
			Stage:         "validation",
			NodeID:        n.NodeID,
			Status:        n.Status,
			DBTLogURI:     n.DBTLogURI,
			RunResultsURI: n.RunResultsURI,
		}))
	}
}

// TestHandleValidationResult_MissingNode_AggregateOK_Promotes covers the only
// way a node can be absent from the store under a single in-order consumer: its
// projection write was permanently dropped. The decision must not block or treat
// the absent node as failing. Node "b" is never stored; because the authoritative
// aggregate_status is "ok" (the dropped row's node actually passed), the release
// must PROMOTE and the missing node must not be fabricated as failing.
func TestHandleValidationResult_MissingNode_AggregateOK_Promotes(t *testing.T) {
	deps, store := seedToValidating(t, "rA")

	// Swap in a buffer-backed logger so the test can assert the missing-node
	// warn actually fires, instead of only inferring it from the promote outcome.
	var logBuf bytes.Buffer
	deps.Logger = slog.New(slog.NewTextHandler(&logBuf, nil))

	// Only node "a" projected; "b"'s projection write was permanently dropped.
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{{NodeID: "a", Status: "ok"}})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
		AggregateStatus: "ok",
	}))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusPromoted, r.Status(),
		"a missing audit row must not reject a release the aggregate says passed")
	assert.Empty(t, r.FailingNodes(), "the missing node must not be fabricated as failing")
	assert.Contains(t, logBuf.String(), "per-node audit missing nodes",
		"the missing-node warn must fire when deciding from aggregate_status")
}

// TestHandleValidationResult_MissingNode_AggregateFailed_Rejects verifies that a
// missing node does not itself count as failing, but the decision still rejects
// when the authoritative aggregate_status is not ok. Node "b" is absent and node
// "a" passed, so no present node is failing; the rejection rests entirely on
// aggregate_status.
func TestHandleValidationResult_MissingNode_AggregateFailed_Rejects(t *testing.T) {
	deps, store := seedToValidating(t, "rA")

	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{{NodeID: "a", Status: "ok"}})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
		AggregateStatus: "failed",
	}))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.RejectReason())
	assert.Empty(t, r.FailingNodes(), "no present node failed; rejection is driven by aggregate_status alone")
}

// TestHandleValidationResult_CompleteProjection_Promotes verifies that once every
// expected per-node result is stored and the aggregate is ok, the release
// promotes.
func TestHandleValidationResult_CompleteProjection_Promotes(t *testing.T) {
	deps, store := seedToValidating(t, "rA")
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "ok"},
	})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
		AggregateStatus: "ok",
	}))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusPromoted, r.Status())
}

// TestHandleValidationResult_FailedNodeInStore_Rejects verifies that a failed
// per-node result already stored in the read model drives a rejection naming
// that node, even though the terminal event itself carries only the aggregate.
func TestHandleValidationResult_FailedNodeInStore_Rejects(t *testing.T) {
	deps, store := seedToValidating(t, "rA")
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{
		{NodeID: "a", Status: "failed", DBTLogURI: "s3://l"},
		{NodeID: "b", Status: "ok"},
	})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
		AggregateStatus: "failed",
	}))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, []string{"a"}, r.FailingNodes())

	e := lastOutbox(t, store)
	assert.Equal(t, streams.ReleaseRejectedV1, e.StreamName)
	assert.JSONEq(t, string(e.Payload), string(r.RejectionPayload()),
		"the rejection payload stored on the release must match the one emitted on release.rejected:v1")
}

// TestHandleValidationResult_SkippedNodeInStore_Rejects verifies that a per-node
// result with status "skipped" (emitted for a node whose upstream failed
// validation, so it never ran) is present in the store and counts as failing:
// the release rejects and names the skipped node. This is the read-model side of
// the executor-controller emitting a skip projection for every node its failure
// propagation skips, so the node is present rather than absent from the store.
func TestHandleValidationResult_SkippedNodeInStore_Rejects(t *testing.T) {
	deps, store := seedToValidating(t, "rA")
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{
		{NodeID: "a", Status: "failed", DBTLogURI: "s3://l"},
		{NodeID: "b", Status: "skipped"}, // downstream of the failed node; never ran
	})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
		AggregateStatus: "failed",
	}))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusRejected, r.Status())
	// Both the failed node and the skipped node are non-ok, so both are failing.
	assert.Equal(t, []string{"a", "b"}, r.FailingNodes())
}

// TestHandleValidationResult_UnknownRelease_DropsWithoutPanic guards against a
// stale or duplicate validation.result:v1 (kind=complete) message whose release row no longer
// exists (e.g. it was pruned, or the message was reclaimed from a previous
// consumer for a deleted release). ReleaseRepo.Get returns (nil, nil) for a
// missing release; the handler must ack and drop rather than dereference a nil
// aggregate and crash the consumer on reclaim.
func TestHandleValidationResult_UnknownRelease_DropsWithoutPanic(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "does-not-exist",
		AggregateStatus: "ok",
	})
	require.NoError(t, err, "unknown release must be dropped, not error")

	// Nothing was written: no promotion, no rejection, no outbox rows.
	require.Empty(t, outboxEntries(store))
	assert.Equal(t, "", store.GetCurrentProd().ReleaseID())
}

func TestHandleValidationResult_AllOK_Promotes(t *testing.T) {
	deps, store := seedToValidating(t, "rA")
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "ok"},
	})

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
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

// seedToValidatingShadow mirrors seedToValidating but registers the release
// with Shadow: true, so promotion tests can assert a shadow release stops at
// StatusValidated instead of reaching current_prod.
func seedToValidatingShadow(t *testing.T, releaseID string) (*handlers.Deps, *fakeStore) {
	t.Helper()
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   "svc-a",
		ReleaseID: releaseID,
		ImageTag:  "sha-a",
		Repo:      "acme/demo",
		CommitSHA: "deadbeef",
		Shadow:    true,
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
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

// TestHandleValidationResult_Shadow_StopsAtValidated_NeverPromotes drives
// HandleValidationResult with a shadow release in Validating and an all-ok
// aggregate. The shadow release must stop at StatusValidated: no
// release.promoted:v1 outbox row, current_prod's Upsert never called, and the
// promoted-telemetry span never fires. A control on the identical flow with
// shadow=false must still promote exactly as today, proving the gate is
// specific to the shadow flag rather than a general regression.
func TestHandleValidationResult_Shadow_StopsAtValidated_NeverPromotes(t *testing.T) {
	t.Run("shadow release stops at validated and never promotes", func(t *testing.T) {
		deps, store := seedToValidatingShadow(t, "rShadow")
		spy := &spyTelemetry{}
		deps.Telemetry = spy
		seedValidationNodes(t, deps, "rShadow", []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "ok"},
		})

		err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
			ReleaseID:       "rShadow",
			AggregateStatus: "ok",
		})
		require.NoError(t, err)

		r, err := store.GetRelease("rShadow")
		require.NoError(t, err)
		assert.Equal(t, release.StatusValidated, r.Status(),
			"a shadow release must stop at validated, not promoted")

		entries := outboxEntries(store)
		for _, e := range entries {
			assert.NotEqual(t, streams.ReleasePromotedV1, e.StreamName,
				"a shadow release must never emit release.promoted:v1")
		}

		assert.Equal(t, 0, store.CurrentProdUpsertCalls(),
			"a shadow release must never call CurrentProdRepo.Upsert")
		cp := store.GetCurrentProd()
		assert.Equal(t, "", cp.ReleaseID(), "current_prod must remain untouched by a shadow release")

		assert.Nil(t, store.GetServiceProd("svc-a"),
			"a shadow release must never upsert service_prod")

		assert.Equal(t, 0, spy.releasePromotedCalls,
			"a shadow release must never fire the promoted telemetry span")
	})

	t.Run("control: shadow=false on the identical flow still promotes", func(t *testing.T) {
		deps, store := seedToValidating(t, "rShadowControl")
		seedValidationNodes(t, deps, "rShadowControl", []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "ok"},
		})

		err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
			ReleaseID:       "rShadowControl",
			AggregateStatus: "ok",
		})
		require.NoError(t, err)

		r, err := store.GetRelease("rShadowControl")
		require.NoError(t, err)
		assert.Equal(t, release.StatusPromoted, r.Status())

		cp := store.GetCurrentProd()
		assert.Equal(t, "rShadowControl", cp.ReleaseID())

		entries := outboxEntries(store)
		var sawPromoted bool
		for _, e := range entries {
			if e.StreamName == streams.ReleasePromotedV1 {
				sawPromoted = true
			}
		}
		assert.True(t, sawPromoted, "a non-shadow release must still emit release.promoted:v1")
	})
}

// seedToValidatingPython mirrors seedToValidating but registers the release as
// Kind: "python" for a distinct service, so promotion tests can assert the
// service_prod pointer written for a python-kind release points at
// contract.yaml rather than manifest.json. A python release has no compile
// leg: AdvanceQueue activates it straight into Parsing, so — unlike
// seedToValidating — there is no HandleCompileResult call here.
func seedToValidatingPython(t *testing.T, releaseID string) (*handlers.Deps, *fakeStore) {
	t.Helper()
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   "svc-py",
		ReleaseID: releaseID,
		ImageTag:  "sha-py",
		Repo:      "acme/demo",
		CommitSHA: "deadbeef",
		Kind:      "python",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-py", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-py", UpstreamUniqueIDs: []string{"a"}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: releaseID,
		Status:    "ok",
		Topology:  topo,
	}))
	return deps, store
}

// TestHandleValidationResult_PythonKind_PromotesWithContractYAMLPointer verifies
// that promoting a python-kind release upserts a service_prod pointer whose
// ManifestKind is python and whose stored S3 key ends in contract.yaml, not
// manifest.json — the promote-path counterpart to
// TestCanonicalManifestKey_PerKindArtifactName.
func TestHandleValidationResult_PythonKind_PromotesWithContractYAMLPointer(t *testing.T) {
	deps, store := seedToValidatingPython(t, "rPy")
	seedValidationNodes(t, deps, "rPy", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "ok"},
	})

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rPy",
		AggregateStatus: "ok",
	})
	require.NoError(t, err)

	sp := store.GetServiceProd("svc-py")
	require.NotNil(t, sp)
	assert.Equal(t, release.ManifestKindPython, sp.ManifestKind())
	assert.True(t, strings.HasSuffix(sp.ManifestS3Key(), "/contract.yaml"))
}

func TestHandleValidationResult_AnyFail_Rejects(t *testing.T) {
	deps, store := seedToValidating(t, "rA")
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/b.log"},
	})

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
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

	var shadow bool
	require.NoError(t, json.Unmarshal(topLevel["shadow"], &shadow))
	assert.False(t, shadow, "a non-shadow release's validation_failed rejection must carry shadow:false")
}

// TestHandleValidationResult_Shadow_Rejected_CarriesShadowTrue verifies that a
// shadow release's validation_failed rejection carries shadow:true on
// release.rejected:v1. This is the case agent-remediation's fix-verification
// loop hinges on: without this signal, a failed shadow release would be
// indistinguishable from a normal rejection and remediation would trigger a
// fresh heal attempt on the release meant to verify one, looping forever.
func TestHandleValidationResult_Shadow_Rejected_CarriesShadowTrue(t *testing.T) {
	deps, store := seedToValidatingShadow(t, "rShadowReject")
	seedValidationNodes(t, deps, "rShadowReject", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/b.log"},
	})

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rShadowReject",
		AggregateStatus: "failed",
	})
	require.NoError(t, err)

	r, err := store.GetRelease("rShadowReject")
	require.NoError(t, err)
	assert.Equal(t, release.StatusRejected, r.Status())

	entry := outboxEntries(store)[len(outboxEntries(store))-1]
	assert.Equal(t, streams.ReleaseRejectedV1, entry.StreamName)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, true, payload["shadow"],
		"a shadow release's validation_failed rejection must carry shadow:true")
}

// seedToValidatingWithURIs is like seedToValidating but uses a two-node topology
// where each node carries a CandidateArtifactURI so the rejected payload enrichment
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
			NodeType: "dbt-model", OriginalFilePath: "models/a.sql",
			CandidateArtifactURI: "s3://continuo/svc-a/" + releaseID + "/candidate_a.sql"},
		{UniqueID: "b", ServiceName: "svc-a", UpstreamUniqueIDs: []string{"a"},
			NodeType: "python-model", OriginalFilePath: "python/b.py",
			CandidateArtifactURI: "s3://continuo/svc-a/" + releaseID + "/candidate_b.json"},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:     releaseID,
		Status:        "ok",
		CodeBundleURI: "s3://continuo/code-bundles/" + releaseID + "/bundle.json",
		Topology:      topo,
	}))
	return deps, store
}

// TestHandleValidationResult_Rejected_CarriesCandidateArtifactURIAndProvenance asserts
// that the release.rejected:v1 outbox payload emitted for a validation failure
// carries:
//   - per_node[*].candidate_artifact_uri  — S3 URI pointer to the candidate artifact for each node
//   - top-level "repo", "commit_sha", and "code_bundle_uri" — provenance fields from the release aggregate
//
// This allows the consumer to fetch the exact artifact that was validated when
// investigating a failure without inlining it into the event.
func TestHandleValidationResult_Rejected_CarriesCandidateArtifactURIAndProvenance(t *testing.T) {
	deps, store := seedToValidatingWithURIs(t, "rA")
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/rA/b.log", RunResultsURI: "run-results/rA/b.json"},
	})

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
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

	var codeBundleURI string
	require.NoError(t, json.Unmarshal(topLevel["code_bundle_uri"], &codeBundleURI))
	assert.Equal(t, "s3://continuo/code-bundles/rA/bundle.json", codeBundleURI,
		"top-level code_bundle_uri must come from the release aggregate")

	// Decode per_node and check candidate_artifact_uri per entry
	var perNode []struct {
		NodeID               string `json:"node_id"`
		Status               string `json:"status"`
		DBTLogURI            string `json:"dbt_log_uri,omitempty"`
		RunResultsURI        string `json:"run_results_uri,omitempty"`
		CandidateArtifactURI string `json:"candidate_artifact_uri,omitempty"`
	}
	require.NoError(t, json.Unmarshal(topLevel["per_node"], &perNode))
	require.Len(t, perNode, 2)

	byID := map[string]string{}
	runResultsByID := map[string]string{}
	for _, pn := range perNode {
		byID[pn.NodeID] = pn.CandidateArtifactURI
		runResultsByID[pn.NodeID] = pn.RunResultsURI
	}
	assert.Equal(t, "s3://continuo/svc-a/rA/candidate_a.sql", byID["a"],
		"ok nodes must also carry candidate_artifact_uri (pointer, not inline content)")
	assert.Equal(t, "s3://continuo/svc-a/rA/candidate_b.json", byID["b"],
		"failing node must carry candidate_artifact_uri")
	assert.Equal(t, "run-results/rA/b.json", runResultsByID["b"],
		"failing node must carry run_results_uri through to release.rejected:v1")
}

// TestHandleValidationResult_Rejected_CarriesCandidateNodeTypeAndLocation asserts
// that each per_node entry of a validation rejection carries the candidate
// topology's own node_type, file_path, and service for that node.
//
// These come from the candidate topology rather than the promoted graph because
// a rejected release is never promoted: the promoted topology holds nothing for
// a newly-added node and the PREVIOUS release's path for a node whose candidate
// moved it. node_type additionally lets the remediation agent recognise a python
// node — whose candidate artifact is a JSON validation spec, not SQL — and skip
// it before reading anything.
func TestHandleValidationResult_Rejected_CarriesCandidateNodeTypeAndLocation(t *testing.T) {
	deps, store := seedToValidatingWithURIs(t, "rLoc")
	seedValidationNodes(t, deps, "rLoc", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/rLoc/b.log"},
	})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rLoc",
		AggregateStatus: "failed",
	}))

	entries := outboxEntries(store)
	require.Len(t, entries, 4)
	var topLevel map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entries[3].Payload, &topLevel))

	var perNode []struct {
		NodeID   string `json:"node_id"`
		NodeType string `json:"node_type,omitempty"`
		FilePath string `json:"file_path,omitempty"`
		Service  string `json:"service,omitempty"`
	}
	require.NoError(t, json.Unmarshal(topLevel["per_node"], &perNode))
	require.Len(t, perNode, 2)

	byID := map[string]struct{ nodeType, filePath, service string }{}
	for _, pn := range perNode {
		byID[pn.NodeID] = struct{ nodeType, filePath, service string }{pn.NodeType, pn.FilePath, pn.Service}
	}
	assert.Equal(t, "dbt-model", byID["a"].nodeType)
	assert.Equal(t, "models/a.sql", byID["a"].filePath)
	assert.Equal(t, "svc-a", byID["a"].service)
	assert.Equal(t, "python-model", byID["b"].nodeType,
		"a python node's rejection must name its kind so the agent can skip it")
	assert.Equal(t, "python/b.py", byID["b"].filePath)
	assert.Equal(t, "svc-a", byID["b"].service)
}

// TestHandleValidationResult_AggregateStatusFailed_Rejects covers the edge case
// where every reported node passed but the aggregate_status is not "ok".
// Without the audit-trail fix the rejected event would carry an empty
// failing_nodes slice and no signal about why the release was rejected;
// this test guards that the outbox payload preserves aggregate_status so
// operators can diagnose the rejection.
func TestHandleValidationResult_AggregateStatusFailed_Rejects(t *testing.T) {
	deps, store := seedToValidating(t, "rA")
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "ok"},
	})

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
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
}

// promotedPayload is the JSON shape released into release.promoted:v1.
type promotedPayload struct {
	ReleaseID     string             `json:"release_id"`
	Repo          string             `json:"repo"`
	CommitSHA     string             `json:"commit_sha"`
	PromotedAt    time.Time          `json:"promoted_at"`
	Topology      []promotedNodeWire `json:"topology"`
	CodeBundleURI string             `json:"code_bundle_uri"`
	Bootstrap     bool               `json:"bootstrap"`
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

	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "ok"},
	})
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
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
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "ok"},
	})

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
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
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{{NodeID: "a", Status: "ok"}})
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA", AggregateStatus: "ok",
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
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{{NodeID: "a", Status: "ok"}})
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA", AggregateStatus: "ok",
	}))

	entries := outboxEntries(store)
	last := entries[len(entries)-1]
	require.Equal(t, streams.ReleasePromotedV1, last.StreamName)
	var p promotedPayload
	require.NoError(t, json.Unmarshal(last.Payload, &p))
	require.Len(t, p.Topology, 1)
	assert.Equal(t, 3, p.Topology[0].TestCount, "release.promoted:v1 must carry per-node test_count")
}

// TestHandleValidationResult_Rejected_CarriesChangedAncestorIDs verifies that
// each failing node's rejected per_node entry names its transitive candidate
// ancestors whose content changed against production. The seeded topology is
// rewired so "b" depends on "a", and current_prod is seeded with a stale hash
// for "a" so it counts as changed; "b" then fails validation and must carry
// "a" as its changed_ancestor_ids, while "a" itself (which has no ancestors)
// must carry none.
func TestHandleValidationResult_Rejected_CarriesChangedAncestorIDs(t *testing.T) {
	deps, store := seedToValidatingWithURIs(t, "rAnc")
	// Rewire the seeded candidate topology so b depends on a, and record a
	// current-prod snapshot where a's content differs: a is the changed
	// ancestor of the failing b.
	r, err := store.GetRelease("rAnc")
	require.NoError(t, err)
	topo := r.CandidateTopology()
	var bHash string
	for i := range topo {
		if topo[i].UniqueID == "b" {
			topo[i].UpstreamUniqueIDs = []string{"a"}
			bHash = topo[i].ContentHash
		}
	}
	// The release is already in Validating (seedToValidatingWithURIs drives it
	// there), so re-transitioning is refused by the state machine; rehydrate a
	// new aggregate carrying every persisted field, with the rewired topology.
	r = release.Rehydrate(release.RehydrateInput{
		ID:                r.ID(),
		Status:            r.Status(),
		ImageTags:         r.ImageTags(),
		ChangedService:    r.ChangedService(),
		CandidateTopology: topo,
		ValidationNodeIDs: r.ValidationNodeIDs(),
		PerNodeResults:    r.PerNodeResults(),
		RejectReason:      r.RejectReason(),
		RejectDetail:      r.RejectDetail(),
		FailingNodes:      r.FailingNodes(),
		CreatedAt:         r.CreatedAt(),
		Transitions:       r.Transitions(),
		Bootstrap:         r.IsBootstrap(),
		Shadow:            r.IsShadow(),
		Repo:              r.Repo(),
		CommitSHA:         r.CommitSHA(),
		CodeBundleURI:     r.CodeBundleURI(),
		ManifestKind:      r.ManifestKind(),
		RemediationRound:  r.RemediationRound(),
		RejectionPayload:  r.RejectionPayload(),
	})
	store.SeedRelease(r)
	cp := release.NewCurrentProd()
	cp.Update("prev", release.Topology{
		{UniqueID: "a", ContentHash: "old-hash-for-a"},
		{UniqueID: "b", ContentHash: bHash},
	}, deps.Clock.Now())
	store.SeedCurrentProd(cp)
	seedValidationNodes(t, deps, "rAnc", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/rAnc/b.log"},
	})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rAnc", AggregateStatus: "failed",
	}))

	entries := outboxEntries(store)
	rej := entries[len(entries)-1]
	require.Equal(t, streams.ReleaseRejectedV1, rej.StreamName)
	var payload struct {
		PerNode []struct {
			NodeID             string   `json:"node_id"`
			ChangedAncestorIDs []string `json:"changed_ancestor_ids"`
		} `json:"per_node"`
	}
	require.NoError(t, json.Unmarshal(rej.Payload, &payload))
	byID := map[string][]string{}
	for _, pn := range payload.PerNode {
		byID[pn.NodeID] = pn.ChangedAncestorIDs
	}
	assert.Equal(t, []string{"a"}, byID["b"], "b's changed ancestor is a")
	assert.Empty(t, byID["a"], "a has no changed ancestors")
}

// TestHandleValidationResult_Promote_EmitsCodeBundleURIAndBootstrap verifies
// that on the normal validation-pass promotion path (HandleValidationResult ->
// promoteToProduction), release.promoted:v1 carries the release's
// code_bundle_uri (persisted at parse time by handleParseOK from
// topology-controller's manifest.loaded.candidate:v1) and bootstrap=false for a
// non-bootstrap release.
func TestHandleValidationResult_Promote_EmitsCodeBundleURIAndBootstrap(t *testing.T) {
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
		ReleaseID:     "rA",
		Status:        "ok",
		CodeBundleURI: "s3://continuo/code-bundles/rA/bundle.json",
		Topology: release.Topology{
			{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
		},
	}))
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{{NodeID: "a", Status: "ok"}})
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA", AggregateStatus: "ok",
	}))

	entries := outboxEntries(store)
	last := entries[len(entries)-1]
	require.Equal(t, streams.ReleasePromotedV1, last.StreamName)
	var p promotedPayload
	require.NoError(t, json.Unmarshal(last.Payload, &p))
	assert.Equal(t, "s3://continuo/code-bundles/rA/bundle.json", p.CodeBundleURI,
		"release.promoted:v1 must carry code_bundle_uri on the validation-pass path")
	assert.False(t, p.Bootstrap, "non-bootstrap release must carry bootstrap=false")
}
