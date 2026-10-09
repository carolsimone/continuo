package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
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
		ReleaseID:   releaseID,
		Status:      "ok",
		TopologyRef: putTopology(t, deps, releaseID, topo),
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
	assert.Equal(t, pipeline.StatusPromoted, r.Status(),
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
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.FailReason())
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
	assert.Equal(t, pipeline.StatusPromoted, r.Status())
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
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Equal(t, []string{"a"}, r.FailingNodes())

	e := findEntry(t, store, streams.ReleaseRejectedV1)
	assert.JSONEq(t, string(e.Payload), string(r.RejectionPayload()),
		"the rejection payload stored on the release must match the one emitted on release.rejected:v1")
	assert.Equal(t, "rejected", outcomeOf(t, findEntry(t, store, streams.PipelineRunFinishedV1)))
}

// TestHandleValidationResult_SkippedNodeInStore_Rejects verifies that a per-node
// result with status "skipped" (emitted for a node whose upstream failed
// validation, so it never ran) is present in the store and counts as failing:
// the release rejects and names the skipped node. This is the read-model side of
// the execution-controller emitting a skip projection for every node its failure
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
	assert.Equal(t, pipeline.StatusRejected, r.Status())
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
	assert.Equal(t, pipeline.StatusPromoted, r.Status())

	cp := store.GetCurrentProd()
	assert.Equal(t, "rA", cp.ReleaseID())

	entries := outboxEntries(store)
	require.Len(t, entries, 5) // CompileRequested + ReleaseRequested + ValidationRequested + ReleasePromoted + PipelineRunFinished

	third := entries[3]
	assert.Equal(t, streams.ReleasePromotedV2, third.StreamName)
	assert.Equal(t, "promoted", outcomeOf(t, entries[4]))

	// Assert the changed service's service_prod row carries the correct values.
	sp := store.GetServiceProd("svc-a")
	require.NotNil(t, sp)
	assert.Equal(t, "rA", sp.ReleaseID())
	assert.Equal(t, "s3://continuo/svc-a/rA/manifest.json", sp.ManifestS3Key())
	assert.Equal(t, "sha-a", sp.ImageTag())
}

// seedToValidatingVerification mirrors seedToValidating but seeds a
// verification run, so promotion tests can assert a verification run stops
// at StatusPassed instead of reaching current_prod.
func seedToValidatingVerification(t *testing.T, releaseID string) (*handlers.Deps, *fakeStore) {
	t.Helper()
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	store.SeedRelease(pipeline.NewVerification(releaseID, "svc-a", "sha-a", "", 1, "", release.ManifestKindDbt, deps.Clock.Now()))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: releaseID, Status: "ok",
	}))

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-a", UpstreamUniqueIDs: []string{"a"}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:   releaseID,
		Status:      "ok",
		TopologyRef: putTopology(t, deps, releaseID, topo),
	}))
	return deps, store
}

// TestHandleValidationResult_Verification_StopsAtPassed_NeverPromotes drives
// HandleValidationResult with a verification run in Validating and an all-ok
// aggregate. The verification run must stop at StatusPassed: no
// release.promoted:v2 outbox row, current_prod's Upsert never called, and the
// promoted-telemetry span never fires. A control on the identical flow with a
// candidate must still promote exactly as today, proving the gate is
// specific to the run's kind rather than a general regression.
func TestHandleValidationResult_Verification_StopsAtPassed_NeverPromotes(t *testing.T) {
	t.Run("verification run stops at passed and never promotes", func(t *testing.T) {
		deps, store := seedToValidatingVerification(t, "rVerify")
		spy := &spyTelemetry{}
		deps.Telemetry = spy
		seedValidationNodes(t, deps, "rVerify", []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "ok"},
		})

		err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
			ReleaseID:       "rVerify",
			AggregateStatus: "ok",
		})
		require.NoError(t, err)

		r, err := store.GetRelease("rVerify")
		require.NoError(t, err)
		assert.Equal(t, pipeline.StatusPassed, r.Status(),
			"a verification run must stop at passed, not promoted")

		entries := outboxEntries(store)
		for _, e := range entries {
			assert.NotEqual(t, streams.ReleasePromotedV2, e.StreamName,
				"a verification run must never emit release.promoted:v2")
		}

		assert.Equal(t, 0, store.CurrentProdUpsertCalls(),
			"a verification run must never call CurrentProdRepo.Upsert")
		cp := store.GetCurrentProd()
		assert.Equal(t, "", cp.ReleaseID(), "current_prod must remain untouched by a verification run")

		assert.Nil(t, store.GetServiceProd("svc-a"),
			"a verification run must never upsert service_prod")

		assert.Equal(t, 0, spy.releasePromotedCalls,
			"a verification run must never fire the promoted telemetry span")
	})

	t.Run("control: a candidate on the identical flow still promotes", func(t *testing.T) {
		deps, store := seedToValidating(t, "rCandidateControl")
		seedValidationNodes(t, deps, "rCandidateControl", []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "ok"},
		})

		err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
			ReleaseID:       "rCandidateControl",
			AggregateStatus: "ok",
		})
		require.NoError(t, err)

		r, err := store.GetRelease("rCandidateControl")
		require.NoError(t, err)
		assert.Equal(t, pipeline.StatusPromoted, r.Status())

		cp := store.GetCurrentProd()
		assert.Equal(t, "rCandidateControl", cp.ReleaseID())

		entries := outboxEntries(store)
		var sawPromoted bool
		for _, e := range entries {
			if e.StreamName == streams.ReleasePromotedV2 {
				sawPromoted = true
			}
		}
		assert.True(t, sawPromoted, "a candidate release must still emit release.promoted:v2")
	})
}

// TestHandleValidationResult_VerificationPasses_NoReleaseEvents_FinishedEmitted
// drives a verification run straight to passed and asserts the exact
// pipeline.run.finished:v1 payload contract: a verification's pass emits
// neither release.promoted:v2 nor release.rejected:v1, but does emit exactly
// one pipeline.run.finished:v1 whose run_kind, outcome, verifies_release_id,
// and candidate_schema describe this verification.
func TestHandleValidationResult_VerificationPasses_NoReleaseEvents_FinishedEmitted(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, store := newDeps(now)
	r := pipeline.NewVerification("verify-rel-1-core-a1", "core", "img", "rel-1", 1, "", release.ManifestKindDbt, now)
	require.NoError(t, r.TransitionToParsing(now))
	require.NoError(t, r.TransitionToValidating(store.storeTopology(r.ID(), release.Topology{{UniqueID: "model.core.orders", ServiceName: "core"}}), []string{"model.core.orders"}, now))
	r.UpsertStageResult("validation", pipeline.NodeValidationResult{NodeID: "model.core.orders", Status: "ok"})
	store.SeedRelease(r)

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{ReleaseID: r.ID(), AggregateStatus: "ok"}))

	got, _ := store.GetRelease(r.ID())
	assert.Equal(t, pipeline.StatusPassed, got.Status())
	assert.Equal(t, 0, store.CurrentProdUpsertCalls(), "a verification never writes current_prod")
	assert.Nil(t, store.GetServiceProd("core"), "a verification never writes service_prod")
	streamsEmitted := map[string]int{}
	for _, e := range store.entries {
		streamsEmitted[e.StreamName]++
	}
	assert.Equal(t, 0, streamsEmitted[streams.ReleasePromotedV2])
	assert.Equal(t, 0, streamsEmitted[streams.ReleaseRejectedV1])
	assert.Equal(t, 1, streamsEmitted[streams.PipelineRunFinishedV1])
	var payload map[string]any
	require.NoError(t, json.Unmarshal(lastEntryOn(store, streams.PipelineRunFinishedV1).Payload, &payload))
	assert.Equal(t, "verification", payload["run_kind"])
	assert.Equal(t, "passed", payload["outcome"])
	assert.Equal(t, "rel-1", payload["verifies_release_id"])
	assert.Equal(t, handlers.CandidateSchemaFor(r.ID()), payload["candidate_schema"])
}

// TestHandleValidationResult_VerificationFails_NoReleaseRejected_FinishedEmitted
// is the failure-side counterpart: a verification's validation failure ends
// the run at Failed, stores no rejection payload, emits no
// release.rejected:v1, and emits pipeline.run.finished:v1 with outcome
// "failed".
func TestHandleValidationResult_VerificationFails_NoReleaseRejected_FinishedEmitted(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, store := newDeps(now)
	r := pipeline.NewVerification("verify-rel-1-core-a1", "core", "img", "rel-1", 1, "", release.ManifestKindDbt, now)
	require.NoError(t, r.TransitionToParsing(now))
	require.NoError(t, r.TransitionToValidating(store.storeTopology(r.ID(), release.Topology{{UniqueID: "model.core.orders", ServiceName: "core"}}), []string{"model.core.orders"}, now))
	r.UpsertStageResult("validation", pipeline.NodeValidationResult{NodeID: "model.core.orders", Status: "failed"})
	store.SeedRelease(r)

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{ReleaseID: r.ID(), AggregateStatus: "failed"}))

	got, _ := store.GetRelease(r.ID())
	assert.Equal(t, pipeline.StatusFailed, got.Status())
	assert.Equal(t, []string{"model.core.orders"}, got.FailingNodes())
	assert.Nil(t, got.RejectionPayload(), "a verification stores no rejection payload")
	for _, e := range store.entries {
		assert.NotEqual(t, streams.ReleaseRejectedV1, e.StreamName, "a verification's failure is not a release rejection")
	}
	assert.Equal(t, "failed", outcomeOf(t, lastEntryOn(store, streams.PipelineRunFinishedV1)))
}

// TestHandleValidationResult_CandidatePromoted_EmitsFinishedToo is the
// control: a candidate's promotion still emits release.promoted:v2, and now
// also emits pipeline.run.finished:v1 with outcome "promoted".
func TestHandleValidationResult_CandidatePromoted_EmitsFinishedToo(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, store := newDeps(now)
	r := pipeline.NewCandidate("rel-1", "core", "img", false, "org/r", "sha", release.ManifestKindDbt, now)
	require.NoError(t, r.TransitionToParsing(now))
	require.NoError(t, r.TransitionToValidating(store.storeTopology(r.ID(), release.Topology{{UniqueID: "model.core.orders", ServiceName: "core"}}), []string{"model.core.orders"}, now))
	r.UpsertStageResult("validation", pipeline.NodeValidationResult{NodeID: "model.core.orders", Status: "ok"})
	store.SeedRelease(r)

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{ReleaseID: r.ID(), AggregateStatus: "ok"}))

	assert.Equal(t, "promoted", outcomeOf(t, lastEntryOn(store, streams.PipelineRunFinishedV1)))
	assert.NotNil(t, lastEntryOn(store, streams.ReleasePromotedV2))
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
		ReleaseID:   releaseID,
		Status:      "ok",
		TopologyRef: putTopology(t, deps, releaseID, topo),
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
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.FailReason())
	assert.Equal(t, []string{"b"}, r.FailingNodes())

	// CurrentProd must remain empty since validation failed.
	cp := store.GetCurrentProd()
	assert.Equal(t, "", cp.ReleaseID())

	entries := outboxEntries(store)
	require.Len(t, entries, 5) // CompileRequested + ReleaseRequested + ValidationRequested + ReleaseRejected + PipelineRunFinished

	third := entries[3]
	assert.Equal(t, streams.ReleaseRejectedV1, third.StreamName)

	// The outbox payload must carry stage="validation" so consumers can distinguish
	// compile-leg rejections from validation-leg rejections.
	var topLevel map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(third.Payload, &topLevel))
	var stage string
	require.NoError(t, json.Unmarshal(topLevel["stage"], &stage))
	assert.Equal(t, "validation", stage)

	assert.Equal(t, "rejected", outcomeOf(t, entries[4]))
}

// TestHandleValidationResult_Verification_Failed_NoReleaseRejected_FinishedEmitted
// verifies that a verification run's validation_failed ends the run at
// Failed and emits no release.rejected:v1 at all (a verification failure is
// never a release rejection — Global Constraint). This is the case
// agent-remediation's fix-verification loop hinges on: without it, a failed
// verification run would be indistinguishable from a rejected candidate and
// remediation would trigger a fresh heal attempt on the run meant to verify
// one, looping forever. The failure travels on pipeline.run.finished:v1
// alone.
func TestHandleValidationResult_Verification_Failed_NoReleaseRejected_FinishedEmitted(t *testing.T) {
	deps, store := seedToValidatingVerification(t, "rVerifyReject")
	seedValidationNodes(t, deps, "rVerifyReject", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/b.log"},
	})

	err := handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rVerifyReject",
		AggregateStatus: "failed",
	})
	require.NoError(t, err)

	r, err := store.GetRelease("rVerifyReject")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusFailed, r.Status())
	assert.Nil(t, r.RejectionPayload(), "a verification stores no rejection payload")

	for _, e := range outboxEntries(store) {
		assert.NotEqual(t, streams.ReleaseRejectedV1, e.StreamName,
			"a verification run's validation failure must not be reported as a release rejection")
	}
	assert.Equal(t, "failed", outcomeOf(t, findEntry(t, store, streams.PipelineRunFinishedV1)))
}

// seedToValidatingWithURIs is like seedToValidating but uses a two-node topology
// where the nodes' candidate artifact URIs are derived (one .sql, one .json) so the rejected payload enrichment
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
			NodeType: "dbt-model", OriginalFilePath: "models/a.sql", ImageTag: "sha-a"},
		{UniqueID: "b", ServiceName: "svc-a", UpstreamUniqueIDs: []string{"a"},
			NodeType: "python-node", OriginalFilePath: "python/b.py", ImageTag: "sha-a"},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:     releaseID,
		Status:        "ok",
		CodeBundleURI: "s3://continuo/code-bundles/" + releaseID + "/bundle.json",
		TopologyRef:   putTopology(t, deps, releaseID, topo),
	}))
	return deps, store
}

// TestHandleValidationResult_Rejected_CarriesCandidateArtifactURIAndProvenance asserts
// that the release.rejected:v1 outbox payload emitted for a validation failure
// carries:
//   - per_node[*].candidate_artifact_uri  — S3 URI pointer to the candidate artifact for each node
//   - top-level "repo", "commit_sha", and "code_bundle_uri" — provenance fields from the release aggregate
//
// This allows the consumer to fetch the exact artifact that was checked when
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
	assert.Equal(t, pipeline.StatusRejected, r.Status())

	entries := outboxEntries(store)
	require.Len(t, entries, 5) // CompileRequested + ReleaseRequested + ValidationRequested + ReleaseRejected + PipelineRunFinished
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
	assert.Equal(t, "s3://continuo/candidate-sql/rA/candidate_a.sql", byID["a"],
		"ok nodes must also carry candidate_artifact_uri (pointer, not inline content)")
	assert.Equal(t, "s3://continuo/candidate-sql/rA/candidate_b.json", byID["b"],
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
	require.Len(t, entries, 5)
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
	assert.Equal(t, "python-node", byID["b"].nodeType,
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
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.FailReason())
	assert.Empty(t, r.FailingNodes(), "no explicitly-failed or missing nodes when only aggregate is bad")

	entries := outboxEntries(store)
	require.Len(t, entries, 5)
	var payload rejectedPayload
	require.NoError(t, json.Unmarshal(entries[3].Payload, &payload))
	assert.Empty(t, payload.FailingNodes)
	assert.Empty(t, payload.MissingNodes)
	assert.Equal(t, "partial_failed", payload.AggregateStatus,
		"aggregate_status must be surfaced so operators can diagnose a rejection with no per-node signal")
}

// TestHandleValidationResult_Promote_StampsChangedAndProvenance verifies that a
// promotion emits release-level provenance (repo, commit_sha, promoted_at) and
// flags exactly the nodes whose content_hash differs from the prior prod: node
// "b" changed (hash differs), node "a" unchanged (hash matches). Node "a" is
// checked because it is "b"'s ancestor, but it must carry changed=false.
func TestHandleValidationResult_Promote_StampsChangedAndProvenance(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	// Prior prod: a@"h", b@"old". Both in svc-a; b depends on a.
	seedProd(t, deps, store, "r0", release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-a", ContentHash: "old", UpstreamUniqueIDs: []string{"a"}},
	}, time.Unix(50, 0).UTC())

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
		TopologyRef: putTopology(t, deps, "rA", release.Topology{
			{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h", UpstreamUniqueIDs: []string{}},
			{UniqueID: "b", ServiceName: "svc-a", ContentHash: "new", UpstreamUniqueIDs: []string{"a"}},
		}),
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
	require.Len(t, entries, 5) // CompileRequested + ReleaseRequested + ValidationRequested + ReleasePromoted + PipelineRunFinished
	promotedEntry := entries[3]

	p := promotedEvent(t, promotedEntry)
	assert.Equal(t, "acme/demo", p.Repo)
	assert.Equal(t, "deadbeef", p.CommitSHA)
	assert.Equal(t, time.Unix(100, 0).UTC(), p.PromotedAt.UTC())
	assert.Equal(t, []string{"b"}, p.ChangedNodeIDs, "b changed (hash differs from prior prod); a is unchanged")
}

// TestHandleValidationResult_Promote_TestNeverChangedButKeptInCurrentProd
// verifies that a dbt-test node in the candidate topology is never announced
// in release.promoted:v2's changed_node_ids — the orchestrator only ever draws
// and schedules relations, never a test — while current_prod's own artifact
// still holds it, so an unchanged test is not re-checked by a future
// release.
func TestHandleValidationResult_Promote_TestNeverChangedButKeptInCurrentProd(t *testing.T) {
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
		TopologyRef: putTopology(t, deps, "rA", release.Topology{
			{UniqueID: "a", ServiceName: "svc-a", NodeType: "dbt-model", UpstreamUniqueIDs: []string{}},
			{UniqueID: "test.p.not_null_a_id.1", ServiceName: "svc-a", NodeType: "dbt-test", UpstreamUniqueIDs: []string{"a"}},
		}),
	}))
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "test.p.not_null_a_id.1", Status: "ok"},
	})
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA", AggregateStatus: "ok",
	}))

	p := promotedEvent(t, findEntry(t, store, streams.ReleasePromotedV2))
	assert.Equal(t, []string{"a"}, p.ChangedNodeIDs, "a test is never announced as changed")

	cp := store.GetCurrentProd()
	prodTopo, err := deps.Topologies.Load(context.Background(), cp.Topology())
	require.NoError(t, err)
	keptIDs := map[string]bool{}
	for _, n := range prodTopo {
		keptIDs[n.UniqueID] = true
	}
	assert.True(t, keptIDs["test.p.not_null_a_id.1"],
		"current_prod's artifact keeps the test so an unchanged test is not re-checked next release")
	assert.True(t, keptIDs["a"])
}

// TestHandleValidationResult_Promote_CarriesCandidateSchema verifies that the
// release.promoted:v2 payload includes candidate_schema so the execution-controller's
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

	promotedEntry := findEntry(t, store, streams.ReleasePromotedV2)

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(promotedEntry.Payload, &payload))

	var candidateSchema string
	require.NoError(t, json.Unmarshal(payload["candidate_schema"], &candidateSchema))
	assert.Equal(t, "_candidate_rA", candidateSchema,
		"release.promoted:v2 must carry candidate_schema for executor teardown")
}

// TestHandleValidationResult_Rejected_CarriesChangedAncestors verifies that
// each failing node's rejected per_node entry names its transitive candidate
// ancestors whose content changed against production, each with the location
// THIS candidate declares for it. The seeded topology is rewired so "b" depends
// on "a", and current_prod is seeded with a stale hash for "a" so it counts as
// changed; "b" then fails validation and must carry "a" — path and service
// included — as its changed_ancestors, while "a" itself (which has no
// ancestors) must carry none.
func TestHandleValidationResult_Rejected_CarriesChangedAncestors(t *testing.T) {
	deps, store := seedToValidatingWithURIs(t, "rAnc")
	// Rewire the seeded candidate topology so b depends on a, and record a
	// current-prod snapshot where a's content differs: a is the changed
	// ancestor of the failing b.
	r, err := store.GetRelease("rAnc")
	require.NoError(t, err)
	topo, err := deps.Topologies.Load(context.Background(), r.CandidateTopologyRef())
	require.NoError(t, err)
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
	r = pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:                r.ID(),
		Kind:              r.Kind(),
		Status:            r.Status(),
		ImageTags:         r.ImageTags(),
		ChangedService:    r.ChangedService(),
		CandidateTopology: store.storeTopology(r.ID(), topo),
		ValidationNodeIDs: r.ValidationNodeIDs(),
		PerNodeResults:    r.PerNodeResults(),
		FailReason:        r.FailReason(),
		FailDetail:        r.FailDetail(),
		FailingNodes:      r.FailingNodes(),
		CreatedAt:         r.CreatedAt(),
		Transitions:       r.Transitions(),
		Bootstrap:         r.IsBootstrap(),
		Repo:              r.Repo(),
		CommitSHA:         r.CommitSHA(),
		CodeBundleURI:     r.CodeBundleURI(),
		ManifestKind:      r.ManifestKind(),
		RemediationRound:  r.RemediationRound(),
		RejectionPayload:  r.RejectionPayload(),
	})
	store.SeedRelease(r)
	seedProd(t, deps, store, "prev", release.Topology{
		{UniqueID: "a", ContentHash: "old-hash-for-a"},
		{UniqueID: "b", ContentHash: bHash},
	}, deps.Clock.Now())
	seedValidationNodes(t, deps, "rAnc", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/rAnc/b.log"},
	})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rAnc", AggregateStatus: "failed",
	}))

	rej := findEntry(t, store, streams.ReleaseRejectedV1)
	var payload struct {
		PerNode []struct {
			NodeID           string `json:"node_id"`
			ChangedAncestors []struct {
				NodeID   string `json:"node_id"`
				FilePath string `json:"file_path"`
				Service  string `json:"service"`
				Depth    int    `json:"depth"`
			} `json:"changed_ancestors"`
		} `json:"per_node"`
	}
	require.NoError(t, json.Unmarshal(rej.Payload, &payload))
	byID := map[string][]struct {
		NodeID   string `json:"node_id"`
		FilePath string `json:"file_path"`
		Service  string `json:"service"`
		Depth    int    `json:"depth"`
	}{}
	for _, pn := range payload.PerNode {
		byID[pn.NodeID] = pn.ChangedAncestors
	}
	require.Len(t, byID["b"], 1, "b's changed ancestor is a")
	assert.Equal(t, "a", byID["b"][0].NodeID)
	assert.Equal(t, "models/a.sql", byID["b"][0].FilePath,
		"the ancestor carries the path THIS candidate declares, which is the file a fix must edit")
	assert.Equal(t, "svc-a", byID["b"][0].Service)
	assert.Equal(t, 1, byID["b"][0].Depth, "a is b's direct upstream")
	assert.Empty(t, byID["a"], "a has no changed ancestors")
}

// TestHandleValidationResult_Promote_EmitsCodeBundleURIAndBootstrap verifies
// that on the normal validation-pass promotion path (HandleValidationResult ->
// promoteToProduction), release.promoted:v2 carries the release's
// code_bundle_uri (persisted at parse time by handleParseOK from
// topology-controller's manifest.loaded.candidate:v2) and bootstrap=false for a
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
		TopologyRef: putTopology(t, deps, "rA", release.Topology{
			{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
		}),
	}))
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{{NodeID: "a", Status: "ok"}})
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA", AggregateStatus: "ok",
	}))

	last := findEntry(t, store, streams.ReleasePromotedV2)
	p := promotedEvent(t, last)
	assert.Equal(t, "s3://continuo/code-bundles/rA/bundle.json", p.CodeBundleURI,
		"release.promoted:v2 must carry code_bundle_uri on the validation-pass path")
	assert.False(t, p.Bootstrap, "non-bootstrap release must carry bootstrap=false")
}

// A later read of the run's topology (after intake verified the artifact) that
// meets a corrupt artifact surfaces ErrTopologyArtifactCorrupt, which the
// bindings dead-letter. The error aborts the handler, so its transaction
// rolls back.
func TestHandleValidationResult_CorruptArtifactOnTheDecisionReadSurfacesTheCorruption(t *testing.T) {
	for _, status := range []string{"ok", "failed"} {
		t.Run(status, func(t *testing.T) {
			deps, store := seedToValidating(t, "rCorrupt")
			r, err := store.GetRelease("rCorrupt")
			require.NoError(t, err)
			store.topologies.failLoad(r.CandidateTopologyRef().URI, fmt.Errorf("%w: tampered", ports.ErrTopologyArtifactCorrupt))

			err = handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
				ReleaseID: "rCorrupt", AggregateStatus: status,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, ports.ErrTopologyArtifactCorrupt))
		})
	}
}

// The per-node projection reads the topology to stamp the node type; a corrupt
// artifact there is surfaced the same way.
func TestHandleNodeValidationResult_CorruptArtifactSurfacesTheCorruption(t *testing.T) {
	deps, store := seedToValidating(t, "rNodeCorrupt")
	r, err := store.GetRelease("rNodeCorrupt")
	require.NoError(t, err)
	store.topologies.failLoad(r.CandidateTopologyRef().URI, fmt.Errorf("%w: tampered", ports.ErrTopologyArtifactCorrupt))

	err = handlers.HandleNodeValidationResult(context.Background(), deps, handlers.NodeValidationResultInput{
		ReleaseID: "rNodeCorrupt", Stage: "validation", NodeID: "a", Status: "ok",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ports.ErrTopologyArtifactCorrupt))
}

func nodeIDs(topo release.Topology) []string {
	ids := make([]string, len(topo))
	for i, n := range topo {
		ids[i] = n.UniqueID
	}
	return ids
}

// TestPromoteToProduction_CurrentProdTakesTheArtifactTopology pins that a
// promotion writes current_prod from the run's artifact — dbt-test nodes
// included, so an unchanged test is not re-checked next release — and
// announces on release.promoted:v2 the artifact's reference together with the
// non-test nodes that changed against the previous prod.
func TestPromoteToProduction_CurrentProdTakesTheArtifactTopology(t *testing.T) {
	deps, store := seedToParsing(t, "rArt", map[string]string{"svc-a": "sha-a"})
	seedProd(t, deps, store, "prev", release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", NodeType: "dbt-model", ContentHash: "h_a"},
	}, time.Unix(50, 0).UTC())
	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", NodeType: "dbt-model", ContentHash: "h_a", ImageTag: "sha-a", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-a", NodeType: "dbt-model", ContentHash: "h_b", ImageTag: "sha-a", UpstreamUniqueIDs: []string{"a"}},
		{UniqueID: "test.b", ServiceName: "svc-a", NodeType: "dbt-test", ImageTag: "sha-a", UpstreamUniqueIDs: []string{"b"}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rArt", Status: "ok", TopologyRef: putTopology(t, deps, "rArt", topo),
	}))
	r, err := store.GetRelease("rArt")
	require.NoError(t, err)
	results := make([]handlers.NodeResult, 0, len(r.ValidationNodeIDs()))
	for _, id := range r.ValidationNodeIDs() {
		results = append(results, handlers.NodeResult{NodeID: id, Status: "ok"})
	}
	seedValidationNodes(t, deps, "rArt", results)
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rArt", AggregateStatus: "ok",
	}))

	cp := store.GetCurrentProd()
	assert.Equal(t, "rArt", cp.ReleaseID())
	prodTopo, err := deps.Topologies.Load(context.Background(), cp.Topology())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b", "test.b"}, nodeIDs(prodTopo), "the artifact current_prod points at keeps the test node")

	p := promotedEvent(t, findEntry(t, store, streams.ReleasePromotedV2))
	assert.Equal(t, cp.Topology().URI, p.TopologyURI)
	assert.Equal(t, []string{"b"}, p.ChangedNodeIDs, "no test node is announced; b is new against prod")
}

// An object store that is unreachable at promotion time leaves everything as
// it was: the handler returns the outage (the consumer pauses and redelivers
// the decision) and neither current_prod nor the run moves.
func TestHandleValidationResult_UnreachableArtifactStoreAtPromotionChangesNothing(t *testing.T) {
	deps, store := seedToValidating(t, "rDown")
	seedValidationNodes(t, deps, "rDown", []handlers.NodeResult{{NodeID: "a", Status: "ok"}, {NodeID: "b", Status: "ok"}})
	r, err := store.GetRelease("rDown")
	require.NoError(t, err)
	store.topologies.failLoad(r.CandidateTopologyRef().URI, storeOutage{})

	err = handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{ReleaseID: "rDown", AggregateStatus: "ok"})
	require.Error(t, err)
	var status interface{ HTTPStatusCode() int }
	assert.True(t, errors.As(err, &status), "the outage keeps its status code, so the consumer pauses")
	assert.Equal(t, 0, store.CurrentProdUpsertCalls())
	got, err := store.GetRelease("rDown")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusValidating, got.Status())
	for _, e := range outboxEntries(store) {
		assert.NotEqual(t, streams.ReleasePromotedV2, e.StreamName)
	}
}

// A corrupt artifact at promotion time is reported as corrupt, which the
// validation.result binding dead-letters (permanentOnCorruptTopology);
// current_prod does not move.
func TestHandleValidationResult_CorruptArtifactAtPromotionIsReportedCorrupt(t *testing.T) {
	deps, store := seedToValidating(t, "rBad")
	seedValidationNodes(t, deps, "rBad", []handlers.NodeResult{{NodeID: "a", Status: "ok"}, {NodeID: "b", Status: "ok"}})
	r, err := store.GetRelease("rBad")
	require.NoError(t, err)
	store.topologies.failLoad(r.CandidateTopologyRef().URI, fmt.Errorf("%w: tampered", ports.ErrTopologyArtifactCorrupt))

	err = handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{ReleaseID: "rBad", AggregateStatus: "ok"})
	assert.ErrorIs(t, err, ports.ErrTopologyArtifactCorrupt)
	assert.Equal(t, 0, store.CurrentProdUpsertCalls())
}

// Promotion moves current_prod to the run's own candidate artifact and takes
// the next promotion seq.
func TestHandleValidationResult_Promote_PointsCurrentProdAtTheCandidateArtifact(t *testing.T) {
	deps, store := seedToValidating(t, "rA")
	seedValidationNodes(t, deps, "rA", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "ok"},
	})
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA", AggregateStatus: "ok",
	}))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	cp := store.GetCurrentProd()
	assert.Equal(t, "rA", cp.ReleaseID())
	assert.Equal(t, r.CandidateTopologyRef(), cp.Topology())
	assert.Equal(t, int64(1), cp.PromotionSeq())
	assert.Equal(t, int64(1), store.LastPromotionSeq())
}
