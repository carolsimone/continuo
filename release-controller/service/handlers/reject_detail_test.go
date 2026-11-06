package handlers_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The stages below reject a release without any error text from
// execution-controller. These tests pin what release-controller records as the
// operator-facing detail instead: the nodes that did not pass and where they
// live, built from the topology, never from a log.

// --- validation ---

func TestHandleValidationResult_Rejected_DetailNamesTheFailingNodes(t *testing.T) {
	deps, store := seedToValidating(t, "rDetail")
	seedValidationNodes(t, deps, "rDetail", []handlers.NodeResult{
		{NodeID: "a", Status: "failed", DBTLogURI: "s3://l"},
		{NodeID: "b", Status: "ok"},
	})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rDetail", AggregateStatus: "failed",
	}))

	r, err := store.GetRelease("rDetail")
	require.NoError(t, err)
	assert.Equal(t,
		fmt.Sprintf("1 of %d nodes did not pass validation: a (svc-a)", len(r.ValidationNodeIDs())),
		r.FailDetail())
}

func TestHandleValidationResult_Rejected_DetailCountsEveryNodeThatDidNotPass(t *testing.T) {
	deps, store := seedToValidating(t, "rDetailAll")
	seedValidationNodes(t, deps, "rDetailAll", []handlers.NodeResult{
		{NodeID: "a", Status: "failed"},
		{NodeID: "b", Status: "skipped"},
	})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rDetailAll", AggregateStatus: "failed",
	}))

	r, err := store.GetRelease("rDetailAll")
	require.NoError(t, err)
	assert.Equal(t,
		fmt.Sprintf("2 of %d nodes did not pass validation: a (svc-a) and b (svc-a)", len(r.ValidationNodeIDs())),
		r.FailDetail(),
		"a node skipped because its upstream failed did not pass either")
}

func TestHandleValidationResult_Rejected_DetailSaysSoWhenOnlyTheAggregateFailed(t *testing.T) {
	deps, store := seedToValidating(t, "rDetailAgg")
	seedValidationNodes(t, deps, "rDetailAgg", []handlers.NodeResult{{NodeID: "a", Status: "ok"}})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rDetailAgg", AggregateStatus: "failed",
	}))

	r, err := store.GetRelease("rDetailAgg")
	require.NoError(t, err)
	assert.Equal(t, "validation failed, but no per-node failure was recorded", r.FailDetail())
}

func TestHandleValidationResult_VerificationFails_DetailNamesTheFailingNodes(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deps, store := newDeps(now)
	r := pipeline.NewVerification("verify-rel-1-core-a1", "core", "img", "rel-1", 1, "", release.ManifestKindDbt, now)
	require.NoError(t, r.TransitionToParsing(now))
	require.NoError(t, r.TransitionToValidating(store.storeTopology(r.ID(), release.Topology{{UniqueID: "model.core.orders", ServiceName: "core"}}), []string{"model.core.orders"}, now))
	r.UpsertStageResult("validation", pipeline.NodeValidationResult{NodeID: "model.core.orders", Status: "failed"})
	store.SeedRelease(r)

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: r.ID(), AggregateStatus: "failed",
	}))

	got, err := store.GetRelease(r.ID())
	require.NoError(t, err)
	assert.Equal(t, "1 of 1 nodes did not pass validation: model.core.orders (core)", got.FailDetail(),
		"a failed verification explains itself on the verification page the same way")
}

// --- seed build ---

func TestHandleSeedBuildResult_Failed_DetailNamesTheSeedsThatDidNotBuild(t *testing.T) {
	deps, store := newTestDeps(t)
	releaseID := "rel-seed-detail"
	putSeedBuildingRelease(t, store, deps, releaseID, topoSeedsWithFilePath())

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: releaseID,
		Status:    "failed",
		PerNode: []handlers.NodeResult{
			{NodeID: "analytics.seed_fx_rates_eur", Status: "failed", DBTLogURI: "s3://logs/eur.log"},
			{NodeID: "analytics.seed_equities", Status: "ok"},
		},
	}))

	r := mustGetRelease(t, store, releaseID)
	const want = "1 seed did not build: analytics.seed_fx_rates_eur (svc-fin)"
	assert.Equal(t, want, r.FailDetail())
	payload := decodeJSON(t, findEntry(t, store, streams.ReleaseRejectedV1).Payload)
	assert.Equal(t, want, payload["error_detail"], "the event carries the same explanation the release records")
}

func TestHandleSeedBuildResult_Failed_KeepsTheDetailTheProducerSends(t *testing.T) {
	deps, store := newTestDeps(t)
	releaseID := "rel-seed-keeps"
	putSeedBuildingRelease(t, store, deps, releaseID, topoSeedsWithFilePath())

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID:   releaseID,
		Status:      "failed",
		PerNode:     []handlers.NodeResult{{NodeID: "analytics.seed_fx_rates_eur", Status: "failed"}},
		ErrorDetail: "csv parse failure",
	}))

	assert.Equal(t, "csv parse failure", mustGetRelease(t, store, releaseID).FailDetail(),
		"text from the producer is the better explanation and is never replaced")
}

func TestHandleSeedBuildResult_Failed_DetailSaysSoWhenNoNodeIsNamed(t *testing.T) {
	deps, store := newTestDeps(t)
	releaseID := "rel-seed-bare"
	putSeedBuildingRelease(t, store, deps, releaseID, topoSeedsWithFilePath())

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: releaseID, Status: "failed",
	}))

	assert.Equal(t, "seed build failed, but no per-node failure was recorded",
		mustGetRelease(t, store, releaseID).FailDetail())
}

// --- compile ---

func TestHandleCompileResult_PlainCompileFailure_DetailNamesWhatFailedToCompile(t *testing.T) {
	d, fakes := newTestDeps(t)
	releaseID := "rel-compile-detail"
	putCompilingRelease(t, fakes, d, releaseID)

	require.NoError(t, handlers.HandleCompileResult(ctx(t), d, handlers.HandleCompileResultInput{
		ReleaseID: releaseID,
		Status:    "failed",
		PerNode:   []handlers.NodeResult{{NodeID: "core", Status: "failed", FailedContainer: "compile", DBTLogURI: "s3://c.log"}},
	}))

	r := mustGetRelease(t, fakes, releaseID)
	assert.Equal(t, "compile_failed", r.FailReason())
	const want = "dbt compile failed for: core"
	assert.Equal(t, want, r.FailDetail())
	payload := decodeJSON(t, findEntry(t, fakes, streams.ReleaseRejectedV1).Payload)
	assert.Equal(t, want, payload["error_detail"])
}

func TestHandleCompileResult_PlainCompileFailure_KeepsTheDetailTheProducerSends(t *testing.T) {
	d, fakes := newTestDeps(t)
	releaseID := "rel-compile-keeps"
	putCompilingRelease(t, fakes, d, releaseID)

	require.NoError(t, handlers.HandleCompileResult(ctx(t), d, handlers.HandleCompileResultInput{
		ReleaseID:   releaseID,
		Status:      "failed",
		PerNode:     []handlers.NodeResult{{NodeID: "core", Status: "failed"}},
		ErrorDetail: "Compilation Error in model daily_transactions",
	}))

	assert.Equal(t, "Compilation Error in model daily_transactions", mustGetRelease(t, fakes, releaseID).FailDetail())
}

func TestHandleCompileResult_UploadFailure_KeepsItsFixedExplanation(t *testing.T) {
	d, fakes := newTestDeps(t)
	releaseID := "rel-compile-upload"
	putCompilingRelease(t, fakes, d, releaseID)

	require.NoError(t, handlers.HandleCompileResult(ctx(t), d, handlers.HandleCompileResultInput{
		ReleaseID: releaseID,
		Status:    "failed",
		PerNode:   []handlers.NodeResult{{NodeID: "core", Status: "failed", FailedContainer: "upload"}},
	}))

	assert.Equal(t, "internal artifact publication failed; no change to the dbt project will fix this",
		mustGetRelease(t, fakes, releaseID).FailDetail(),
		"a failure attributed to a pod container already has a precise explanation")
}
