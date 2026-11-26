package handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pythonKindRefusal is the refusal text the release domain gives for the kind
// python. A stored run of that kind ends with it as its detail.
const pythonKindRefusal = `kind "python" is no longer accepted: release the project as "python-odcs", one ODCS contract per node (docs/contracts.md)`

// asStoredPython replaces the run releaseID with the same run carrying the
// kind python, as a row an earlier continuo version accepted and stored holds.
func asStoredPython(t *testing.T, store *fakeStore, releaseID string) {
	t.Helper()
	r := mustGetRelease(t, store, releaseID)
	store.SeedRelease(pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:                      r.ID(),
		Kind:                    r.Kind(),
		Status:                  r.Status(),
		ImageTags:               r.ImageTags(),
		ChangedProject:          r.ChangedProject(),
		ManifestKind:            release.ManifestKindPython,
		CandidateTopology:       r.CandidateTopologyRef(),
		ValidationNodeIDs:       r.ValidationNodeIDs(),
		PerNodeResults:          r.PerNodeResults(),
		FailReason:              r.FailReason(),
		FailDetail:              r.FailDetail(),
		FailingNodes:            r.FailingNodes(),
		CodeBundleURI:           r.CodeBundleURI(),
		CandidateContractBundle: r.CandidateContractBundle(),
		ContinuoContracts:       r.ContinuoContracts(),
		CreatedAt:               r.CreatedAt(),
		Transitions:             r.Transitions(),
		Notices:                 r.Notices(),
		Bootstrap:               r.IsBootstrap(),
		Rebaseline:              r.IsRebaseline(),
		Repo:                    r.Repo(),
		CommitSHA:               r.CommitSHA(),
		RemediationRound:        r.RemediationRound(),
		RejectionPayload:        r.RejectionPayload(),
		VerifiesReleaseID:       r.VerifiesReleaseID(),
		Attempt:                 r.Attempt(),
		SourceOverlayURI:        r.SourceOverlayURI(),
	}))
}

// assertStoredPythonCandidateRejected checks that the candidate releaseID of
// the project named project ended rejected invalid_artifact with the refusal text,
// emitted release.rejected:v1 and pipeline.run.finished:v1, and wrote neither
// current_prod nor a service_prod row.
func assertStoredPythonCandidateRejected(t *testing.T, store *fakeStore, releaseID, project string) {
	t.Helper()
	r := mustGetRelease(t, store, releaseID)
	assert.Equal(t, pipeline.StatusRejected, r.Status(), "a stored kind-python run is rejected, never promoted")
	assert.Equal(t, string(pkg_model.RejectReasonInvalidArtifact), r.FailReason())
	assert.Contains(t, r.FailDetail(), pythonKindRefusal)

	assert.Nil(t, store.GetServiceProd(project), "no service_prod row is written for a kind-python run")
	assert.Zero(t, store.CurrentProdUpsertCalls(), "current_prod is not moved")
	for _, e := range store.OutboxEntries() {
		assert.NotEqual(t, streams.ReleasePromotedV1, e.StreamName, "no release.promoted:v1 is emitted")
	}

	var rejected struct {
		ReleaseID    string   `json:"release_id"`
		Reason       string   `json:"reason"`
		ErrorDetail  string   `json:"error_detail"`
		FailingNodes []string `json:"failing_nodes"`
	}
	require.NoError(t, json.Unmarshal(findEntry(t, store, streams.ReleaseRejectedV1).Payload, &rejected))
	assert.Equal(t, releaseID, rejected.ReleaseID)
	assert.Equal(t, string(pkg_model.RejectReasonInvalidArtifact), rejected.Reason)
	assert.Contains(t, rejected.ErrorDetail, pythonKindRefusal)
	assert.Empty(t, rejected.FailingNodes)
	assert.Equal(t, "rejected", outcomeOf(t, findEntry(t, store, streams.PipelineRunFinishedV1)))
}

func TestHandleValidationResult_AStoredPythonRunIsRejectedNotPromoted(t *testing.T) {
	deps, store := seedToValidatingPython(t, "rStoredPy")
	asStoredPython(t, store, "rStoredPy")
	seedValidationNodes(t, deps, "rStoredPy", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "ok"},
	})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rStoredPy",
		AggregateStatus: "ok",
	}))

	assertStoredPythonCandidateRejected(t, store, "rStoredPy", "svc-py")
}

func TestHandleValidationResult_AStoredPythonVerificationFailsWithNoReleaseEvent(t *testing.T) {
	deps, store := seedToValidatingVerification(t, "vStoredPy")
	asStoredPython(t, store, "vStoredPy")
	seedValidationNodes(t, deps, "vStoredPy", []handlers.NodeResult{
		{NodeID: "a", Status: "ok"},
		{NodeID: "b", Status: "ok"},
	})

	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "vStoredPy",
		AggregateStatus: "ok",
	}))

	r := mustGetRelease(t, store, "vStoredPy")
	assert.Equal(t, pipeline.StatusFailed, r.Status(), "a stored kind-python verification fails, never passes")
	assert.Equal(t, string(pkg_model.RejectReasonInvalidArtifact), r.FailReason())
	assert.Contains(t, r.FailDetail(), pythonKindRefusal)
	for _, e := range store.OutboxEntries() {
		assert.NotEqual(t, streams.ReleaseRejectedV1, e.StreamName, "a verification emits no release event")
		assert.NotEqual(t, streams.ReleasePromotedV1, e.StreamName, "a verification emits no release event")
	}
	assert.Equal(t, "failed", outcomeOf(t, findEntry(t, store, streams.PipelineRunFinishedV1)))
}

func TestHandleParsedManifest_AStoredPythonBootstrapRunIsRejectedNotPromoted(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project:   "svc-py",
		ReleaseID: "rStoredPyBoot",
		ImageTag:  "sha-py",
		Repo:      "acme/demo",
		CommitSHA: "deadbeef",
		Kind:      "python-odcs",
		Bootstrap: true,
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.Equal(t, pipeline.StatusParsing, mustGetRelease(t, store, "rStoredPyBoot").Status())
	asStoredPython(t, store, "rStoredPyBoot")

	topo := release.Topology{
		{UniqueID: "a", ProjectName: "svc-py", UpstreamUniqueIDs: []string{}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:   "rStoredPyBoot",
		Status:      "ok",
		TopologyRef: putTopology(t, deps, "rStoredPyBoot", topo),
	}))

	assertStoredPythonCandidateRejected(t, store, "rStoredPyBoot", "svc-py")
}

func TestHandleSeedBuildResult_AStoredPythonRunIsRejectedNotPromoted(t *testing.T) {
	deps, store := newTestDeps(t)
	putSeedBuildingRelease(t, store, deps, "rStoredPySeed", topoSeedOnly())
	asStoredPython(t, store, "rStoredPySeed")

	require.NoError(t, handlers.HandleSeedBuildResult(ctx(t), deps, handlers.HandleSeedBuildResultInput{
		ReleaseID: "rStoredPySeed", Status: "ok",
	}))

	assertStoredPythonCandidateRejected(t, store, "rStoredPySeed", "svc-fin")
}

func TestAdvanceQueue_AQueuedPythonRunIsRejectedAtActivation(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"
	store.SeedRelease(pipeline.NewCandidate("rQueuedPy", "svc-py", "sha-py", false, false,
		"acme/demo", "deadbeef", release.ManifestKindPython, deps.Clock.Now()))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project:   "svc-a",
		ReleaseID: "rQueuedDbt",
		ImageTag:  "sha-a",
		Repo:      "acme/demo",
		CommitSHA: "cafebabe",
	}))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	assertStoredPythonCandidateRejected(t, store, "rQueuedPy", "svc-py")
	for _, e := range store.OutboxEntries() {
		assert.NotEqual(t, streams.ReleaseRequestedV1, e.StreamName, "a kind-python run requests no parse")
	}
	assert.Equal(t, pipeline.StatusCompiling, mustGetRelease(t, store, "rQueuedDbt").Status(),
		"the queue activates the next run once the kind-python run is rejected")
}

func TestAdvanceQueue_AQueuedPythonVerificationFailsAtActivation(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"
	store.SeedRelease(pipeline.NewVerification("vQueuedPy", "svc-py", "sha-py", "rRejected", 1, "",
		release.ManifestKindPython, deps.Clock.Now()))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r := mustGetRelease(t, store, "vQueuedPy")
	assert.Equal(t, pipeline.StatusFailed, r.Status())
	assert.Equal(t, string(pkg_model.RejectReasonInvalidArtifact), r.FailReason())
	assert.Contains(t, r.FailDetail(), pythonKindRefusal)
	for _, e := range store.OutboxEntries() {
		assert.NotEqual(t, streams.ReleaseRequestedV1, e.StreamName, "a kind-python run requests no parse")
		assert.NotEqual(t, streams.ReleaseRejectedV1, e.StreamName, "a verification emits no release event")
	}
	assert.Equal(t, "failed", outcomeOf(t, findEntry(t, store, streams.PipelineRunFinishedV1)))
}
