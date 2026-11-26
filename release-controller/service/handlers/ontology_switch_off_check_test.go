package handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertOntologyCheckRejectedWhileSwitchIsOff checks that the ontology check
// releaseID ended rejected invalid_artifact with the text intake answers while
// the switch is off, emitted release.rejected:v1 and release_check.finished:v1,
// and wrote neither current_prod nor a service_prod row for the ontology
// project.
func assertOntologyCheckRejectedWhileSwitchIsOff(t *testing.T, store *fakeStore, releaseID, project string) {
	t.Helper()
	r := mustGetRelease(t, store, releaseID)
	assert.Equal(t, releasecheck.StatusRejected, r.Status(), "an ontology check met with the switch off is rejected, never promoted")
	assert.Equal(t, string(pkg_model.RejectReasonInvalidArtifact), r.FailReason())
	assert.Equal(t, handlers.ErrOntologyDisabled.Error(), r.FailDetail())

	assert.Nil(t, store.GetServiceProd(project), "no service_prod row is written for the ontology file")
	assert.Zero(t, store.CurrentProdUpsertCalls(), "current_prod is not moved")
	for _, e := range store.OutboxEntries() {
		assert.NotEqual(t, streams.ReleasePromotedV1, e.StreamName, "nothing is promoted")
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
	assert.Equal(t, handlers.ErrOntologyDisabled.Error(), rejected.ErrorDetail)
	assert.Empty(t, rejected.FailingNodes)
	assert.Equal(t, "rejected", outcomeOf(t, findEntry(t, store, streams.ReleaseCheckFinishedV1)))
}

// A check accepted while the switch was on can still be queued when the switch is
// turned off: activation rejects it instead of requesting a parse that
// topology-controller would answer ok without reading the ontology file, and the
// check queued behind it is activated in the same call.
func TestAdvanceQueue_AQueuedOntologyCheckIsRejectedAtActivationWhileTheSwitchIsOff(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	require.False(t, deps.OntologyEnabled)
	store.SeedRelease(releasecheck.NewCandidate("rOnt", "company-ontology", "img:1", false, false, "acme/ontology", "cafebabe",
		release.ManifestKindOntology, deps.Clock.Now()))
	store.SeedRelease(releasecheck.NewCandidate("rBehind", "svc-y", "img:2", false, false, "acme/demo", "deadbeef",
		release.ManifestKindDbt, deps.Clock.Now().Add(time.Second)))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	assertOntologyCheckRejectedWhileSwitchIsOff(t, store, "rOnt", "company-ontology")
	for _, e := range store.OutboxEntries() {
		assert.NotEqual(t, streams.ReleaseRequestedV1, e.StreamName, "a rejected ontology check requests no parse")
	}
	assert.Equal(t, releasecheck.StatusCompiling, mustGetRelease(t, store, "rBehind").Status(),
		"the queue activates the next check once the ontology check is rejected")
}

// A check already parsing when the switch is turned off meets the switch off at
// its parse result: an ok result does not promote it, whether or not the check
// has anything to validate.
func TestHandleParsedManifest_AParsingOntologyCheckIsRejectedWhileTheSwitchIsOff(t *testing.T) {
	deps, store := ontologyDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"
	store.SeedServiceProd(release.NewServiceProd("svc-a", "prev", "s3://continuo/svc-a/prev/manifest.json", "sha-a",
		release.ManifestKindDbt, time.Unix(0, 0)))
	topo := release.Topology{
		{UniqueID: "a", ProjectName: "svc-a", ContentHash: "h_a"},
		{UniqueID: "b", ProjectName: "svc-a", ContentHash: "h_b", UpstreamUniqueIDs: []string{"a"}},
	}
	seedProd(t, deps, store, "prev", topo, time.Unix(50, 0).UTC())
	require.NoError(t, receiveOntology(deps, "rOnt", "company-ontology", "ontology"))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.Equal(t, releasecheck.StatusParsing, mustGetRelease(t, store, "rOnt").Status())

	deps.OntologyEnabled = false
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:     "rOnt",
		Status:        "ok",
		TopologyRef:   putTopology(t, deps, "rOnt", topo),
		CodeBundleURI: "s3://continuo/code-bundles/rOnt/bundle.json",
	}))

	assertOntologyCheckRejectedWhileSwitchIsOff(t, store, "rOnt", "company-ontology")
	assert.Equal(t, "prev", store.GetCurrentProd().ReleaseID(), "current_prod stays on the release it named")
	assert.Equal(t, "dbt", string(store.GetServiceProd("svc-a").ManifestKind()), "another project's pointer is not touched")
}

// The refusal is the same for a failed parse result: the check is rejected with
// the switch's text, not with the parse failure of a file nobody read.
func TestHandleParsedManifest_AParsingOntologyCheckWithAFailedResultIsRejectedWhileTheSwitchIsOff(t *testing.T) {
	deps, store := ontologyDeps(time.Unix(100, 0).UTC())
	require.NoError(t, receiveOntology(deps, "rOnt", "company-ontology", "ontology"))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.Equal(t, releasecheck.StatusParsing, mustGetRelease(t, store, "rOnt").Status())

	deps.OntologyEnabled = false
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:   "rOnt",
		Status:      "failed",
		FailureKind: pkg_model.ParseFailureKindInvalidContract,
		Detail:      "a contract is invalid",
	}))

	assertOntologyCheckRejectedWhileSwitchIsOff(t, store, "rOnt", "company-ontology")
}

// The switch concerns the ontology kind alone: with it off, a check of a node kind
// is activated and, once parsed ok, promoted as before.
func TestNodeKindChecksAreUnaffectedByTheOntologySwitchBeingOff(t *testing.T) {
	t.Run("a dbt run is activated into its compile leg", func(t *testing.T) {
		deps, store := newDeps(time.Unix(200, 0).UTC())
		deps.Bucket = "b"
		require.False(t, deps.OntologyEnabled)
		require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
			Project: "svc-a", ReleaseID: "rDbt", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
		}))

		require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

		assert.Equal(t, releasecheck.StatusCompiling, mustGetRelease(t, store, "rDbt").Status())
	})

	t.Run("a Python run is activated, parsed and promoted", func(t *testing.T) {
		deps, store := newDeps(time.Unix(100, 0).UTC())
		deps.Bucket = "continuo"
		require.False(t, deps.OntologyEnabled)
		require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
			Project: "svc-py", ReleaseID: "rPy", ImageTag: "sha-py", Repo: "acme/demo", CommitSHA: "deadbeef",
			Kind: "python", Bootstrap: true,
		}))
		require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
		require.Equal(t, releasecheck.StatusParsing, mustGetRelease(t, store, "rPy").Status())

		topo := release.Topology{{UniqueID: "a", ProjectName: "svc-py", UpstreamUniqueIDs: []string{}}}
		require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
			ReleaseID:   "rPy",
			Status:      "ok",
			TopologyRef: putTopology(t, deps, "rPy", topo),
		}))

		assert.Equal(t, releasecheck.StatusPromoted, mustGetRelease(t, store, "rPy").Status())
		require.NotNil(t, store.GetServiceProd("svc-py"))
	})
}
