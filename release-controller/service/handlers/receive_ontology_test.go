package handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ontologyDisabledText = `kind "ontology" is accepted only when the install's ontology switch is on (contracts.ontology.enabled)`
	verificationText     = `a verification run cannot have kind "ontology"`
)

func secondOntologyText(other string) string {
	return `project "` + other + `" is the install's ontology: an install has one ontology project`
}

func kindChangeText(project string, stored release.ManifestKind) string {
	return `project "` + project + `" is released as kind "` + string(stored) + `": a project cannot change between kind "ontology" and a node kind`
}

// receiveOntology submits a candidate of the given kind for project.
func receiveOntology(deps *handlers.Deps, releaseID, project, kind string) error {
	return handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: project, ReleaseID: releaseID, ImageTag: "img:1", Repo: "acme/ontology", CommitSHA: "cafebabe", Kind: kind,
	})
}

func ontologyDeps(now time.Time) (*handlers.Deps, *fakeStore) {
	deps, store := newDeps(now)
	deps.OntologyEnabled = true
	return deps, store
}

// While the install's ontology switch is off the kind ontology is refused as
// a client error, whose message names the setting, and nothing is stored.
func TestReceiveCandidate_RefusesOntologyWhileTheSwitchIsOff(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	require.False(t, deps.OntologyEnabled)

	err := receiveOntology(deps, "rOnt", "company-ontology", "ontology")

	require.ErrorIs(t, err, handlers.ErrOntologyDisabled)
	require.ErrorIs(t, err, handlers.ErrInvalidCandidate, "the refusal is answered as a client error")
	assert.EqualError(t, err, ontologyDisabledText)
	_, getErr := store.GetRelease("rOnt")
	assert.Error(t, getErr, "a refused release is not stored")
}

func TestReceiveCandidate_AcceptsOntologyWhileTheSwitchIsOn(t *testing.T) {
	deps, store := ontologyDeps(time.Unix(100, 0).UTC())

	require.NoError(t, receiveOntology(deps, "rOnt", "company-ontology", "ontology"))

	r, err := store.GetRelease("rOnt")
	require.NoError(t, err)
	assert.Equal(t, release.ManifestKindOntology, r.ManifestKind())
	assert.Equal(t, pipeline.StatusReceived, r.Status())
}

// An install has one ontology project: another project holding the kind in
// service_prod, or in a candidate run that has not ended, refuses a second.
func TestReceiveCandidate_RefusesASecondOntologyProject(t *testing.T) {
	t.Run("another project holds it in service_prod", func(t *testing.T) {
		deps, store := ontologyDeps(time.Unix(100, 0).UTC())
		store.SeedServiceProd(release.NewServiceProd("ontology-a", "rel-1", "s3://b/ontology-a/rel-1/ontology.yaml", "img:1",
			release.ManifestKindOntology, time.Unix(0, 0)))

		err := receiveOntology(deps, "rOnt", "ontology-b", "ontology")

		require.ErrorIs(t, err, handlers.ErrOntologyProjectConflict)
		assert.EqualError(t, err, secondOntologyText("ontology-a"))
		_, getErr := store.GetRelease("rOnt")
		assert.Error(t, getErr)
	})

	t.Run("another project has a candidate run in flight", func(t *testing.T) {
		deps, store := ontologyDeps(time.Unix(100, 0).UTC())
		require.NoError(t, receiveOntology(deps, "rOntA", "ontology-a", "ontology"))

		err := receiveOntology(deps, "rOntB", "ontology-b", "ontology")

		require.ErrorIs(t, err, handlers.ErrOntologyProjectConflict)
		assert.EqualError(t, err, secondOntologyText("ontology-a"))
		_, getErr := store.GetRelease("rOntB")
		assert.Error(t, getErr)
	})

	t.Run("a candidate run that ended does not count", func(t *testing.T) {
		deps, store := ontologyDeps(time.Unix(100, 0).UTC())
		ended := pipeline.NewCandidate("rOntA", "ontology-a", "img:1", false, false, "acme/ontology", "cafebabe",
			release.ManifestKindOntology, time.Unix(50, 0).UTC())
		require.NoError(t, ended.Fail("invalid_contract", "", nil, time.Unix(60, 0).UTC()))
		store.SeedRelease(ended)

		require.NoError(t, receiveOntology(deps, "rOntB", "ontology-b", "ontology"))
	})

	t.Run("a candidate run of another kind does not count", func(t *testing.T) {
		deps, store := ontologyDeps(time.Unix(100, 0).UTC())
		store.SeedRelease(pipeline.NewCandidate("rDbt", "svc-dbt", "img:1", false, false, "acme/dbt", "cafebabe",
			release.ManifestKindDbt, time.Unix(50, 0).UTC()))

		require.NoError(t, receiveOntology(deps, "rOnt", "company-ontology", "ontology"))
	})
}

// A project keeps one side of the line for good: a project released with a node
// kind cannot become the ontology, and the ontology project cannot be released
// with a node kind. The refusal leaves the project's production row, and so its
// nodes, alone.
func TestReceiveCandidate_RefusesAKindChangeToOrFromOntology(t *testing.T) {
	cases := map[string]struct {
		stored release.ManifestKind
		submit string
	}{
		"dbt to ontology":            {release.ManifestKindDbt, "ontology"},
		"python to ontology":         {release.ManifestKindPython, "ontology"},
		"ontology to python":         {release.ManifestKindOntology, "python"},
		"ontology to dbt":            {release.ManifestKindOntology, "dbt"},
		"ontology to an absent kind": {release.ManifestKindOntology, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			deps, store := ontologyDeps(time.Unix(100, 0).UTC())
			store.SeedServiceProd(release.NewServiceProd("svc", "rel-1", "s3://b/svc/rel-1/artifact", "img:1", tc.stored, time.Unix(0, 0)))

			err := receiveOntology(deps, "rKind", "svc", tc.submit)

			require.ErrorIs(t, err, handlers.ErrOntologyProjectConflict)
			assert.EqualError(t, err, kindChangeText("svc", tc.stored))
			_, getErr := store.GetRelease("rKind")
			assert.Error(t, getErr, "a refused release is not stored")
			sp := store.GetServiceProd("svc")
			require.NotNil(t, sp)
			assert.Equal(t, tc.stored, sp.ManifestKind(), "the production row stays as it was")
		})
	}
}

// The line between the ontology and the node kinds is also drawn when the
// switch is off: a dbt project is never moved by a release of the other side,
// and a dbt release for a dbt project is untouched.
func TestReceiveCandidate_NodeKindsKeepWorkingNextToTheOntologyRules(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	store.SeedServiceProd(release.NewServiceProd("svc-dbt", "rel-1", "s3://b/svc-dbt/rel-1/manifest.json", "img:1",
		release.ManifestKindDbt, time.Unix(0, 0)))
	store.SeedServiceProd(release.NewServiceProd("svc-py", "rel-1", "s3://b/svc-py/rel-1/odcs-bundle.json", "img:1",
		release.ManifestKindPython, time.Unix(0, 0)))

	require.NoError(t, receiveOntology(deps, "rDbt", "svc-dbt", ""))
	require.NoError(t, receiveOntology(deps, "rPy", "svc-py", "python"))
	require.NoError(t, receiveOntology(deps, "rNew", "svc-new", "dbt"))
}

// The ontology project releases again and again: its own production row and its
// own queued runs never count as a second ontology project.
func TestReceiveCandidate_AcceptsTheSameOntologyProjectAgain(t *testing.T) {
	t.Run("it holds the kind in service_prod", func(t *testing.T) {
		deps, store := ontologyDeps(time.Unix(100, 0).UTC())
		store.SeedServiceProd(release.NewServiceProd("company-ontology", "rel-1", "s3://b/company-ontology/rel-1/ontology.yaml", "img:1",
			release.ManifestKindOntology, time.Unix(0, 0)))

		require.NoError(t, receiveOntology(deps, "rOnt2", "company-ontology", "ontology"))

		r, err := store.GetRelease("rOnt2")
		require.NoError(t, err)
		assert.Equal(t, release.ManifestKindOntology, r.ManifestKind())
	})

	t.Run("it has a candidate run in flight", func(t *testing.T) {
		deps, _ := ontologyDeps(time.Unix(100, 0).UTC())
		require.NoError(t, receiveOntology(deps, "rOnt1", "company-ontology", "ontology"))

		require.NoError(t, receiveOntology(deps, "rOnt2", "company-ontology", "ontology"))
	})
}

// A release id the install already stored is answered as before, whatever the
// install holds now: a CI retry of an accepted ontology release never turns
// into a conflict because another project has since become the ontology.
func TestReceiveCandidate_ResubmittingAnOntologyReleaseStaysIdempotent(t *testing.T) {
	deps, store := ontologyDeps(time.Unix(100, 0).UTC())
	require.NoError(t, receiveOntology(deps, "rOnt", "ontology-b", "ontology"))
	store.SeedServiceProd(release.NewServiceProd("ontology-a", "rel-1", "s3://b/ontology-a/rel-1/ontology.yaml", "img:1",
		release.ManifestKindOntology, time.Unix(0, 0)))

	require.NoError(t, receiveOntology(deps, "rOnt", "ontology-b", "ontology"))
}

func TestReceiveVerification_RefusesOntology(t *testing.T) {
	for name, enabled := range map[string]bool{"switch off": false, "switch on": true} {
		t.Run(name, func(t *testing.T) {
			deps, store := newDeps(time.Unix(100, 0).UTC())
			deps.OntologyEnabled = enabled

			err := handlers.ReceiveVerification(context.Background(), deps, handlers.ReceiveVerificationInput{
				RunID: "v1", Project: "company-ontology", ImageTag: "img:1", Kind: "ontology",
				VerifiesReleaseID: "rel-0", Attempt: 1,
			})

			require.ErrorIs(t, err, handlers.ErrInvalidInput, "the refusal is answered as a client error")
			assert.EqualError(t, err, verificationText)
			_, getErr := store.GetRelease("v1")
			assert.Error(t, getErr, "a refused verification is not stored")
		})
	}
}

// The ontology project declares no node. An ontology release whose parse names
// no change to any node has nothing to validate: it promotes at once, moves
// current_prod, and records the project's production pointer with the kind
// ontology and the ontology file as its artifact.
func TestHandleParsedManifest_AnOntologyReleasePromotesWithNothingToValidate(t *testing.T) {
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
	parsing, err := store.GetRelease("rOnt")
	require.NoError(t, err)
	require.Equal(t, pipeline.StatusParsing, parsing.Status(), "an ontology release has no compile leg")

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:     "rOnt",
		Status:        "ok",
		TopologyRef:   putTopology(t, deps, "rOnt", topo),
		CodeBundleURI: "s3://continuo/code-bundles/rOnt/bundle.json",
	}))

	r, err := store.GetRelease("rOnt")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusPromoted, r.Status(), "nothing to validate: promote directly")
	for _, e := range outboxEntries(store) {
		assert.NotEqual(t, streams.ValidationRequestedV1, e.StreamName, "an empty validation request is never sent")
	}
	assert.Equal(t, "rOnt", store.GetCurrentProd().ReleaseID(), "current prod moved to the ontology release")

	sp := store.GetServiceProd("company-ontology")
	require.NotNil(t, sp, "the ontology project's production pointer is stored")
	assert.Equal(t, release.ManifestKindOntology, sp.ManifestKind())
	assert.Equal(t, "s3://continuo/company-ontology/rOnt/ontology.yaml", sp.ManifestS3Key())
	assert.Equal(t, "rOnt", sp.ReleaseID())
	assert.Equal(t, "dbt", string(store.GetServiceProd("svc-a").ManifestKind()), "another project's pointer is not touched")

	var promoted struct {
		ReleaseID string `json:"release_id"`
	}
	require.NoError(t, json.Unmarshal(findEntry(t, store, streams.ReleasePromotedV1).Payload, &promoted))
	assert.Equal(t, "rOnt", promoted.ReleaseID)
	assert.Equal(t, "promoted", outcomeOf(t, findEntry(t, store, streams.PipelineRunFinishedV1)))
}
