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

func TestAdvanceQueue_NoActive_ActivatesToCompilingAndEmitsCompileRequested(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "my-bucket"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc-a", ReleaseID: "rA", ImageTag: "tag-a", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rA")
	assert.Equal(t, releasecheck.StatusCompiling, r.Status())

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.CompileRequestedV1, entries[0].StreamName)

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entries[0].Payload, &payload))

	// Must carry release_id, project, image_tag, bucket; must NOT carry manifest_keys.
	var releaseID string
	require.NoError(t, json.Unmarshal(payload["release_id"], &releaseID))
	assert.Equal(t, "rA", releaseID)

	var project string
	require.NoError(t, json.Unmarshal(payload["project"], &project))
	assert.Equal(t, "svc-a", project)

	var imageTag string
	require.NoError(t, json.Unmarshal(payload["image_tag"], &imageTag))
	assert.Equal(t, "tag-a", imageTag)

	var bucket string
	require.NoError(t, json.Unmarshal(payload["bucket"], &bucket))
	assert.Equal(t, "my-bucket", bucket)

	var candidateSchema string
	require.NoError(t, json.Unmarshal(payload["candidate_schema"], &candidateSchema))
	assert.Equal(t, handlers.CandidateSchemaFor(releaseID), candidateSchema)

	_, hasManifestKeys := payload["manifest_keys"]
	assert.False(t, hasManifestKeys, "manifest_keys must not appear in compile.requested payload")
	_, hasManifestsURI := payload["manifests_uri"]
	assert.False(t, hasManifestsURI, "manifests_uri must not appear in the payload")
}

func TestAdvanceQueue_OtherProjectsIncluded_ImageTagsAssembledOnRelease(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "bucket"

	// Pre-seed two other projects' production pointers.
	store.SeedProductionPointer(release.NewProductionPointer("svc-b", "rOLD1", "s3://bucket/svc-b/rOLD1/manifest.json", "tag-b-old", release.ManifestKindDbt, time.Unix(0, 0)))
	store.SeedProductionPointer(release.NewProductionPointer("svc-c", "rOLD2", "s3://bucket/svc-c/rOLD2/manifest.json", "tag-c-old", release.ManifestKindDbt, time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc-a", ReleaseID: "rNEW", ImageTag: "tag-a-new", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.CompileRequestedV1, entries[0].StreamName)

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entries[0].Payload, &payload))

	// compile.requested carries the changed project + its image_tag.
	var project string
	require.NoError(t, json.Unmarshal(payload["project"], &project))
	assert.Equal(t, "svc-a", project)

	var imageTag string
	require.NoError(t, json.Unmarshal(payload["image_tag"], &imageTag))
	assert.Equal(t, "tag-a-new", imageTag)

	// Assembled image tags must already be persisted on the release (SetAssembledImageTags).
	r, _ := store.GetRelease("rNEW")
	assert.Equal(t, releasecheck.StatusCompiling, r.Status())
	assert.Equal(t, map[string]string{
		"svc-a": "tag-a-new",
		"svc-b": "tag-b-old",
		"svc-c": "tag-c-old",
	}, r.ImageTags())
}

func TestAdvanceQueue_ProdSeeded_UncoveredProject_BlocksActivation(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "bucket"

	// The live release lists three live projects, but none of them has a
	// production pointer.
	topology := release.Topology{
		{UniqueID: "a", ProjectName: "svc-a"},
		{UniqueID: "b", ProjectName: "svc-b"},
		{UniqueID: "c", ProjectName: "svc-c"},
	}
	seedProd(t, deps, store, "rProd", topology, time.Unix(0, 0).UTC())

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc-a", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	// svc-b and svc-c are uncovered, so the release stays Received and nothing
	// is emitted; the operator must seed their production pointers first.
	r, _ := store.GetRelease("rA")
	assert.Equal(t, releasecheck.StatusReceived, r.Status())
	assert.Empty(t, outboxEntries(store))
}

func TestAdvanceQueue_ProdSeeded_AllProjectsCovered_Proceeds(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "bucket"

	topology := release.Topology{
		{UniqueID: "a", ProjectName: "svc-a"},
		{UniqueID: "b", ProjectName: "svc-b"},
		{UniqueID: "c", ProjectName: "svc-c"},
	}
	seedProd(t, deps, store, "rProd", topology, time.Unix(0, 0).UTC())

	// svc-b and svc-c are covered by pointers; svc-a is the changed project.
	store.SeedProductionPointer(release.NewProductionPointer("svc-b", "rOLD1", "s3://bucket/svc-b/rOLD1/manifest.json", "tag-b-old", release.ManifestKindDbt, time.Unix(0, 0)))
	store.SeedProductionPointer(release.NewProductionPointer("svc-c", "rOLD2", "s3://bucket/svc-c/rOLD2/manifest.json", "tag-c-old", release.ManifestKindDbt, time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc-a", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rA")
	assert.Equal(t, releasecheck.StatusCompiling, r.Status())

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.CompileRequestedV1, entries[0].StreamName)
}

// TestUncoveredProdProjects_IgnoresUnknownExternalNodes pins that the live
// release's unknown-external nodes do not count as a project without a
// production pointer: an unknown-external node has no project, so no pointer can ever cover it,
// and counting it would hold every release in the queue.
func TestUncoveredProdProjects_IgnoresUnknownExternalNodes(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "bucket"

	seedProd(t, deps, store, "rProd", release.Topology{
		{UniqueID: "raw.orders", SchemaName: "raw", TableName: "orders", ResolvedRelationID: "raw.orders", NodeType: "unknown-external", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ProjectName: "svc-b", NodeType: "dbt-model", UpstreamUniqueIDs: []string{"raw.orders"}},
	}, time.Unix(0, 0).UTC())
	store.SeedProductionPointer(release.NewProductionPointer("svc-b", "rOLD1", "s3://bucket/svc-b/rOLD1/manifest.json", "tag-b-old", release.ManifestKindDbt, time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc-a", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rA")
	assert.Equal(t, releasecheck.StatusCompiling, r.Status(), "svc-b is covered and the unknown-external node needs no pointer")
}

// TestUncoveredProdProjects_StillBlocksOnAnUncoveredProjectBesideAnUnknownExternalNode
// is the control: an unknown-external node is skipped, a real project without a
// pointer still holds the release.
func TestUncoveredProdProjects_StillBlocksOnAnUncoveredProjectBesideAnUnknownExternalNode(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "bucket"

	seedProd(t, deps, store, "rProd", release.Topology{
		{UniqueID: "raw.orders", SchemaName: "raw", TableName: "orders", ResolvedRelationID: "raw.orders", NodeType: "unknown-external", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ProjectName: "svc-b", NodeType: "dbt-model", UpstreamUniqueIDs: []string{"raw.orders"}},
	}, time.Unix(0, 0).UTC())

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc-a", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rA")
	assert.Equal(t, releasecheck.StatusReceived, r.Status(), "svc-b has no pointer")
}

func TestAdvanceQueue_ActiveExists_DoesNothing(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc", ReleaseID: "rB", ImageTag: "t2", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	rB, _ := store.GetRelease("rB")
	assert.Equal(t, releasecheck.StatusReceived, rB.Status())
	assert.Len(t, outboxEntries(store), 1) // only rA emitted
}

func TestAdvanceQueue_NoQueued_DoesNothing(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	assert.Empty(t, outboxEntries(store))
}

func TestAdvanceQueue_PythonRelease_SkipsCompile_EmitsReleaseRequested(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedProductionPointer(release.NewProductionPointer("svc-dbt", "rOld", "s3://b/svc-dbt/rOld/manifest.json", "t-old", release.ManifestKindDbt, time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc-py", ReleaseID: "rPy", ImageTag: "img:1", Repo: "acme/py", CommitSHA: "cafebabe",
		Kind: "python",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rPy")
	assert.Equal(t, release.ManifestKindPython, r.ManifestKind())
	assert.Equal(t, releasecheck.StatusParsing, r.Status(), "Python releases activate straight into Parsing")
	assert.Equal(t, map[string]string{"svc-py": "img:1", "svc-dbt": "t-old"}, r.ImageTags())

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName, "no compile.requested for a Python release")

	var p struct {
		ReleaseID    string `json:"release_id"`
		ManifestKeys []struct {
			Project string `json:"project"`
			S3URI   string `json:"s3_uri"`
			Kind    string `json:"kind"`
		} `json:"manifest_keys"`
	}
	require.NoError(t, json.Unmarshal(entries[0].Payload, &p))
	assert.Equal(t, "rPy", p.ReleaseID)
	require.Len(t, p.ManifestKeys, 2)
	byProject := map[string]string{}
	uris := map[string]string{}
	for _, k := range p.ManifestKeys {
		byProject[k.Project] = k.Kind
		uris[k.Project] = k.S3URI
	}
	assert.Equal(t, map[string]string{"svc-py": "python", "svc-dbt": "dbt"}, byProject)
	assert.Equal(t, "s3://b/svc-py/rPy/odcs-bundle.json", uris["svc-py"])
	assert.Equal(t, "s3://b/svc-dbt/rOld/manifest.json", uris["svc-dbt"])
	assert.Equal(t, map[string]string{"svc-py": "img:1", "svc-dbt": "t-old"}, releaseRequestedImageTags(t, entries[0].Payload),
		"release.requested carries the check's assembled image tags: topology-controller joins exactly these onto every node")
}

// The ontology project ships its ontology file before the release is
// requested, so its release has no compile leg: it activates straight into
// Parsing, and release.requested names the file as the project's artifact.
func TestAdvanceQueue_AnOntologyReleaseGoesStraightToParsing(t *testing.T) {
	deps, store := ontologyDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedProductionPointer(release.NewProductionPointer("svc-dbt", "rOld", "s3://b/svc-dbt/rOld/manifest.json", "t-old", release.ManifestKindDbt, time.Unix(0, 0)))
	store.SeedRelease(releasecheck.NewCandidate("rOnt", "company-ontology", "img:1", false, false, "acme/ontology", "cafebabe",
		release.ManifestKindOntology, deps.Clock.Now()))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rOnt")
	assert.Equal(t, release.ManifestKindOntology, r.ManifestKind())
	assert.Equal(t, releasecheck.StatusParsing, r.Status(), "an ontology release has no compile leg")

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName, "no compile.requested for an ontology release")

	var p struct {
		ManifestKeys []struct {
			Project string `json:"project"`
			S3URI   string `json:"s3_uri"`
			Kind    string `json:"kind"`
		} `json:"manifest_keys"`
	}
	require.NoError(t, json.Unmarshal(entries[0].Payload, &p))
	kinds := map[string]string{}
	uris := map[string]string{}
	for _, k := range p.ManifestKeys {
		kinds[k.Project] = k.Kind
		uris[k.Project] = k.S3URI
	}
	assert.Equal(t, map[string]string{"company-ontology": "ontology", "svc-dbt": "dbt"}, kinds)
	assert.Equal(t, "s3://b/company-ontology/rOnt/ontology.yaml", uris["company-ontology"])
	assert.Equal(t, "s3://b/svc-dbt/rOld/manifest.json", uris["svc-dbt"])
}

func TestAdvanceQueue_DbtRelease_StillCompiles(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc-a", ReleaseID: "rDbt", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	r, _ := store.GetRelease("rDbt")
	assert.Equal(t, releasecheck.StatusCompiling, r.Status())
	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.CompileRequestedV1, entries[0].StreamName)
}

func TestAdvanceQueue_PicksOldestFirst(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc", ReleaseID: "rOLD", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Project: "svc", ReleaseID: "rNEW", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	rOLD, _ := store.GetRelease("rOLD")
	rNEW, _ := store.GetRelease("rNEW")
	assert.Equal(t, releasecheck.StatusCompiling, rOLD.Status())
	assert.Equal(t, releasecheck.StatusReceived, rNEW.Status())
}

func TestAdvanceQueue_CompileRequestedCarriesSourceOverlayURI(t *testing.T) {
	deps, store := newDeps(time.Now())
	r := releasecheck.NewFix("r-ov", "svc", "tag", "rel-orig", 1,
		"s3://b/svc/r-ov/source-overlay.tar.gz", release.ManifestKindDbt, deps.Clock.Now())
	store.SeedRelease(r)

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	var found bool
	for _, e := range outboxEntries(store) {
		if e.StreamName != streams.CompileRequestedV1 {
			continue
		}
		var p struct {
			SourceOverlayURI string `json:"source_overlay_uri"`
		}
		require.NoError(t, json.Unmarshal(e.Payload, &p))
		assert.Equal(t, "s3://b/svc/r-ov/source-overlay.tar.gz", p.SourceOverlayURI)
		found = true
	}
	require.True(t, found, "compile.requested must be emitted for a dbt fix's release check")
}

// verifiedOriginal is a rejected candidate release of svc-a, parsed (so its
// candidate manifest exists at its canonical key) and carrying its own image
// tag for svc-a. A fix's release check verifying it must assemble THAT manifest
// for svc-a rather than svc-a's live production pointer.
func verifiedOriginal(id string, now time.Time) *releasecheck.Check {
	return releasecheck.Rehydrate(releasecheck.RehydrateInput{
		ID:             id,
		Status:         releasecheck.StatusRejected,
		ImageTags:      map[string]string{"svc-a": "tag-a-candidate", "svc-b": "tag-b-old"},
		ChangedProject: "svc-a",
		// Activation only needs to know the verified release was parsed; it
		// never reads the artifact.
		CandidateTopology: release.TopologyRef{
			URI: "s3://b/tenants/default/topologies/" + id + "/topology.json.gz", SHA256: "sha-" + id, NodeCount: 1,
		},
		ManifestKind: release.ManifestKindDbt,
		CreatedAt:    now,
	})
}

// TestAdvanceQueue_FixVerifiesTheRejectedReleasesCandidate covers the
// fix whose edit lands in a DIFFERENT project than the one whose release was
// rejected: the rejection came from svc-a's candidate, so verifying the fix
// against svc-a's PRODUCTION manifest would judge it on code the rejection was
// never about. The fix's release check names the release it verifies, and svc-a is
// assembled from that release's candidate — manifest key and image tag both.
func TestAdvanceQueue_FixVerifiesTheRejectedReleasesCandidate(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedRelease(verifiedOriginal("rO", deps.Clock.Now()))
	store.SeedProductionPointer(release.NewProductionPointer("svc-a", "rProd", "s3://b/svc-a/rProd/manifest.json", "tag-a-prod", release.ManifestKindDbt, time.Unix(0, 0)))

	store.SeedRelease(releasecheck.NewFix("rVerify", "svc-b", "img:1", "rO", 1, "", release.ManifestKindPython, deps.Clock.Now()))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	require.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName)
	uris := manifestKeyURIs(t, entries[0].Payload)
	assert.Equal(t, "s3://b/svc-a/rO/manifest.json", uris["svc-a"],
		"svc-a must be assembled from the rejected release's candidate, not from its production pointer")
	assert.Equal(t, "s3://b/svc-b/rVerify/odcs-bundle.json", uris["svc-b"], "the fix's own project is its own delta")

	r, _ := store.GetRelease("rVerify")
	assert.Equal(t, "tag-a-candidate", r.ImageTags()["svc-a"],
		"the image tag must come from the rejected release too, or the check would execute production's image against the candidate manifest")
	assert.Equal(t, "tag-a-candidate", releaseRequestedImageTags(t, entries[0].Payload)["svc-a"],
		"the verified release's image tag must reach topology-controller too, or the artifact would carry production's image")
}

// TestAdvanceQueue_FixVerifyingAnUnknownRelease_FallsBackToProd
// covers the verified release having been deleted (or never persisted): the
// fix's release check still runs, assembled from the live production pointers
// exactly as any other check is, rather than failing to activate.
func TestAdvanceQueue_FixVerifyingAnUnknownRelease_FallsBackToProd(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedProductionPointer(release.NewProductionPointer("svc-a", "rProd", "s3://b/svc-a/rProd/manifest.json", "tag-a-prod", release.ManifestKindDbt, time.Unix(0, 0)))

	store.SeedRelease(releasecheck.NewFix("rVerify2", "svc-b", "img:1", "rGone", 1, "", release.ManifestKindPython, deps.Clock.Now()))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	uris := manifestKeyURIs(t, entries[0].Payload)
	assert.Equal(t, "s3://b/svc-a/rProd/manifest.json", uris["svc-a"])

	r, _ := store.GetRelease("rVerify2")
	assert.Equal(t, "tag-a-prod", r.ImageTags()["svc-a"])
}

// TestAdvanceQueue_FixVerifyingItsOwnProjectsRelease_ChangesNothing
// covers the ordinary fix's release check, whose fix edits the very project whose
// release was rejected: that project is the fix's own delta
// already, so there is nothing to override.
func TestAdvanceQueue_FixVerifyingItsOwnProjectsRelease_ChangesNothing(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedRelease(verifiedOriginal("rO2", deps.Clock.Now()))

	store.SeedRelease(releasecheck.NewFix("rVerify3", "svc-a", "img:2", "rO2", 1, "", release.ManifestKindPython, deps.Clock.Now()))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	uris := manifestKeyURIs(t, entries[0].Payload)
	assert.Equal(t, "s3://b/svc-a/rVerify3/odcs-bundle.json", uris["svc-a"], "the fix's own delta wins")

	r, _ := store.GetRelease("rVerify3")
	assert.Equal(t, "img:2", r.ImageTags()["svc-a"])
}

// manifestKeyURIs decodes a release.requested payload into project → s3 uri.
func manifestKeyURIs(t *testing.T, payload []byte) map[string]string {
	t.Helper()
	var p struct {
		ManifestKeys []struct {
			Project string `json:"project"`
			S3URI   string `json:"s3_uri"`
		} `json:"manifest_keys"`
	}
	require.NoError(t, json.Unmarshal(payload, &p))
	uris := map[string]string{}
	for _, k := range p.ManifestKeys {
		uris[k.Project] = k.S3URI
	}
	return uris
}

// releaseRequestedImageTags decodes the image_tags map of a release.requested
// payload.
func releaseRequestedImageTags(t *testing.T, payload []byte) map[string]string {
	t.Helper()
	var p struct {
		ImageTags map[string]string `json:"image_tags"`
	}
	require.NoError(t, json.Unmarshal(payload, &p))
	return p.ImageTags
}

// assertKindChangeRejectedAtActivation checks that the queued check releaseID of
// the project named project ended with kind-change refusal text as an invalid_artifact
// rejection, was never parsed, and left the project's production pointer and
// the live release as they were.
func assertKindChangeRejectedAtActivation(t *testing.T, store *fakeStore, releaseID, project, refusal string, row *release.ProductionPointer) {
	t.Helper()
	r := mustGetRelease(t, store, releaseID)
	assert.Equal(t, releasecheck.StatusRejected, r.Status(), "a kind change is rejected, never promoted")
	assert.Equal(t, string(pkg_model.RejectReasonInvalidArtifact), r.FailReason())
	assert.Equal(t, refusal, r.FailDetail())

	assert.Equal(t, row.ManifestKind(), store.GetProductionPointer(project).ManifestKind(), "the project keeps its stored kind")
	assert.Equal(t, row.ReleaseID(), store.GetProductionPointer(project).ReleaseID(), "no production pointer is written")
	assert.Zero(t, store.LiveReleaseUpsertCalls(), "the live release is not moved")
	for _, e := range store.OutboxEntries() {
		assert.NotEqual(t, streams.ReleaseRequestedV1, e.StreamName, "a refused kind change requests no parse")
		assert.NotEqual(t, streams.ReleasePromotedV1, e.StreamName, "a refused kind change is not promoted")
	}

	var rejected struct {
		ReleaseID   string `json:"release_id"`
		Reason      string `json:"reason"`
		ErrorDetail string `json:"error_detail"`
	}
	require.NoError(t, json.Unmarshal(findEntry(t, store, streams.ReleaseRejectedV1).Payload, &rejected))
	assert.Equal(t, releaseID, rejected.ReleaseID)
	assert.Equal(t, string(pkg_model.RejectReasonInvalidArtifact), rejected.Reason)
	assert.Equal(t, refusal, rejected.ErrorDetail)
	assert.Equal(t, "rejected", outcomeOf(t, findEntry(t, store, streams.ReleaseCheckFinishedV1)))
}

// A dbt release of the project may promote between the intake of a queued
// ontology release of the same project and its activation, so activation
// refuses the kind change too: the ontology file would replace the project's
// manifest and drop its nodes.
func TestAdvanceQueue_AnOntologyCheckOfAServiceNowReleasedAsDbtIsRejectedAtActivation(t *testing.T) {
	deps, store := ontologyDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	row := release.NewProductionPointer("svc-x", "rDbt", "s3://b/svc-x/rDbt/manifest.json", "t-dbt", release.ManifestKindDbt, time.Unix(0, 0))
	store.SeedProductionPointer(row)
	store.SeedRelease(releasecheck.NewCandidate("rOnt", "svc-x", "img:1", false, false, "acme/ontology", "cafebabe",
		release.ManifestKindOntology, deps.Clock.Now()))
	store.SeedRelease(releasecheck.NewCandidate("rBehind", "svc-y", "img:2", false, false, "acme/demo", "deadbeef",
		release.ManifestKindDbt, deps.Clock.Now().Add(time.Second)))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	assertKindChangeRejectedAtActivation(t, store, "rOnt", "svc-x",
		`project "svc-x" is released as kind "dbt": a project cannot change between kind "ontology" and a node kind`, row)
	assert.Equal(t, releasecheck.StatusCompiling, mustGetRelease(t, store, "rBehind").Status(),
		"the queue activates the next check once the kind change is rejected")
}

// The ontology project of an install may be released as a node kind by a dbt
// check queued before the ontology was released: activation refuses it, so the
// install's ontology is not silently replaced by a dbt project.
func TestAdvanceQueue_ADbtCheckOfTheOntologyProjectIsRejectedAtActivation(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	row := release.NewProductionPointer("svc-x", "rOnt", "s3://b/svc-x/rOnt/ontology.yaml", "img:1", release.ManifestKindOntology, time.Unix(0, 0))
	store.SeedProductionPointer(row)
	store.SeedRelease(releasecheck.NewCandidate("rDbt", "svc-x", "img:2", false, false, "acme/demo", "deadbeef",
		release.ManifestKindDbt, deps.Clock.Now()))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	assertKindChangeRejectedAtActivation(t, store, "rDbt", "svc-x",
		`project "svc-x" is released as kind "ontology": a project cannot change between kind "ontology" and a node kind`, row)
}

// A check whose kind is on the same side of the line as its project's stored
// kind activates as usual.
func TestAdvanceQueue_ACheckKeepingItsProjectsKindSideIsNotRejected(t *testing.T) {
	deps, store := ontologyDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedProductionPointer(release.NewProductionPointer("svc-x", "rOnt", "s3://b/svc-x/rOnt/ontology.yaml", "img:1", release.ManifestKindOntology, time.Unix(0, 0)))
	store.SeedRelease(releasecheck.NewCandidate("rOnt2", "svc-x", "img:2", false, false, "acme/ontology", "cafebabe",
		release.ManifestKindOntology, deps.Clock.Now()))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	assert.Equal(t, releasecheck.StatusParsing, mustGetRelease(t, store, "rOnt2").Status())
}
