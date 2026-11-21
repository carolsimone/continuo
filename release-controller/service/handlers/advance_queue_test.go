package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
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

func TestAdvanceQueue_NoActive_ActivatesToCompilingAndEmitsCompileRequested(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "my-bucket"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "tag-a", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rA")
	assert.Equal(t, pipeline.StatusCompiling, r.Status())

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.CompileRequestedV1, entries[0].StreamName)

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entries[0].Payload, &payload))

	// Must carry release_id, service, image_tag, bucket; must NOT carry manifest_keys.
	var releaseID string
	require.NoError(t, json.Unmarshal(payload["release_id"], &releaseID))
	assert.Equal(t, "rA", releaseID)

	var service string
	require.NoError(t, json.Unmarshal(payload["service"], &service))
	assert.Equal(t, "svc-a", service)

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

func TestAdvanceQueue_OtherServicesIncluded_ImageTagsAssembledOnRelease(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "bucket"

	// Pre-seed two other services' production pointers.
	store.SeedServiceProd(release.NewServiceProd("svc-b", "rOLD1", "s3://bucket/svc-b/rOLD1/manifest.json", "tag-b-old", release.ManifestKindDbt, time.Unix(0, 0)))
	store.SeedServiceProd(release.NewServiceProd("svc-c", "rOLD2", "s3://bucket/svc-c/rOLD2/manifest.json", "tag-c-old", release.ManifestKindDbt, time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rNEW", ImageTag: "tag-a-new", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.CompileRequestedV1, entries[0].StreamName)

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entries[0].Payload, &payload))

	// compile.requested carries the changed service + its image_tag.
	var service string
	require.NoError(t, json.Unmarshal(payload["service"], &service))
	assert.Equal(t, "svc-a", service)

	var imageTag string
	require.NoError(t, json.Unmarshal(payload["image_tag"], &imageTag))
	assert.Equal(t, "tag-a-new", imageTag)

	// Assembled image tags must already be persisted on the release (SetAssembledImageTags).
	r, _ := store.GetRelease("rNEW")
	assert.Equal(t, pipeline.StatusCompiling, r.Status())
	assert.Equal(t, map[string]string{
		"svc-a": "tag-a-new",
		"svc-b": "tag-b-old",
		"svc-c": "tag-c-old",
	}, r.ImageTags())
}

func TestAdvanceQueue_ProdSeeded_UncoveredService_BlocksActivation(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "bucket"

	// current_prod lists three live services, but no service_prod pointers exist
	// (the state right after an upgrade from the whole-snapshot model).
	topology := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a"},
		{UniqueID: "b", ServiceName: "svc-b"},
		{UniqueID: "c", ServiceName: "svc-c"},
	}
	seedProd(t, deps, store, "rProd", topology, time.Unix(0, 0).UTC())

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	// svc-b and svc-c are uncovered, so the release stays Received and nothing
	// is emitted; the operator must seed service_prod first.
	r, _ := store.GetRelease("rA")
	assert.Equal(t, pipeline.StatusReceived, r.Status())
	assert.Empty(t, outboxEntries(store))
}

func TestAdvanceQueue_ProdSeeded_AllServicesCovered_Proceeds(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "bucket"

	topology := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a"},
		{UniqueID: "b", ServiceName: "svc-b"},
		{UniqueID: "c", ServiceName: "svc-c"},
	}
	seedProd(t, deps, store, "rProd", topology, time.Unix(0, 0).UTC())

	// svc-b and svc-c are covered by pointers; svc-a is the changed service.
	store.SeedServiceProd(release.NewServiceProd("svc-b", "rOLD1", "s3://bucket/svc-b/rOLD1/manifest.json", "tag-b-old", release.ManifestKindDbt, time.Unix(0, 0)))
	store.SeedServiceProd(release.NewServiceProd("svc-c", "rOLD2", "s3://bucket/svc-c/rOLD2/manifest.json", "tag-c-old", release.ManifestKindDbt, time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rA")
	assert.Equal(t, pipeline.StatusCompiling, r.Status())

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.CompileRequestedV1, entries[0].StreamName)
}

// TestUncoveredProdServices_IgnoresUnknownExternalNodes pins that the live production
// topology's unknown-external nodes do not count as a service without a service_prod
// pointer: an unknown-external node has no service, so no pointer can ever cover it,
// and counting it would hold every release in the queue.
func TestUncoveredProdServices_IgnoresUnknownExternalNodes(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "bucket"

	seedProd(t, deps, store, "rProd", release.Topology{
		{UniqueID: "raw.orders", SchemaName: "raw", TableName: "orders", ResolvedRelationID: "raw.orders", NodeType: "unknown-external", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-b", NodeType: "dbt-model", UpstreamUniqueIDs: []string{"raw.orders"}},
	}, time.Unix(0, 0).UTC())
	store.SeedServiceProd(release.NewServiceProd("svc-b", "rOLD1", "s3://bucket/svc-b/rOLD1/manifest.json", "tag-b-old", release.ManifestKindDbt, time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rA")
	assert.Equal(t, pipeline.StatusCompiling, r.Status(), "svc-b is covered and the unknown-external node needs no pointer")
}

// TestUncoveredProdServices_StillBlocksOnAnUncoveredServiceBesideAnUnknownExternalNode
// is the control: an unknown-external node is skipped, a real service without a
// pointer still holds the release.
func TestUncoveredProdServices_StillBlocksOnAnUncoveredServiceBesideAnUnknownExternalNode(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "bucket"

	seedProd(t, deps, store, "rProd", release.Topology{
		{UniqueID: "raw.orders", SchemaName: "raw", TableName: "orders", ResolvedRelationID: "raw.orders", NodeType: "unknown-external", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-b", NodeType: "dbt-model", UpstreamUniqueIDs: []string{"raw.orders"}},
	}, time.Unix(0, 0).UTC())

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rA")
	assert.Equal(t, pipeline.StatusReceived, r.Status(), "svc-b has no pointer")
}

func TestAdvanceQueue_ActiveExists_DoesNothing(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc", ReleaseID: "rB", ImageTag: "t2", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	rB, _ := store.GetRelease("rB")
	assert.Equal(t, pipeline.StatusReceived, rB.Status())
	assert.Len(t, outboxEntries(store), 1) // only rA emitted
}

func TestAdvanceQueue_NoQueued_DoesNothing(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	assert.Empty(t, outboxEntries(store))
}

func TestAdvanceQueue_PythonODCSRelease_SkipsCompile_EmitsReleaseRequested(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedServiceProd(release.NewServiceProd("svc-dbt", "rOld", "s3://b/svc-dbt/rOld/manifest.json", "t-old", release.ManifestKindDbt, time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-py", ReleaseID: "rPy", ImageTag: "img:1", Repo: "acme/py", CommitSHA: "cafebabe",
		Kind: "python-odcs",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rPy")
	assert.Equal(t, release.ManifestKindPythonODCS, r.ManifestKind())
	assert.Equal(t, pipeline.StatusParsing, r.Status(), "python-odcs releases activate straight into Parsing")
	assert.Equal(t, map[string]string{"svc-py": "img:1", "svc-dbt": "t-old"}, r.ImageTags())

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName, "no compile.requested for a python-odcs release")

	var p struct {
		ReleaseID    string `json:"release_id"`
		ManifestKeys []struct {
			Service string `json:"service"`
			S3URI   string `json:"s3_uri"`
			Kind    string `json:"kind"`
		} `json:"manifest_keys"`
	}
	require.NoError(t, json.Unmarshal(entries[0].Payload, &p))
	assert.Equal(t, "rPy", p.ReleaseID)
	require.Len(t, p.ManifestKeys, 2)
	byService := map[string]string{}
	uris := map[string]string{}
	for _, k := range p.ManifestKeys {
		byService[k.Service] = k.Kind
		uris[k.Service] = k.S3URI
	}
	assert.Equal(t, map[string]string{"svc-py": "python-odcs", "svc-dbt": "dbt"}, byService)
	assert.Equal(t, "s3://b/svc-py/rPy/odcs-bundle.json", uris["svc-py"])
	assert.Equal(t, "s3://b/svc-dbt/rOld/manifest.json", uris["svc-dbt"])
	assert.Equal(t, map[string]string{"svc-py": "img:1", "svc-dbt": "t-old"}, releaseRequestedImageTags(t, entries[0].Payload),
		"release.requested carries the run's assembled image tags: topology-controller joins exactly these onto every node")
}

// The ontology service ships its ontology file before the release is
// requested, so its release has no compile leg: it activates straight into
// Parsing, and release.requested names the file as the service's artifact.
func TestAdvanceQueue_AnOntologyReleaseGoesStraightToParsing(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedServiceProd(release.NewServiceProd("svc-dbt", "rOld", "s3://b/svc-dbt/rOld/manifest.json", "t-old", release.ManifestKindDbt, time.Unix(0, 0)))
	store.SeedRelease(pipeline.NewCandidate("rOnt", "company-ontology", "img:1", false, false, "acme/ontology", "cafebabe",
		release.ManifestKindOntology, deps.Clock.Now()))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rOnt")
	assert.Equal(t, release.ManifestKindOntology, r.ManifestKind())
	assert.Equal(t, pipeline.StatusParsing, r.Status(), "an ontology release has no compile leg")

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName, "no compile.requested for an ontology release")

	var p struct {
		ManifestKeys []struct {
			Service string `json:"service"`
			S3URI   string `json:"s3_uri"`
			Kind    string `json:"kind"`
		} `json:"manifest_keys"`
	}
	require.NoError(t, json.Unmarshal(entries[0].Payload, &p))
	kinds := map[string]string{}
	uris := map[string]string{}
	for _, k := range p.ManifestKeys {
		kinds[k.Service] = k.Kind
		uris[k.Service] = k.S3URI
	}
	assert.Equal(t, map[string]string{"company-ontology": "ontology", "svc-dbt": "dbt"}, kinds)
	assert.Equal(t, "s3://b/company-ontology/rOnt/ontology.yaml", uris["company-ontology"])
	assert.Equal(t, "s3://b/svc-dbt/rOld/manifest.json", uris["svc-dbt"])
}

func TestAdvanceQueue_DbtRelease_StillCompiles(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rDbt", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	r, _ := store.GetRelease("rDbt")
	assert.Equal(t, pipeline.StatusCompiling, r.Status())
	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.CompileRequestedV1, entries[0].StreamName)
}

func TestAdvanceQueue_PicksOldestFirst(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc", ReleaseID: "rOLD", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc", ReleaseID: "rNEW", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	rOLD, _ := store.GetRelease("rOLD")
	rNEW, _ := store.GetRelease("rNEW")
	assert.Equal(t, pipeline.StatusCompiling, rOLD.Status())
	assert.Equal(t, pipeline.StatusReceived, rNEW.Status())
}

func TestAdvanceQueue_CompileRequestedCarriesSourceOverlayURI(t *testing.T) {
	deps, store := newDeps(time.Now())
	r := pipeline.NewVerification("r-ov", "svc", "tag", "rel-orig", 1,
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
	require.True(t, found, "compile.requested must be emitted for a dbt verification run")
}

// verifiedOriginal is a rejected candidate release of svc-a, parsed (so its
// candidate manifest exists at its canonical key) and carrying its own image
// tag for svc-a. A verification run verifying it must assemble THAT manifest
// for svc-a rather than svc-a's live production pointer.
func verifiedOriginal(id string, now time.Time) *pipeline.Run {
	return pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:             id,
		Status:         pipeline.StatusRejected,
		ImageTags:      map[string]string{"svc-a": "tag-a-candidate", "svc-b": "tag-b-old"},
		ChangedService: "svc-a",
		// Activation only needs to know the verified release was parsed; it
		// never reads the artifact.
		CandidateTopology: release.TopologyRef{
			URI: "s3://b/tenants/default/topologies/" + id + "/topology.json.gz", SHA256: "sha-" + id, NodeCount: 1,
		},
		ManifestKind: release.ManifestKindDbt,
		CreatedAt:    now,
	})
}

// TestAdvanceQueue_VerificationVerifiesTheRejectedReleasesCandidate covers the
// fix whose edit lands in a DIFFERENT service than the one whose release was
// rejected: the rejection came from svc-a's candidate, so verifying the fix
// against svc-a's PRODUCTION manifest would judge it on code the rejection was
// never about. The verification names the release it verifies, and svc-a is
// assembled from that release's candidate — manifest key and image tag both.
func TestAdvanceQueue_VerificationVerifiesTheRejectedReleasesCandidate(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedRelease(verifiedOriginal("rO", deps.Clock.Now()))
	store.SeedServiceProd(release.NewServiceProd("svc-a", "rProd", "s3://b/svc-a/rProd/manifest.json", "tag-a-prod", release.ManifestKindDbt, time.Unix(0, 0)))

	store.SeedRelease(pipeline.NewVerification("rVerify", "svc-b", "img:1", "rO", 1, "", release.ManifestKindPythonODCS, deps.Clock.Now()))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	require.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName)
	uris := manifestKeyURIs(t, entries[0].Payload)
	assert.Equal(t, "s3://b/svc-a/rO/manifest.json", uris["svc-a"],
		"svc-a must be assembled from the rejected release's candidate, not from its production pointer")
	assert.Equal(t, "s3://b/svc-b/rVerify/odcs-bundle.json", uris["svc-b"], "the verification run's own service is its own delta")

	r, _ := store.GetRelease("rVerify")
	assert.Equal(t, "tag-a-candidate", r.ImageTags()["svc-a"],
		"the image tag must come from the rejected release too, or the run would execute production's image against the candidate manifest")
	assert.Equal(t, "tag-a-candidate", releaseRequestedImageTags(t, entries[0].Payload)["svc-a"],
		"the verified release's image tag must reach topology-controller too, or the artifact would carry production's image")
}

// TestAdvanceQueue_VerificationVerifyingAnUnknownRelease_FallsBackToProd
// covers the verified release having been deleted (or never persisted): the
// verification still runs, assembled from the live production pointers
// exactly as any other run is, rather than failing to activate.
func TestAdvanceQueue_VerificationVerifyingAnUnknownRelease_FallsBackToProd(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedServiceProd(release.NewServiceProd("svc-a", "rProd", "s3://b/svc-a/rProd/manifest.json", "tag-a-prod", release.ManifestKindDbt, time.Unix(0, 0)))

	store.SeedRelease(pipeline.NewVerification("rVerify2", "svc-b", "img:1", "rGone", 1, "", release.ManifestKindPythonODCS, deps.Clock.Now()))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	uris := manifestKeyURIs(t, entries[0].Payload)
	assert.Equal(t, "s3://b/svc-a/rProd/manifest.json", uris["svc-a"])

	r, _ := store.GetRelease("rVerify2")
	assert.Equal(t, "tag-a-prod", r.ImageTags()["svc-a"])
}

// TestAdvanceQueue_VerificationVerifyingAStoredPythonRelease_FallsBackToProd
// covers a verified release stored with the kind python: its candidate
// artifact is a contract.yaml no parser reads, and its canonical key would
// name a manifest.json that does not exist, so the verification is assembled
// from the live production pointers, as for a release that never parsed.
func TestAdvanceQueue_VerificationVerifyingAStoredPythonRelease_FallsBackToProd(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	var logBuf bytes.Buffer
	deps.Logger = slog.New(slog.NewTextHandler(&logBuf, nil))
	original := verifiedOriginal("rOPy", deps.Clock.Now())
	store.SeedRelease(pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:                original.ID(),
		Status:            original.Status(),
		ImageTags:         original.ImageTags(),
		ChangedService:    original.ChangedService(),
		CandidateTopology: original.CandidateTopologyRef(),
		ManifestKind:      release.ManifestKindPython,
		CreatedAt:         original.CreatedAt(),
	}))
	store.SeedServiceProd(release.NewServiceProd("svc-a", "rProd", "s3://b/svc-a/rProd/odcs-bundle.json", "tag-a-prod", release.ManifestKindPythonODCS, time.Unix(0, 0)))

	store.SeedRelease(pipeline.NewVerification("rVerifyPy", "svc-b", "img:1", "rOPy", 1, "", release.ManifestKindPythonODCS, deps.Clock.Now()))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	require.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName)
	uris := manifestKeyURIs(t, entries[0].Payload)
	assert.Equal(t, "s3://b/svc-a/rProd/odcs-bundle.json", uris["svc-a"],
		"a verified release of the kind python contributes no candidate key")
	assert.NotContains(t, string(entries[0].Payload), `"kind":"python"`,
		"no manifest key of the kind python reaches topology-controller")

	r, _ := store.GetRelease("rVerifyPy")
	assert.Equal(t, "tag-a-prod", r.ImageTags()["svc-a"])
	assert.Contains(t, logBuf.String(), "assembling from production instead")
}

// TestAdvanceQueue_VerificationVerifyingItsOwnServicesRelease_ChangesNothing
// covers the ordinary verification, whose fix edits the very service whose
// release was rejected: that service is the verification's own delta
// already, so there is nothing to override.
func TestAdvanceQueue_VerificationVerifyingItsOwnServicesRelease_ChangesNothing(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedRelease(verifiedOriginal("rO2", deps.Clock.Now()))

	store.SeedRelease(pipeline.NewVerification("rVerify3", "svc-a", "img:2", "rO2", 1, "", release.ManifestKindPythonODCS, deps.Clock.Now()))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	uris := manifestKeyURIs(t, entries[0].Payload)
	assert.Equal(t, "s3://b/svc-a/rVerify3/odcs-bundle.json", uris["svc-a"], "the verification run's own delta wins")

	r, _ := store.GetRelease("rVerify3")
	assert.Equal(t, "img:2", r.ImageTags()["svc-a"])
}

// manifestKeyURIs decodes a release.requested payload into service → s3 uri.
func manifestKeyURIs(t *testing.T, payload []byte) map[string]string {
	t.Helper()
	var p struct {
		ManifestKeys []struct {
			Service string `json:"service"`
			S3URI   string `json:"s3_uri"`
		} `json:"manifest_keys"`
	}
	require.NoError(t, json.Unmarshal(payload, &p))
	uris := map[string]string{}
	for _, k := range p.ManifestKeys {
		uris[k.Service] = k.S3URI
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

// assertKindChangeRejectedAtActivation checks that the queued run releaseID of
// the service service ended with kind-change refusal text as an invalid_artifact
// rejection, was never parsed, and left the service's service_prod row and
// current_prod as they were.
func assertKindChangeRejectedAtActivation(t *testing.T, store *fakeStore, releaseID, service, refusal string, row *release.ServiceProd) {
	t.Helper()
	r := mustGetRelease(t, store, releaseID)
	assert.Equal(t, pipeline.StatusRejected, r.Status(), "a kind change is rejected, never promoted")
	assert.Equal(t, string(pkg_model.RejectReasonInvalidArtifact), r.FailReason())
	assert.Equal(t, refusal, r.FailDetail())

	assert.Equal(t, row.ManifestKind(), store.GetServiceProd(service).ManifestKind(), "the service keeps its stored kind")
	assert.Equal(t, row.ReleaseID(), store.GetServiceProd(service).ReleaseID(), "no service_prod row is written")
	assert.Zero(t, store.CurrentProdUpsertCalls(), "current_prod is not moved")
	for _, e := range store.OutboxEntries() {
		assert.NotEqual(t, streams.ReleaseRequestedV1, e.StreamName, "a refused kind change requests no parse")
		assert.NotEqual(t, streams.ReleasePromotedV2, e.StreamName, "a refused kind change is not promoted")
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
	assert.Equal(t, "rejected", outcomeOf(t, findEntry(t, store, streams.PipelineRunFinishedV1)))
}

// A dbt release of the service may promote between the intake of a queued
// ontology release of the same service and its activation, so activation
// refuses the kind change too: the ontology file would replace the service's
// manifest and drop its nodes.
func TestAdvanceQueue_AnOntologyRunOfAServiceNowReleasedAsDbtIsRejectedAtActivation(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	row := release.NewServiceProd("svc-x", "rDbt", "s3://b/svc-x/rDbt/manifest.json", "t-dbt", release.ManifestKindDbt, time.Unix(0, 0))
	store.SeedServiceProd(row)
	store.SeedRelease(pipeline.NewCandidate("rOnt", "svc-x", "img:1", false, false, "acme/ontology", "cafebabe",
		release.ManifestKindOntology, deps.Clock.Now()))
	store.SeedRelease(pipeline.NewCandidate("rBehind", "svc-y", "img:2", false, false, "acme/demo", "deadbeef",
		release.ManifestKindDbt, deps.Clock.Now().Add(time.Second)))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	assertKindChangeRejectedAtActivation(t, store, "rOnt", "svc-x",
		`service "svc-x" is released as kind "dbt": a service cannot change between kind "ontology" and a node kind`, row)
	assert.Equal(t, pipeline.StatusCompiling, mustGetRelease(t, store, "rBehind").Status(),
		"the queue activates the next run once the kind change is rejected")
}

// The ontology service of an install may be released as a node kind by a dbt
// run queued before the ontology was released: activation refuses it, so the
// install's ontology is not silently replaced by a dbt service.
func TestAdvanceQueue_ADbtRunOfTheOntologyServiceIsRejectedAtActivation(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	row := release.NewServiceProd("svc-x", "rOnt", "s3://b/svc-x/rOnt/ontology.yaml", "img:1", release.ManifestKindOntology, time.Unix(0, 0))
	store.SeedServiceProd(row)
	store.SeedRelease(pipeline.NewCandidate("rDbt", "svc-x", "img:2", false, false, "acme/demo", "deadbeef",
		release.ManifestKindDbt, deps.Clock.Now()))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	assertKindChangeRejectedAtActivation(t, store, "rDbt", "svc-x",
		`service "svc-x" is released as kind "ontology": a service cannot change between kind "ontology" and a node kind`, row)
}

// A run whose kind is on the same side of the line as its service's stored
// kind activates as usual.
func TestAdvanceQueue_ARunKeepingItsServicesKindSideIsNotRejected(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedServiceProd(release.NewServiceProd("svc-x", "rOnt", "s3://b/svc-x/rOnt/ontology.yaml", "img:1", release.ManifestKindOntology, time.Unix(0, 0)))
	store.SeedRelease(pipeline.NewCandidate("rOnt2", "svc-x", "img:2", false, false, "acme/ontology", "cafebabe",
		release.ManifestKindOntology, deps.Clock.Now()))

	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	assert.Equal(t, pipeline.StatusParsing, mustGetRelease(t, store, "rOnt2").Status())
}
