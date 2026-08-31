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

func TestAdvanceQueue_NoActive_ActivatesToCompilingAndEmitsCompileRequested(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "my-bucket"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "tag-a", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rA")
	assert.Equal(t, release.StatusCompiling, r.Status())

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
	assert.Equal(t, release.StatusCompiling, r.Status())
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
	cp := release.NewCurrentProd()
	cp.Update("rProd", topology, time.Unix(0, 0).UTC())
	store.SeedCurrentProd(cp)

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	// svc-b and svc-c are uncovered, so the release stays Received and nothing
	// is emitted; the operator must seed service_prod first.
	r, _ := store.GetRelease("rA")
	assert.Equal(t, release.StatusReceived, r.Status())
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
	cp := release.NewCurrentProd()
	cp.Update("rProd", topology, time.Unix(0, 0).UTC())
	store.SeedCurrentProd(cp)

	// svc-b and svc-c are covered by pointers; svc-a is the changed service.
	store.SeedServiceProd(release.NewServiceProd("svc-b", "rOLD1", "s3://bucket/svc-b/rOLD1/manifest.json", "tag-b-old", release.ManifestKindDbt, time.Unix(0, 0)))
	store.SeedServiceProd(release.NewServiceProd("svc-c", "rOLD2", "s3://bucket/svc-c/rOLD2/manifest.json", "tag-c-old", release.ManifestKindDbt, time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rA")
	assert.Equal(t, release.StatusCompiling, r.Status())

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.CompileRequestedV1, entries[0].StreamName)
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
	assert.Equal(t, release.StatusReceived, rB.Status())
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
	store.SeedServiceProd(release.NewServiceProd("svc-dbt", "rOld", "s3://b/svc-dbt/rOld/manifest.json", "t-old", release.ManifestKindDbt, time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-py", ReleaseID: "rPy", ImageTag: "img:1", Repo: "acme/py", CommitSHA: "cafebabe",
		Kind: "python",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rPy")
	assert.Equal(t, release.StatusParsing, r.Status(), "python releases activate straight into Parsing")
	assert.Equal(t, map[string]string{"svc-py": "img:1", "svc-dbt": "t-old"}, r.ImageTags())

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName, "no compile.requested for a python release")

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
	assert.Equal(t, map[string]string{"svc-py": "python", "svc-dbt": "dbt"}, byService)
	assert.Equal(t, "s3://b/svc-py/rPy/contract.yaml", uris["svc-py"])
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
	assert.Equal(t, release.StatusCompiling, r.Status())
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
	assert.Equal(t, release.StatusCompiling, rOLD.Status())
	assert.Equal(t, release.StatusReceived, rNEW.Status())
}

func TestAdvanceQueue_CompileRequestedCarriesSourceOverlayURI(t *testing.T) {
	deps, store := newDeps(time.Now())
	r := release.New("r-ov", "svc", "tag", false, true, "o/r", "sha", release.ManifestKindDbt, deps.Clock.Now())
	r.SetSourceOverlayURI("s3://b/svc/r-ov/source-overlay.tar.gz")
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
	require.True(t, found, "compile.requested must be emitted for a dbt shadow release")
}

// verifiedOriginal is a rejected candidate release of svc-a, parsed (so its
// candidate manifest exists at its canonical key) and carrying its own image
// tag for svc-a. A shadow release verifying it must assemble THAT manifest for
// svc-a rather than svc-a's live production pointer.
func verifiedOriginal(id string, now time.Time) *release.Release {
	return release.Rehydrate(release.RehydrateInput{
		ID:             id,
		Status:         release.StatusRejected,
		ImageTags:      map[string]string{"svc-a": "tag-a-candidate", "svc-b": "tag-b-old"},
		ChangedService: "svc-a",
		CandidateTopology: release.Topology{
			{UniqueID: "s.a", ServiceName: "svc-a", OriginalFilePath: "models/a.sql"},
		},
		ManifestKind: release.ManifestKindDbt,
		CreatedAt:    now,
	})
}

// TestAdvanceQueue_ShadowVerifiesTheRejectedReleasesCandidate covers the fix
// whose edit lands in a DIFFERENT service than the one whose release was
// rejected: the rejection came from svc-a's candidate, so verifying the fix
// against svc-a's PRODUCTION manifest would judge it on code the rejection was
// never about. The shadow names the release it verifies, and svc-a is assembled
// from that release's candidate — manifest key and image tag both.
func TestAdvanceQueue_ShadowVerifiesTheRejectedReleasesCandidate(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedRelease(verifiedOriginal("rO", deps.Clock.Now()))
	store.SeedServiceProd(release.NewServiceProd("svc-a", "rProd", "s3://b/svc-a/rProd/manifest.json", "tag-a-prod", release.ManifestKindDbt, time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-b", ReleaseID: "rShadow", ImageTag: "img:1", Repo: "acme/py", CommitSHA: "cafebabe",
		Kind: "python", Shadow: true, VerifiesReleaseID: "rO",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	require.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName)
	uris := manifestKeyURIs(t, entries[0].Payload)
	assert.Equal(t, "s3://b/svc-a/rO/manifest.json", uris["svc-a"],
		"svc-a must be assembled from the rejected release's candidate, not from its production pointer")
	assert.Equal(t, "s3://b/svc-b/rShadow/contract.yaml", uris["svc-b"], "the shadow's own service is its own delta")

	r, _ := store.GetRelease("rShadow")
	assert.Equal(t, "tag-a-candidate", r.ImageTags()["svc-a"],
		"the image tag must come from the rejected release too, or the run would execute production's image against the candidate manifest")
}

// TestAdvanceQueue_ShadowVerifyingAnUnknownRelease_FallsBackToProd covers the
// verified release having been deleted (or never persisted): the shadow still
// runs, assembled from the live production pointers exactly as any other
// release is, rather than failing to activate.
func TestAdvanceQueue_ShadowVerifyingAnUnknownRelease_FallsBackToProd(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedServiceProd(release.NewServiceProd("svc-a", "rProd", "s3://b/svc-a/rProd/manifest.json", "tag-a-prod", release.ManifestKindDbt, time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-b", ReleaseID: "rShadow2", ImageTag: "img:1", Repo: "acme/py", CommitSHA: "cafebabe",
		Kind: "python", Shadow: true, VerifiesReleaseID: "rGone",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	uris := manifestKeyURIs(t, entries[0].Payload)
	assert.Equal(t, "s3://b/svc-a/rProd/manifest.json", uris["svc-a"])

	r, _ := store.GetRelease("rShadow2")
	assert.Equal(t, "tag-a-prod", r.ImageTags()["svc-a"])
}

// TestAdvanceQueue_ShadowVerifyingItsOwnServicesRelease_ChangesNothing covers
// the ordinary shadow, whose fix edits the very service whose release was
// rejected: that service is the shadow's own delta already, so there is nothing
// to override.
func TestAdvanceQueue_ShadowVerifyingItsOwnServicesRelease_ChangesNothing(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	store.SeedRelease(verifiedOriginal("rO2", deps.Clock.Now()))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rShadow3", ImageTag: "img:2", Repo: "acme/py", CommitSHA: "cafebabe",
		Kind: "python", Shadow: true, VerifiesReleaseID: "rO2",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	uris := manifestKeyURIs(t, entries[0].Payload)
	assert.Equal(t, "s3://b/svc-a/rShadow3/contract.yaml", uris["svc-a"], "the shadow's own delta wins")

	r, _ := store.GetRelease("rShadow3")
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
