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

func TestAdvanceQueue_FlagTrue_ActivatesToCompilingAndEmitsCompileRequested(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "my-bucket"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "tag-a", Repo: "acme/demo", CommitSHA: "deadbeef",
		CompileInContinuo: true,
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

	_, hasManifestKeys := payload["manifest_keys"]
	assert.False(t, hasManifestKeys, "manifest_keys must not appear in compile.requested payload")
	_, hasManifestsURI := payload["manifests_uri"]
	assert.False(t, hasManifestsURI, "manifests_uri must not appear in the payload")
}

func TestAdvanceQueue_FlagFalse_ActivatesToParsingAndEmitsReleaseRequested(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "my-bucket"

	store.SeedServiceProd(release.NewServiceProd("svc-a", "rOLD0", "s3://my-bucket/svc-a/rOLD0/manifest.json", "tag-a-old", time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "tag-a", Repo: "acme/demo", CommitSHA: "deadbeef",
		// CompileInContinuo defaults to false
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

	r, _ := store.GetRelease("rA")
	assert.Equal(t, release.StatusParsing, r.Status())

	entries := outboxEntries(store)
	require.Len(t, entries, 1)
	assert.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName)

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entries[0].Payload, &payload))

	var releaseID string
	require.NoError(t, json.Unmarshal(payload["release_id"], &releaseID))
	assert.Equal(t, "rA", releaseID)

	// manifest_keys must be present and contain at least the changed service's key.
	var keys []struct {
		Service string `json:"service"`
		S3URI   string `json:"s3_uri"`
	}
	require.NoError(t, json.Unmarshal(payload["manifest_keys"], &keys))
	require.NotEmpty(t, keys)
	found := false
	for _, k := range keys {
		if k.Service == "svc-a" {
			found = true
			// Canonical S3 URI: s3://<bucket>/<service>/<releaseID>/manifest.json
			assert.Equal(t, "s3://my-bucket/svc-a/rA/manifest.json", k.S3URI)
		}
	}
	assert.True(t, found, "changed service svc-a must appear in manifest_keys")

	_, hasCompileFields := payload["service"]
	assert.False(t, hasCompileFields, "service field must not appear in release.requested payload")
}

func TestAdvanceQueue_OtherServicesIncluded_ImageTagsAssembledOnRelease(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "bucket"

	// Pre-seed two other services' production pointers.
	store.SeedServiceProd(release.NewServiceProd("svc-b", "rOLD1", "s3://bucket/svc-b/rOLD1/manifest.json", "tag-b-old", time.Unix(0, 0)))
	store.SeedServiceProd(release.NewServiceProd("svc-c", "rOLD2", "s3://bucket/svc-c/rOLD2/manifest.json", "tag-c-old", time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rNEW", ImageTag: "tag-a-new", Repo: "acme/demo", CommitSHA: "deadbeef",
		CompileInContinuo: true,
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
		CompileInContinuo: true,
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
	store.SeedServiceProd(release.NewServiceProd("svc-b", "rOLD1", "s3://bucket/svc-b/rOLD1/manifest.json", "tag-b-old", time.Unix(0, 0)))
	store.SeedServiceProd(release.NewServiceProd("svc-c", "rOLD2", "s3://bucket/svc-c/rOLD2/manifest.json", "tag-c-old", time.Unix(0, 0)))

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
		CompileInContinuo: true,
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
		CompileInContinuo: true,
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc", ReleaseID: "rB", ImageTag: "t2", Repo: "acme/demo", CommitSHA: "deadbeef",
		CompileInContinuo: true,
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

func TestAdvanceQueue_PicksOldestFirst(t *testing.T) {
	deps, store := newDeps(time.Unix(200, 0).UTC())
	deps.Bucket = "b"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc", ReleaseID: "rOLD", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
		CompileInContinuo: true,
	}))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc", ReleaseID: "rNEW", ImageTag: "t", Repo: "acme/demo", CommitSHA: "deadbeef",
		CompileInContinuo: true,
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	rOLD, _ := store.GetRelease("rOLD")
	rNEW, _ := store.GetRelease("rNEW")
	assert.Equal(t, release.StatusCompiling, rOLD.Status())
	assert.Equal(t, release.StatusReceived, rNEW.Status())
}
