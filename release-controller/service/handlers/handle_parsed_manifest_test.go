package handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedToParsing advances a release from Received to Parsing via
// ReceiveCandidate → AdvanceQueue → HandleCompileResult(ok).
// imageTags must have exactly one entry: {changedService: imageTag}. Returns the deps and fakeStore.
func seedToParsing(t *testing.T, releaseID string, imageTags map[string]string) (*handlers.Deps, *fakeStore) {
	t.Helper()
	if len(imageTags) != 1 {
		t.Fatal("seedToParsing: imageTags must have exactly one entry {service: tag}")
	}
	var svc, tag string
	for s, tg := range imageTags {
		svc, tag = s, tg
	}
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   svc,
		ReleaseID: releaseID,
		ImageTag:  tag,
		Repo:      "acme/demo",
		CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	// Simulate the compile leg completing (Compiling → Parsing).
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: releaseID,
		Status:    "ok",
	}))
	return deps, store
}

// seedToParsingBootstrap is seedToParsing for a bootstrap release.
// imageTags may have multiple entries when the test topology has cross-service nodes;
// only the first entry is used as the changed service, others are pre-seeded as service_prod.
func seedToParsingBootstrap(t *testing.T, releaseID string, imageTags map[string]string) (*handlers.Deps, *fakeStore) {
	t.Helper()
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	// Pick a stable "changed service" for bootstrap (use lexically-first service).
	var changedSvc, changedTag string
	for s, tg := range imageTags {
		if changedSvc == "" || s < changedSvc {
			changedSvc, changedTag = s, tg
		}
	}
	// Pre-seed other services so AdvanceQueue can assemble the full image-tag map.
	for s, tg := range imageTags {
		if s == changedSvc {
			continue
		}
		store.SeedServiceProd(release.NewServiceProd(s, releaseID+"-prev", "s3://continuo/"+s+"/prev/manifest.json", tg, release.ManifestKindDbt, time.Unix(0, 0)))
	}

	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service:   changedSvc,
		ReleaseID: releaseID,
		ImageTag:  changedTag,
		Bootstrap: true,
		Repo:      "acme/demo",
		CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	// Simulate the compile leg completing (Compiling → Parsing).
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: releaseID,
		Status:    "ok",
	}))
	return deps, store
}

func TestHandleParsedManifest_OK_TransitionsToValidating(t *testing.T) {
	// Single-service topology avoids cross-service upstream rejection in bootstrap
	// (no prod snapshot means every cross-service upstream would be "new").
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-a", UpstreamUniqueIDs: []string{"a"}},
	}
	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	})
	require.NoError(t, err)

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusValidating, r.Status())

	validIDs := r.ValidationNodeIDs()
	assert.Contains(t, validIDs, "a")
	assert.Contains(t, validIDs, "b")

	entries := outboxEntries(store)
	require.Len(t, entries, 3) // CompileRequested + ReleaseRequested + ValidationRequested

	last := entries[2]
	assert.Equal(t, streams.ValidationRequestedV1, last.StreamName)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(last.Payload, &payload))
	assert.Equal(t, "rA", payload["release_id"])
	assert.Equal(t, "validation", payload["mode"])
}

// TestHandleParsedManifest_OK_StoresCodeBundleURI verifies that handleParseOK
// persists CodeBundleURI (published by topology-controller on
// manifest.loaded.candidate:v1) onto the saved release on the normal
// (non-bootstrap) validating path, so it survives to be carried on
// release.promoted:v1 once validation passes.
func TestHandleParsedManifest_OK_StoresCodeBundleURI(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-a", UpstreamUniqueIDs: []string{"a"}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:     "rA",
		Status:        "ok",
		Topology:      topo,
		CodeBundleURI: "s3://continuo/code-bundles/rA/bundle.json",
	}))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusValidating, r.Status())
	assert.Equal(t, "s3://continuo/code-bundles/rA/bundle.json", r.CodeBundleURI())
}

// TestHandleParsedManifest_Bootstrap_StoresCodeBundleURI verifies that
// handleParseOK persists CodeBundleURI on the bootstrap path (promoteBootstrap),
// which skips validation entirely but still goes through the same
// r.SetCodeBundleURI call right after joinImageTags.
func TestHandleParsedManifest_Bootstrap_StoresCodeBundleURI(t *testing.T) {
	deps, store := seedToParsingBootstrap(t, "rBoot", map[string]string{"svc-a": "sha-a"})

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:     "rBoot",
		Status:        "ok",
		Topology:      topo,
		CodeBundleURI: "s3://continuo/code-bundles/rBoot/bundle.json",
	}))

	r, err := store.GetRelease("rBoot")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusPromoted, r.Status())
	assert.Equal(t, "s3://continuo/code-bundles/rBoot/bundle.json", r.CodeBundleURI())
}

func TestHandleParsedManifest_Failed_TransitionsToRejected(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})

	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:   "rA",
		Status:      "failed",
		ErrorClass:  "UnresolvedReference",
		ErrorDetail: "ref('missing') unresolved in service_1.table_a",
	})
	require.NoError(t, err)

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Equal(t, "parse_failed", r.FailReason())

	entries := outboxEntries(store)
	require.Len(t, entries, 3) // CompileRequested + ReleaseRequested + ReleaseRejected

	last := entries[2]
	assert.Equal(t, streams.ReleaseRejectedV1, last.StreamName)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(last.Payload, &payload))
	assert.Equal(t, false, payload["shadow"],
		"a non-shadow release's parse_failed rejection must carry shadow:false")
}

// seedToParsingShadow mirrors seedToParsing but seeds a verification run, so
// a rejection emitted from the parsing leg can be asserted to carry
// shadow:true — the signal remediation uses to avoid re-triggering itself on
// a failed fix-verification attempt.
func seedToParsingShadow(t *testing.T, releaseID string, imageTags map[string]string) (*handlers.Deps, *fakeStore) {
	t.Helper()
	return seedToParsingShadowWithOverlay(t, releaseID, imageTags, "")
}

// seedToParsingShadowWithOverlay is seedToParsingShadow for a shadow release
// that carries a source overlay: the S3 tarball of proposed source files the
// release's dbt Jobs lay over the checked-in project before running. An empty
// overlayURI registers the shadow without one.
func seedToParsingShadowWithOverlay(t *testing.T, releaseID string, imageTags map[string]string, overlayURI string) (*handlers.Deps, *fakeStore) {
	t.Helper()
	if len(imageTags) != 1 {
		t.Fatal("seedToParsingShadow: imageTags must have exactly one entry {service: tag}")
	}
	var svc, tag string
	for s, tg := range imageTags {
		svc, tag = s, tg
	}
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"
	store.SeedRelease(pipeline.NewVerification(releaseID, svc, tag, "", 1, overlayURI, release.ManifestKindDbt, deps.Clock.Now()))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: releaseID,
		Status:    "ok",
	}))
	return deps, store
}

// TestHandleParsedManifest_Failed_Shadow_CarriesShadowTrue verifies that a
// shadow release's parse_failed rejection carries shadow:true on
// release.rejected:v1, so a consumer (remediation) can tell this failure came
// from a shadow fix-verification attempt and must not re-trigger remediation
// on it — re-triggering would loop.
func TestHandleParsedManifest_Failed_Shadow_CarriesShadowTrue(t *testing.T) {
	deps, store := seedToParsingShadow(t, "rShadow", map[string]string{"svc-a": "sha-a"})

	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:   "rShadow",
		Status:      "failed",
		ErrorClass:  "UnresolvedReference",
		ErrorDetail: "ref('missing') unresolved in service_1.table_a",
	})
	require.NoError(t, err)

	entry := findEntry(t, store, streams.ReleaseRejectedV1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, true, payload["shadow"],
		"a shadow release's parse_failed rejection must carry shadow:true")
}

// TestHandleParsedManifest_OK_ValidationRequestedCarriesPerNodeFields asserts
// per-node fields are forwarded correctly in the outbound event. When the
// topology contains a new dbt-seed (no prod snapshot → in the changed-closure),
// B5 routing applies: the release routes to seed_building and
// seed.build.requested:v1 is emitted instead of validation.requested:v1. The
// seed node's per-node fields (unique_id, service_name, node_type, schema_name,
// table_name, image_tag) must appear in the "seeds" array.
//
// Note: the dbt-model node "a" is also new (no prod snapshot), but it is NOT a
// seed, so it does not appear in seed.build.requested. The model node will
// appear in validation.requested once seed.build.completed is handled (B6).
func TestHandleParsedManifest_OK_ValidationRequestedCarriesPerNodeFields(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{
		"svc-a": "sha-a",
	})

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", NodeType: "dbt-model", SchemaName: "schema_a", TableName: "table_a"},
		{UniqueID: "b", ServiceName: "svc-a", NodeType: "dbt-seed", SchemaName: "schema_b", TableName: "table_b", UpstreamUniqueIDs: []string{"a"}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	}))

	// B5: a new dbt-seed routes to seed_building, not validating.
	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusSeedBuilding, r.Status())

	// seed.build.requested:v1 must be emitted; validation.requested must not.
	entries := outboxEntries(store)
	require.Len(t, entries, 3) // CompileRequested + ReleaseRequested + SeedBuildRequested
	require.Equal(t, streams.SeedBuildRequestedV1, entries[2].StreamName)

	var payload struct {
		ReleaseID string `json:"release_id"`
		Mode      string `json:"mode"`
		Seeds     []struct {
			UniqueID    string `json:"unique_id"`
			ServiceName string `json:"service_name"`
			NodeType    string `json:"node_type"`
			SchemaName  string `json:"schema_name"`
			TableName   string `json:"table_name"`
			ImageTag    string `json:"image_tag"`
		} `json:"seeds"`
	}
	require.NoError(t, json.Unmarshal(entries[2].Payload, &payload))

	assert.Equal(t, "rA", payload.ReleaseID)
	assert.Equal(t, "seed_build", payload.Mode)
	require.Len(t, payload.Seeds, 1, "only the seed node appears (model node is not a seed)")

	b := payload.Seeds[0]
	assert.Equal(t, "b", b.UniqueID)
	assert.Equal(t, "svc-a", b.ServiceName)
	assert.Equal(t, "dbt-seed", b.NodeType, "node_type is forwarded from the candidate topology")
	assert.Equal(t, "schema_b", b.SchemaName)
	assert.Equal(t, "table_b", b.TableName)
	assert.Equal(t, "sha-a", b.ImageTag, "b is in svc-a so it gets svc-a's image tag")
}

// TestHandleParsedManifest_OK_DerivesChangedSetFromContentHashDiff seeds a
// current_prod snapshot with known hashes, then submits a candidate where one
// existing node's hash changed and one node is new. The validation closure must
// contain the changed node, the new node, and the downstream of the changed
// node — but not an unchanged node that is neither.
func TestHandleParsedManifest_OK_DerivesChangedSetFromContentHashDiff(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})

	// Prod: a (hash h_a), b (hash h_b, downstream of a), c (hash h_c, isolated).
	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h_a"},
		{UniqueID: "b", ServiceName: "svc-a", ContentHash: "h_b", UpstreamUniqueIDs: []string{"a"}},
		{UniqueID: "c", ServiceName: "svc-a", ContentHash: "h_c"},
	}, time.Unix(50, 0).UTC()))

	// Candidate: a unchanged, b changed (h_b2), c unchanged, d new (downstream of c).
	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h_a"},
		{UniqueID: "b", ServiceName: "svc-a", ContentHash: "h_b2", UpstreamUniqueIDs: []string{"a"}},
		{UniqueID: "c", ServiceName: "svc-a", ContentHash: "h_c"},
		{UniqueID: "d", ServiceName: "svc-a", ContentHash: "h_d", UpstreamUniqueIDs: []string{"c"}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	}))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	validIDs := r.ValidationNodeIDs()

	assert.Contains(t, validIDs, "b", "b's hash changed -> seed")
	assert.Contains(t, validIDs, "d", "d is new -> seed")
	assert.Contains(t, validIDs, "a", "a is an intra-service ancestor of changed b -> pulled into build set so its ref()s resolve in the candidate schema")
	assert.Contains(t, validIDs, "c", "c is an intra-service ancestor of new node d -> pulled into build set so d's ref()s resolve in the candidate schema")

	// defer_state_uri is no longer emitted in validation.requested:v1.
	var rawPayload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(findEntry(t, store, streams.ValidationRequestedV1).Payload, &rawPayload))
	_, hasDeferURI := rawPayload["defer_state_uri"]
	assert.False(t, hasDeferURI, "defer_state_uri must not appear in validation.requested:v1")
}

// findEntry returns the single outbox entry on the given stream, failing if
// there is not exactly one.
func findEntry(t *testing.T, store *fakeStore, stream string) *pkgoutbox.Entry {
	t.Helper()
	var match *pkgoutbox.Entry
	for _, e := range outboxEntries(store) {
		if e.StreamName == stream {
			require.Nil(t, match, "expected exactly one %s entry", stream)
			match = e
		}
	}
	require.NotNil(t, match, "no %s outbox entry written", stream)
	return match
}

// TestHandleParsedManifest_OK_ChangedNodePullsDownstream confirms a changed
// node drags its downstream descendants into the validation closure even when
// the downstream node's own hash is unchanged.
func TestHandleParsedManifest_OK_ChangedNodePullsDownstream(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})

	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h_a"},
		{UniqueID: "b", ServiceName: "svc-a", ContentHash: "h_b", UpstreamUniqueIDs: []string{"a"}},
	}, time.Unix(50, 0).UTC()))

	// a changed, b unchanged but downstream of a.
	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h_a2"},
		{UniqueID: "b", ServiceName: "svc-a", ContentHash: "h_b", UpstreamUniqueIDs: []string{"a"}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	}))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	validIDs := r.ValidationNodeIDs()
	assert.Contains(t, validIDs, "a")
	assert.Contains(t, validIDs, "b", "downstream of changed a is pulled in")
}

// TestHandleParsedManifest_OK_BootstrapValidatesAllNodes confirms that with no
// current_prod row, every candidate node is treated as new -> validate-all.
func TestHandleParsedManifest_OK_BootstrapValidatesAllNodes(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h_a"},
		{UniqueID: "b", ServiceName: "svc-a", ContentHash: "h_b", UpstreamUniqueIDs: []string{"a"}},
		{UniqueID: "c", ServiceName: "svc-a", ContentHash: "h_c"},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	}))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	validIDs := r.ValidationNodeIDs()
	assert.ElementsMatch(t, []string{"a", "b", "c"}, validIDs, "bootstrap validates every node")
}

// A candidate whose nodes all match prod (same hashes, no new nodes) has nothing
// to validate. The handler must promote directly — NOT emit an empty
// validation.requested, which executor-controller rejects as a permanent parse
// error, leaving no validation.completed and blocking the queue forever.
func TestHandleParsedManifest_OK_NothingToValidate_PromotesDirectly(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})
	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h_a"},
		{UniqueID: "b", ServiceName: "svc-a", ContentHash: "h_b", UpstreamUniqueIDs: []string{"a"}},
	}, time.Unix(50, 0).UTC()))

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h_a"},
		{UniqueID: "b", ServiceName: "svc-a", ContentHash: "h_b", UpstreamUniqueIDs: []string{"a"}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:     "rA",
		Status:        "ok",
		Topology:      topo,
		CodeBundleURI: "s3://continuo/code-bundles/rA/bundle.json",
	}))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusPromoted, r.Status(), "nothing to validate -> promote directly")

	entries := outboxEntries(store)
	for _, e := range entries {
		assert.NotEqual(t, streams.ValidationRequestedV1, e.StreamName, "must not emit an empty validation request")
	}
	last := entries[len(entries)-1]
	assert.Equal(t, streams.ReleasePromotedV1, last.StreamName, "promotes directly")

	var payload map[string]any
	require.NoError(t, json.Unmarshal(last.Payload, &payload))
	assert.Equal(t, "s3://continuo/code-bundles/rA/bundle.json", payload["code_bundle_uri"],
		"empty-diff promote-direct path must carry code_bundle_uri")
	assert.Equal(t, false, payload["bootstrap"], "non-bootstrap release must carry bootstrap=false")

	cp := store.GetCurrentProd()
	assert.Equal(t, "rA", cp.ReleaseID(), "current prod advanced to this release")
}

// A shadow release exists only to find out whether a proposed fix survives the
// real pipeline, and its terminal "validated" status is what a human is shown
// as proof before being asked to merge that fix. What that proof requires
// depends on WHY the validation set is empty.
//
// A candidate whose nodes all match production has nothing to validate
// because it IS production's code — the validated, promoted baseline — so the
// fix is proven by identity with it. The shadow takes the same trivial pass a
// normal release does and ends in "validated" without touching prod. This is
// the shape of a fix that restores a compile-broken model to its promoted
// content: the shadow already proved the fix by compiling it, and there is
// nothing left to measure.
//
// A candidate that declares no node at all measured nothing and can prove
// nothing, so it is rejected instead — the outcome the shadow-verify
// reconciler already treats as a failed attempt.
//
// The last subtest is the control: the same empty validation set on a
// non-shadow release must still promote directly, since emitting an empty
// validation request would block the release queue forever.
func TestHandleParsedManifest_OK_NothingToValidate_Shadow(t *testing.T) {
	unchangedTopo := func() release.Topology {
		return release.Topology{
			{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h_a"},
			{UniqueID: "b", ServiceName: "svc-a", ContentHash: "h_b", UpstreamUniqueIDs: []string{"a"}},
		}
	}

	t.Run("shadow whose candidate matches production ends validated", func(t *testing.T) {
		deps, store := seedToParsingShadow(t, "rShadow", map[string]string{"svc-a": "sha-a"})
		store.SeedCurrentProd(release.RehydrateCurrentProd("prev", unchangedTopo(), time.Unix(50, 0).UTC()))

		require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
			ReleaseID: "rShadow",
			Status:    "ok",
			Topology:  unchangedTopo(),
		}))

		r, err := store.GetRelease("rShadow")
		require.NoError(t, err)
		assert.Equal(t, pipeline.StatusPassed, r.Status(),
			"a candidate identical to production is production's validated code, so the fix it carries is proven")
		assert.Empty(t, r.FailReason())
		assert.Len(t, r.CandidateTopology(), 2,
			"the parsed topology is persisted, so the release reports its real node count")
		assert.Empty(t, r.ValidationNodeIDs(), "nothing was sent to the validation leg")

		for _, e := range outboxEntries(store) {
			assert.NotEqual(t, streams.ValidationRequestedV1, e.StreamName, "must not emit an empty validation request")
			assert.NotEqual(t, streams.ReleasePromotedV1, e.StreamName, "a shadow release never promotes")
			assert.NotEqual(t, streams.ReleaseRejectedV1, e.StreamName, "a proven fix must not be reported as a failed attempt")
		}

		assert.Equal(t, "prev", store.GetCurrentProd().ReleaseID(),
			"a validated shadow release must not advance current prod")
	})

	t.Run("shadow with an empty candidate topology is rejected instead of reported verified", func(t *testing.T) {
		deps, store := seedToParsingShadow(t, "rShadow", map[string]string{"svc-a": "sha-a"})
		store.SeedCurrentProd(release.RehydrateCurrentProd("prev", unchangedTopo(), time.Unix(50, 0).UTC()))

		require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
			ReleaseID: "rShadow",
			Status:    "ok",
			Topology:  release.Topology{},
		}))

		r, err := store.GetRelease("rShadow")
		require.NoError(t, err)
		assert.Equal(t, pipeline.StatusFailed, r.Status(),
			"a shadow release that declares no node measured nothing and must not end in a status that means the fix is verified")
		assert.Equal(t, "nothing_to_validate", r.FailReason())
		assert.NotEmpty(t, r.FailDetail(), "the rejection must explain itself to an operator")

		for _, e := range outboxEntries(store) {
			assert.NotEqual(t, streams.ValidationRequestedV1, e.StreamName, "must not emit an empty validation request")
			assert.NotEqual(t, streams.ReleasePromotedV1, e.StreamName, "a shadow release never promotes")
		}

		var payload map[string]any
		require.NoError(t, json.Unmarshal(findEntry(t, store, streams.ReleaseRejectedV1).Payload, &payload))
		assert.Equal(t, "rShadow", payload["release_id"])
		assert.Equal(t, "nothing_to_validate", payload["reason"])
		assert.NotEmpty(t, payload["error_detail"])
		assert.Equal(t, true, payload["shadow"],
			"the rejection must carry shadow:true so remediation does not classify a failed fix attempt")

		assert.Equal(t, "prev", store.GetCurrentProd().ReleaseID(),
			"a rejected shadow release must not advance current prod")
	})

	t.Run("non-shadow release with the same empty validation set still promotes", func(t *testing.T) {
		deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})
		store.SeedCurrentProd(release.RehydrateCurrentProd("prev", unchangedTopo(), time.Unix(50, 0).UTC()))

		require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
			ReleaseID: "rA",
			Status:    "ok",
			Topology:  unchangedTopo(),
		}))

		r, err := store.GetRelease("rA")
		require.NoError(t, err)
		assert.Equal(t, pipeline.StatusPromoted, r.Status(),
			"the empty-set trivial pass must be unchanged for a normal release")

		entries := outboxEntries(store)
		assert.Equal(t, streams.ReleasePromotedV1, entries[len(entries)-1].StreamName, "promotes directly")
		assert.Equal(t, "rA", store.GetCurrentProd().ReleaseID(), "current prod advanced to this release")
	})
}

// TestHandleParseOK_RejectsUnbuildableCrossServiceUpstream verifies that a
// candidate where a changed node references an upstream that is absent from the
// candidate topology entirely is rejected early with reason
// "unbuildable_cross_service_upstream" and no validation.requested:v1 event is
// emitted. A cross-service upstream that IS present in the candidate topology
// is buildable and must NOT trigger rejection.
func TestHandleParseOK_RejectsUnbuildableCrossServiceUpstream(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})

	// Candidate: a2 (svc-a) is a new node with an upstream "ghost_upstream" that
	// does not appear anywhere in the candidate topology — a dangling reference
	// that cannot be built into the candidate schema.
	topo := release.Topology{
		{UniqueID: "a2", ServiceName: "svc-a", ContentHash: "h_a2", UpstreamUniqueIDs: []string{"ghost_upstream"}},
	}
	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	})
	require.NoError(t, err, "handler must return nil (graceful rejection, not an infrastructure error)")

	r, rErr := store.GetRelease("rA")
	require.NoError(t, rErr)
	assert.Equal(t, pipeline.StatusRejected, r.Status(), "release must be Rejected")
	assert.Equal(t, "unbuildable_cross_service_upstream", r.FailReason())

	entries := outboxEntries(store)
	var rejectedEntry *pkgoutbox.Entry
	for _, e := range entries {
		assert.NotEqual(t, streams.ValidationRequestedV1, e.StreamName, "must NOT emit validation.requested:v1")
		if e.StreamName == streams.ReleaseRejectedV1 {
			rejectedEntry = e
		}
	}
	require.NotNil(t, rejectedEntry, "release.rejected:v1 outbox entry must be created")

	var payload map[string]any
	require.NoError(t, json.Unmarshal(rejectedEntry.Payload, &payload))
	assert.Equal(t, "unbuildable_cross_service_upstream", payload["reason"])
	assert.Equal(t, "rA", payload["release_id"])
	assert.Equal(t, false, payload["shadow"],
		"a non-shadow release's unbuildable_cross_service_upstream rejection must carry shadow:false")
}

// TestHandleParseOK_RejectsUnbuildableUpstreamOnDownstreamNode proves the guard
// covers the WHOLE validation build set, not just the changed seeds. A
// non-changed downstream node dragged into the closure, whose own upstream is
// absent from the candidate topology, must reject the release early rather than
// fail its dbt --empty job at runtime on a missing relation.
func TestHandleParseOK_RejectsUnbuildableUpstreamOnDownstreamNode(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})

	// Seed prod with c2 (unchanged hash) so only c1 is in the changed set. c2 is a
	// downstream of c1 (so it is pulled into the validation closure), and c2
	// references "ghost_upstream", which is absent from the candidate topology.
	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", release.Topology{
		{UniqueID: "c2", ServiceName: "svc-a", ContentHash: "h_c2"},
	}, time.Unix(50, 0).UTC()))

	topo := release.Topology{
		{UniqueID: "c1", ServiceName: "svc-a", ContentHash: "h_c1_new"},
		{UniqueID: "c2", ServiceName: "svc-a", ContentHash: "h_c2", UpstreamUniqueIDs: []string{"c1", "ghost_upstream"}},
	}
	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	})
	require.NoError(t, err)

	r, rErr := store.GetRelease("rA")
	require.NoError(t, rErr)
	assert.Equal(t, pipeline.StatusRejected, r.Status(),
		"a downstream node with an unbuildable upstream must reject the whole release")
	assert.Equal(t, "unbuildable_cross_service_upstream", r.FailReason())
}

// TestHandleParseOK_CrossServiceUpstreamInCandidatePromotes verifies that a
// changed node with a cross-service upstream that IS present in the candidate
// topology is NOT rejected. Under self-contained validation the upstream is
// built into the candidate schema, so the reference is resolvable.
func TestHandleParseOK_CrossServiceUpstreamInCandidatePromotes(t *testing.T) {
	// svc-a is the changed service. svc-b has a pre-existing service_prod pointer
	// so AdvanceQueue assembles both image tags. Seed service_prod BEFORE AdvanceQueue.
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"
	store.SeedServiceProd(release.NewServiceProd("svc-b", "prev", "s3://continuo/svc-b/prev/manifest.json", "sha-b", release.ManifestKindDbt, time.Unix(0, 0)))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "sha-a", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	// Simulate the compile leg completing (Compiling → Parsing).
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: "rA", Status: "ok",
	}))

	// a2 (svc-a) is new and depends on b_up (svc-b). b_up is present in the
	// candidate topology — it is buildable and must not cause a rejection.
	topo := release.Topology{
		{UniqueID: "b_up", ServiceName: "svc-b", ContentHash: "h_b"},
		{UniqueID: "a2", ServiceName: "svc-a", ContentHash: "h_a2", UpstreamUniqueIDs: []string{"b_up"}},
	}
	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	})
	require.NoError(t, err)

	r, rErr := store.GetRelease("rA")
	require.NoError(t, rErr)
	assert.Equal(t, pipeline.StatusValidating, r.Status(), "cross-service upstream present in candidate must transition to Validating")

	// The full ancestor closure must include b_up (cross-service upstream of the changed a2).
	validIDs := r.ValidationNodeIDs()
	assert.Contains(t, validIDs, "a2", "changed node is in validation set")
	assert.Contains(t, validIDs, "b_up", "cross-service upstream pulled into validation closure")

	// validation.requested:v1 must be emitted (not rejected), with b_up listed as
	// upstream_node_ids for a2.
	entry := findEntry(t, store, streams.ValidationRequestedV1)
	var rawPayload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entry.Payload, &rawPayload))
	var nodes []struct {
		UniqueID        string   `json:"unique_id"`
		UpstreamNodeIDs []string `json:"upstream_node_ids"`
	}
	require.NoError(t, json.Unmarshal(rawPayload["nodes"], &nodes))
	byID := map[string][]string{}
	for _, n := range nodes {
		byID[n.UniqueID] = n.UpstreamNodeIDs
	}
	assert.Equal(t, []string{"b_up"}, byID["a2"], "a2's upstream_node_ids carries cross-service b_up")
	assert.Empty(t, byID["b_up"], "b_up has no in-set upstreams")
}

// TestHandleParseOK_EmitsCandidateArtifactURIPerNode verifies each node's
// candidate_artifact_uri (the S3 URI of the object validation must fetch to
// build the node in the candidate schema, produced by topology-controller) is
// carried into the validation.requested:v1 payload under the key
// "candidate_artifact_uri", where the executor fetches it to build the empty
// candidate table.
func TestHandleParseOK_EmitsCandidateArtifactURIPerNode(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})

	topo := release.Topology{
		{UniqueID: "a1", ServiceName: "svc-a", ContentHash: "h_a1",
			CandidateArtifactURI: "s3://continuo/svc-a/rA/candidate_a1.sql"},
		{UniqueID: "a2", ServiceName: "svc-a", ContentHash: "h_a2", UpstreamUniqueIDs: []string{"a1"},
			CandidateArtifactURI: "s3://continuo/svc-a/rA/candidate_a2.sql"},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	}))

	entry := findEntry(t, store, streams.ValidationRequestedV1)
	var rawPayload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entry.Payload, &rawPayload))
	var nodes []struct {
		UniqueID             string `json:"unique_id"`
		CandidateArtifactURI string `json:"candidate_artifact_uri"`
		ValidationOp         string `json:"validation_op"`
		ProdSchema           string `json:"prod_schema"`
	}
	require.NoError(t, json.Unmarshal(rawPayload["nodes"], &nodes))
	got := map[string]string{}
	op := map[string]string{}
	prodSchema := map[string]string{}
	for _, n := range nodes {
		got[n.UniqueID] = n.CandidateArtifactURI
		op[n.UniqueID] = n.ValidationOp
		prodSchema[n.UniqueID] = n.ProdSchema
	}
	assert.Equal(t, "s3://continuo/svc-a/rA/candidate_a1.sql", got["a1"])
	assert.Equal(t, "s3://continuo/svc-a/rA/candidate_a2.sql", got["a2"])

	// No prod snapshot is seeded, so both nodes are new -> in the changed-closure
	// -> build_from_sql with empty prod_schema.
	assert.Equal(t, "build_from_sql", op["a1"])
	assert.Equal(t, "", prodSchema["a1"])
	assert.Equal(t, "build_from_sql", op["a2"])
	assert.Equal(t, "", prodSchema["a2"])
}

// TestHandleParsedManifest_OK_TestCountSurvivesToCandidateTopology verifies
// that test_count on the manifest.loaded.candidate:v1 payload survives
// unmarshalling into HandleParsedManifestInput (the same JSON decode the
// real Redis binding performs) and persists on the release's candidate
// topology.
func TestHandleParsedManifest_OK_TestCountSurvivesToCandidateTopology(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})

	rawPayload := []byte(`{
		"release_id": "rA",
		"status": "ok",
		"topology": [
			{"unique_id": "a", "service_name": "svc-a", "test_count": 3},
			{"unique_id": "b", "service_name": "svc-a", "upstream_unique_ids": ["a"], "test_count": 0}
		]
	}`)
	var in handlers.HandleParsedManifestInput
	require.NoError(t, json.Unmarshal(rawPayload, &in))

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, in))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)

	byID := map[string]release.Node{}
	for _, n := range r.CandidateTopology() {
		byID[n.UniqueID] = n
	}
	assert.Equal(t, 3, byID["a"].TestCount)
	assert.Equal(t, 0, byID["b"].TestCount)
}

// TestHandleParseOK_EmitsUpstreamNodeIDs_NoDeferURI verifies that the
// validation.requested:v1 payload carries upstream_node_ids per node and does
// NOT contain defer_state_uri. Single-service chain a1->a2, both changed.
func TestHandleParseOK_EmitsUpstreamNodeIDs_NoDeferURI(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})

	// a1 and a2 are both new (bootstrap, no prod snapshot), a2 depends on a1.
	topo := release.Topology{
		{UniqueID: "a1", ServiceName: "svc-a", ContentHash: "h_a1", UpstreamUniqueIDs: []string{}},
		{UniqueID: "a2", ServiceName: "svc-a", ContentHash: "h_a2", UpstreamUniqueIDs: []string{"a1"}},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	}))

	entry := findEntry(t, store, streams.ValidationRequestedV1)

	var rawPayload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entry.Payload, &rawPayload))

	_, hasDeferURI := rawPayload["defer_state_uri"]
	assert.False(t, hasDeferURI, "payload must NOT contain defer_state_uri")

	_, hasNodes := rawPayload["nodes"]
	assert.True(t, hasNodes, "payload must contain nodes array")

	var nodes []struct {
		UniqueID        string   `json:"unique_id"`
		UpstreamNodeIDs []string `json:"upstream_node_ids"`
	}
	require.NoError(t, json.Unmarshal(rawPayload["nodes"], &nodes))

	byID := map[string][]string{}
	for _, n := range nodes {
		byID[n.UniqueID] = n.UpstreamNodeIDs
	}

	assert.Empty(t, byID["a1"], "a1 has no in-set upstreams")
	assert.Equal(t, []string{"a1"}, byID["a2"], "a2 must carry a1 as its in-set upstream")
}

// TestHandleParseOK_UnknownRelease_DropsWithoutPanic guards against a stale or
// duplicate manifest.loaded.candidate:v1 message whose release row no longer exists (e.g.
// it was pruned, or the message was reclaimed from a previous consumer for a
// deleted release). ReleaseRepo.Get returns (nil, nil) for a missing release;
// the handler must ack and drop rather than dereference a nil aggregate and
// crash the consumer on reclaim.
func TestHandleParseOK_UnknownRelease_DropsWithoutPanic(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())

	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "does-not-exist",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "a", ServiceName: "svc-a"},
		},
	})
	require.NoError(t, err, "unknown release must be dropped, not error")

	// Nothing was written: no validation.requested, no release.rejected, no promotion.
	require.Empty(t, outboxEntries(store))
	assert.Equal(t, "", store.GetCurrentProd().ReleaseID())
}

func TestHandleParsedManifest_ImageTagJoinedIntoTopology(t *testing.T) {
	// Two-service topology with all upstreams present in the candidate topology
	// (no dangling references). This tests that image tags from the Release are
	// joined into the stored candidate topology correctly.
	// svc-a is the changed service; svc-b already has a service_prod pointer so
	// its tag is assembled into the release's image_tags at AdvanceQueue time.
	// Seed service_prod BEFORE AdvanceQueue so the assembly picks it up.
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"
	store.SeedServiceProd(release.NewServiceProd("svc-b", "prev", "s3://continuo/svc-b/prev/manifest.json", "tag-beta", release.ManifestKindDbt, time.Unix(0, 0)))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "tag-alpha", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	// Simulate the compile leg completing (Compiling → Parsing).
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: "rA", Status: "ok",
	}))

	// Seed prod with "a" (unchanged hash) so "a" is not in the changed set; "b"
	// is the only changed node and its upstream "a" is present in the candidate
	// topology, so no unbuildable-upstream rejection fires.
	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", ContentHash: "h_a"},
	}, time.Unix(50, 0).UTC()))

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}, ContentHash: "h_a"},
		{UniqueID: "b", ServiceName: "svc-b", UpstreamUniqueIDs: []string{"a"}, ContentHash: "h_b_new"},
	}
	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	})
	require.NoError(t, err)

	r, err := store.GetRelease("rA")
	require.NoError(t, err)

	stored := r.CandidateTopology()
	require.Len(t, stored, 2)

	nodeByID := map[string]release.Node{}
	for _, n := range stored {
		nodeByID[n.UniqueID] = n
	}
	assert.Equal(t, "tag-alpha", nodeByID["a"].ImageTag)
	assert.Equal(t, "tag-beta", nodeByID["b"].ImageTag)
}

func TestHandleParsedManifest_Bootstrap_PromotesWithoutValidation(t *testing.T) {
	deps, store := seedToParsingBootstrap(t, "rBoot", map[string]string{"svc-a": "sha-a", "svc-b": "sha-b"})

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-b", UpstreamUniqueIDs: []string{"a"}}, // cross-service upstream — bootstrap bypasses all guards
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:     "rBoot",
		Status:        "ok",
		Topology:      topo,
		CodeBundleURI: "s3://continuo/code-bundles/rBoot/bundle.json",
	}))

	r, err := store.GetRelease("rBoot")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusPromoted, r.Status())
	// The candidate topology is recorded — promoteToProduction reads it to seed
	// current_prod and the release.promoted:v1 payload; an empty one would
	// silently produce an empty prod snapshot.
	assert.Len(t, r.CandidateTopology(), 2)

	// current_prod seeded to this release.
	assert.Equal(t, "rBoot", store.GetCurrentProd().ReleaseID())

	// Exactly CompileRequested + ReleaseRequested + ReleasePromoted; NO validation_requested, NO rejection.
	entries := outboxEntries(store)
	require.Len(t, entries, 3)
	assert.Equal(t, streams.ReleasePromotedV1, entries[2].StreamName)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(entries[2].Payload, &payload))
	assert.Equal(t, "s3://continuo/code-bundles/rBoot/bundle.json", payload["code_bundle_uri"],
		"bootstrap promote path must carry code_bundle_uri")
	assert.Equal(t, true, payload["bootstrap"], "bootstrap release must carry bootstrap=true")

	// Assert the changed service's service_prod row carries the correct values.
	// Changed service is the lexically-first service ("svc-a", tag "sha-a").
	sp := store.GetServiceProd("svc-a")
	require.NotNil(t, sp)
	assert.Equal(t, "rBoot", sp.ReleaseID())
	assert.Equal(t, "s3://continuo/svc-a/rBoot/manifest.json", sp.ManifestS3Key())
	assert.Equal(t, "sha-a", sp.ImageTag())
}

// TestHandleParsedManifest_AssignsPerNodeValidationOp asserts the six
// dbt/python-model/python-csv x changed/unchanged quadrants:
//   - a changed dbt-model gets build_from_sql (prod_schema empty);
//   - a changed python-model gets build_from_columns (prod_schema empty) —
//     it has no compiled SQL to rewrite, only a JSON validation spec of
//     declared reads + output columns;
//   - a changed python-csv gets build_from_columns too — it is part of the
//     python family (IsPython), so it is classified the same as
//     python-model here;
//   - an unchanged upstream, dbt, python-model, or python-csv, is never
//     built from a candidate artifact: it gets clone_from_prod with
//     prod_schema = its schema_name.
func TestHandleParsedManifest_AssignsPerNodeValidationOp(t *testing.T) {
	deps, store := seedToParsing(t, "rel-op-1", map[string]string{"shop": "sha-shop"})

	// prod has upstreams "model.core.dim", "python.core.stats", and
	// "csv.core.rates" (all unchanged); candidate adds changed
	// "model.shop.orders", "python.shop.enrich", and "csv.shop.fx", each
	// depending on its respective upstream.
	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", release.Topology{
		{UniqueID: "model.core.dim", ServiceName: "shop", NodeType: "dbt-model", SchemaName: "analytics", TableName: "dim", ContentHash: "hash-dim-OLD"},
		{UniqueID: "python.core.stats", ServiceName: "shop", NodeType: "python-model", SchemaName: "analytics", TableName: "stats", ContentHash: "hash-stats-OLD"},
		{UniqueID: "csv.core.rates", ServiceName: "shop", NodeType: "python-csv", SchemaName: "analytics", TableName: "rates", ContentHash: "hash-rates-OLD"},
	}, time.Unix(50, 0).UTC()))

	topo := release.Topology{
		{UniqueID: "model.core.dim", ServiceName: "shop", NodeType: "dbt-model", SchemaName: "analytics", TableName: "dim", ContentHash: "hash-dim-OLD"},
		{UniqueID: "model.shop.orders", ServiceName: "shop", NodeType: "dbt-model", SchemaName: "shop", TableName: "orders", ContentHash: "hash-orders-NEW",
			UpstreamUniqueIDs: []string{"model.core.dim"}},
		{UniqueID: "python.core.stats", ServiceName: "shop", NodeType: "python-model", SchemaName: "analytics", TableName: "stats", ContentHash: "hash-stats-OLD"},
		{UniqueID: "python.shop.enrich", ServiceName: "shop", NodeType: "python-model", SchemaName: "shop", TableName: "enrich", ContentHash: "hash-enrich-NEW",
			UpstreamUniqueIDs: []string{"python.core.stats"}},
		{UniqueID: "csv.core.rates", ServiceName: "shop", NodeType: "python-csv", SchemaName: "analytics", TableName: "rates", ContentHash: "hash-rates-OLD"},
		{UniqueID: "csv.shop.fx", ServiceName: "shop", NodeType: "python-csv", SchemaName: "shop", TableName: "fx", ContentHash: "hash-fx-NEW",
			UpstreamUniqueIDs: []string{"csv.core.rates"}},
	}

	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rel-op-1", Status: "ok", Topology: topo,
	})
	require.NoError(t, err)

	nodes := decodeValidationRequestedNodes(t, store)
	byID := indexNodesByUniqueID(t, nodes)

	// dbt changed.
	assert.Equal(t, "build_from_sql", byID["model.shop.orders"]["validation_op"])
	assert.Equal(t, "", byID["model.shop.orders"]["prod_schema"])

	// dbt unchanged.
	assert.Equal(t, "clone_from_prod", byID["model.core.dim"]["validation_op"])
	assert.Equal(t, "analytics", byID["model.core.dim"]["prod_schema"])

	// python-model changed.
	assert.Equal(t, "build_from_columns", byID["python.shop.enrich"]["validation_op"])
	assert.Equal(t, "", byID["python.shop.enrich"]["prod_schema"])

	// python-model unchanged.
	assert.Equal(t, "clone_from_prod", byID["python.core.stats"]["validation_op"])
	assert.Equal(t, "analytics", byID["python.core.stats"]["prod_schema"])

	// python-csv changed.
	assert.Equal(t, "build_from_columns", byID["csv.shop.fx"]["validation_op"])
	assert.Equal(t, "", byID["csv.shop.fx"]["prod_schema"])

	// python-csv unchanged.
	assert.Equal(t, "clone_from_prod", byID["csv.core.rates"]["validation_op"])
	assert.Equal(t, "analytics", byID["csv.core.rates"]["prod_schema"])
}

// decodeValidationRequestedNodes finds the validation.requested:v1 outbox entry
// and unmarshals its "nodes" array as []map[string]any.
func decodeValidationRequestedNodes(t *testing.T, store *fakeStore) []map[string]any {
	t.Helper()
	entry := findEntry(t, store, streams.ValidationRequestedV1)
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entry.Payload, &raw))
	var nodes []map[string]any
	require.NoError(t, json.Unmarshal(raw["nodes"], &nodes))
	return nodes
}

// indexNodesByUniqueID indexes a []map[string]any by the "unique_id" key. A node
// whose unique_id is missing or not a string is a malformed payload and fails
// the test loudly rather than being silently dropped.
func indexNodesByUniqueID(t *testing.T, nodes []map[string]any) map[string]map[string]any {
	t.Helper()
	out := make(map[string]map[string]any, len(nodes))
	for i, n := range nodes {
		id, ok := n["unique_id"].(string)
		if !ok {
			t.Fatalf("node[%d] has missing or non-string unique_id: %#v", i, n["unique_id"])
		}
		out[id] = n
	}
	return out
}

// TestHandleParsedManifest_NewSeedRoutesToSeedBuild verifies that when the
// changed-closure contains at least one dbt-seed node, handleParseOK transitions
// the release to SeedBuilding and emits seed.build.requested:v1 (seeds only),
// NOT validation.requested:v1.
func TestHandleParsedManifest_NewSeedRoutesToSeedBuild(t *testing.T) {
	deps, store := seedToParsing(t, "rel-seed-1", map[string]string{"core": "sha-core"})
	// No prod snapshot → the seed is new/changed.

	topo := release.Topology{
		{UniqueID: "seed.core.fx", ServiceName: "core", NodeType: "dbt-seed", SchemaName: "analytics", TableName: "fx", ContentHash: "hash-fx-NEW"},
		{UniqueID: "model.fin.report", ServiceName: "core", NodeType: "dbt-model", SchemaName: "fin", TableName: "report", ContentHash: "hash-rep-NEW",
			UpstreamUniqueIDs: []string{"seed.core.fx"}},
	}

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rel-seed-1", Status: "ok", Topology: topo,
	}))

	// Release must be in seed_building state.
	r, err := store.GetRelease("rel-seed-1")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusSeedBuilding, r.Status())

	// seed.build.requested:v1 must be emitted with only the seed node.
	entry := findEntry(t, store, streams.SeedBuildRequestedV1)
	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))

	var seeds []map[string]any
	require.NoError(t, json.Unmarshal(payload["seeds"], &seeds))
	require.Len(t, seeds, 1, "only the seed node appears in the seeds array")
	assert.Equal(t, "seed.core.fx", seeds[0]["unique_id"])

	// validation.requested must NOT be emitted at this stage.
	for _, e := range outboxEntries(store) {
		assert.NotEqual(t, streams.ValidationRequestedV1, e.StreamName,
			"validation.requested must not be emitted when seeds need building first")
	}
}

// TestHandleParsedManifest_SeedBuildRequestedCarriesSourceOverlay verifies that
// a shadow release's seed-build request carries its source overlay. A proposed
// fix to a seed edits the team's CSV, so the seed Job must load the PROPOSED
// file; without the overlay on this leg the Job reloads the checked-in CSV and
// the fix fails its own verification.
func TestHandleParsedManifest_SeedBuildRequestedCarriesSourceOverlay(t *testing.T) {
	const overlay = "s3://continuo/core/shadow-rel-seed-1/source-overlay.tar.gz"
	deps, store := seedToParsingShadowWithOverlay(t, "shadow-rel-seed-1",
		map[string]string{"core": "sha-core"}, overlay)

	topo := release.Topology{
		{UniqueID: "seed.core.fx", ServiceName: "core", NodeType: "dbt-seed", SchemaName: "analytics", TableName: "fx", ContentHash: "hash-fx-NEW"},
		{UniqueID: "model.fin.report", ServiceName: "core", NodeType: "dbt-model", SchemaName: "fin", TableName: "report", ContentHash: "hash-rep-NEW",
			UpstreamUniqueIDs: []string{"seed.core.fx"}},
	}

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "shadow-rel-seed-1", Status: "ok", Topology: topo,
	}))

	entry := findEntry(t, store, streams.SeedBuildRequestedV1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, overlay, payload["source_overlay_uri"],
		"the seed-build leg must carry the shadow's overlay, like the compile leg does")
}

// TestHandleParsedManifest_SeedBuildRequestedOmitsAbsentSourceOverlay verifies
// that a production release (no overlay) emits the seed-build payload without
// the key at all, leaving the pre-overlay wire format byte-identical.
func TestHandleParsedManifest_SeedBuildRequestedOmitsAbsentSourceOverlay(t *testing.T) {
	deps, store := seedToParsing(t, "rel-seed-nooverlay", map[string]string{"core": "sha-core"})

	topo := release.Topology{
		{UniqueID: "seed.core.fx", ServiceName: "core", NodeType: "dbt-seed", SchemaName: "analytics", TableName: "fx", ContentHash: "hash-fx-NEW"},
	}

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rel-seed-nooverlay", Status: "ok", Topology: topo,
	}))

	entry := findEntry(t, store, streams.SeedBuildRequestedV1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	_, present := payload["source_overlay_uri"]
	assert.False(t, present, "a release without an overlay must not emit the key")
}

// TestHandleParsedManifest_NoNewSeedsGoesStraightToValidation verifies that
// when the changed-closure contains no dbt-seed nodes, the existing Part-A path
// is taken: release transitions to Validating and validation.requested:v1 is
// emitted directly.
func TestHandleParsedManifest_NoNewSeedsGoesStraightToValidation(t *testing.T) {
	deps, store := seedToParsing(t, "rel-noseed-1", map[string]string{"svc-a": "sha-a"})

	topo := release.Topology{
		{UniqueID: "model.a", ServiceName: "svc-a", NodeType: "dbt-model", SchemaName: "sc", TableName: "a", ContentHash: "h-new"},
	}
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rel-noseed-1", Status: "ok", Topology: topo,
	}))

	// validation.requested:v1 must be emitted (Part-A path, unchanged).
	var found bool
	for _, e := range outboxEntries(store) {
		if e.StreamName == streams.ValidationRequestedV1 {
			found = true
			break
		}
	}
	assert.True(t, found, "no new/changed seeds → validation.requested must be emitted directly")
}

// A candidate topology where two services claim analytics.orders is rejected
// before promotion, with both claimants named and no validation requested.
func TestHandleParsedManifest_DuplicateTableRejects(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"marketing": "sha-m"})

	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID:     "rA",
		Status:        "ok",
		CodeBundleURI: "s3://continuo/code-bundles/rA/bundle.json",
		Topology: release.Topology{
			{UniqueID: "analytics.orders", SchemaName: "analytics", TableName: "orders",
				ServiceName: "finance", OriginalFilePath: "models/orders.sql", ContentHash: "h1"},
			{UniqueID: "analytics.orders", SchemaName: "analytics", TableName: "orders",
				ServiceName: "marketing", OriginalFilePath: "models/orders.sql", ContentHash: "h2"},
		},
	})
	require.NoError(t, err)

	r, rErr := store.GetRelease("rA")
	require.NoError(t, rErr)
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Equal(t, "duplicate_table", r.FailReason())
	assert.Contains(t, r.FailDetail(), "finance (models/orders.sql)")
	assert.Contains(t, r.FailDetail(), "marketing (models/orders.sql)")
	assert.Equal(t, []string{"analytics.orders"}, r.FailingNodes())

	for _, e := range outboxEntries(store) {
		assert.NotEqual(t, streams.ValidationRequestedV1, e.StreamName,
			"a rejected release must not request validation")
	}

	entry := findEntry(t, store, streams.ReleaseRejectedV1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, "duplicate_table", payload["reason"])
	assert.Equal(t, "DuplicatedTable", payload["error_class"])
	assert.Equal(t, "s3://continuo/code-bundles/rA/bundle.json", payload["code_bundle_uri"],
		"top-level code_bundle_uri must come from the release aggregate, set at parse time")
	assert.Equal(t, false, payload["shadow"],
		"a non-shadow release's duplicate_table rejection must carry shadow:false")
	assert.JSONEq(t, string(entry.Payload), string(r.RejectionPayload()),
		"the rejection payload stored on the release must match the one emitted on release.rejected:v1")
}

// TestHandleParsedManifest_DuplicateTable_Shadow_CarriesShadowTrue verifies
// that a shadow release's duplicate_table rejection carries shadow:true on
// release.rejected:v1, so remediation can tell this failure came from a
// shadow fix-verification attempt and must not re-trigger remediation on it.
func TestHandleParsedManifest_DuplicateTable_Shadow_CarriesShadowTrue(t *testing.T) {
	deps, store := seedToParsingShadow(t, "rShadow", map[string]string{"marketing": "sha-m"})

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rShadow",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "analytics.orders", SchemaName: "analytics", TableName: "orders",
				ServiceName: "finance", OriginalFilePath: "models/orders.sql", ContentHash: "h1"},
			{UniqueID: "analytics.orders", SchemaName: "analytics", TableName: "orders",
				ServiceName: "marketing", OriginalFilePath: "models/orders.sql", ContentHash: "h2"},
		},
	}))

	entry := findEntry(t, store, streams.ReleaseRejectedV1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, true, payload["shadow"],
		"a shadow release's duplicate_table rejection must carry shadow:true")
}

// The rejection payload carries the claimant a fix should target and the
// competing service, so remediation can build evidence without a second lookup.
func TestHandleParsedManifest_DuplicateTablePayloadNamesTargetAndOther(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"marketing": "sha-m"})

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "analytics.orders", ServiceName: "finance", OriginalFilePath: "models/orders.sql", NodeType: "dbt-model"},
			{UniqueID: "analytics.orders", ServiceName: "marketing", OriginalFilePath: "models/orders.sql", NodeType: "python-model"},
		},
	}))

	entry := findEntry(t, store, streams.ReleaseRejectedV1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	perNode, ok := payload["per_node"].([]any)
	require.True(t, ok, "per_node must be present so remediation can build evidence")
	require.Len(t, perNode, 1)
	node := perNode[0].(map[string]any)
	assert.Equal(t, "analytics.orders", node["node_id"])
	assert.Equal(t, "failed", node["status"])
	assert.Equal(t, "marketing", node["service"], "the changed service is the rename target")
	assert.Equal(t, "models/orders.sql", node["file_path"])
	assert.Equal(t, "python-model", node["node_type"], "the target claimant's kind, so remediation can skip an unfixable python target")
	assert.Equal(t, "finance", node["other_service"])
	assert.Equal(t, "models/orders.sql", node["other_file_path"])
}

// DuplicateClaims supports two nodes in the SAME service colliding. In that
// case service and other_service are both "marketing" — degenerate and not by
// itself enough to tell the claimants apart — so other_file_path must carry
// the one datum (the competing source file) that lets a downstream consumer
// identify the competitor without parsing error_detail.
func TestHandleParsedManifest_DuplicateTablePayloadSameServiceCarriesBothFilePaths(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"marketing": "sha-m"})

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "analytics.orders", ServiceName: "marketing", OriginalFilePath: "models/orders.sql"},
			{UniqueID: "analytics.orders", ServiceName: "marketing", OriginalFilePath: "models/orders_v2.sql"},
		},
	}))

	entry := findEntry(t, store, streams.ReleaseRejectedV1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	perNode, ok := payload["per_node"].([]any)
	require.True(t, ok, "per_node must be present so remediation can build evidence")
	require.Len(t, perNode, 1)
	node := perNode[0].(map[string]any)
	assert.Equal(t, "marketing", node["service"])
	assert.Equal(t, "marketing", node["other_service"], "both claimants are in the same service")
	assert.Equal(t, "models/orders.sql", node["file_path"])
	assert.Equal(t, "models/orders_v2.sql", node["other_file_path"])
	assert.NotEqual(t, node["file_path"], node["other_file_path"],
		"the two claimants must remain distinguishable by file path even when their service names collide")
}

// Bootstrap skips validation and promotes directly, so the gate must run above
// that branch or a colliding bootstrap topology reaches current_prod.
func TestHandleParsedManifest_DuplicateTableRejectsBootstrap(t *testing.T) {
	deps, store := seedToParsingBootstrap(t, "rBoot", map[string]string{"finance": "sha-f"})

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rBoot",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "analytics.orders", ServiceName: "finance", OriginalFilePath: "models/orders.sql"},
			{UniqueID: "analytics.orders", ServiceName: "marketing", OriginalFilePath: "models/orders.sql"},
		},
	}))

	r, rErr := store.GetRelease("rBoot")
	require.NoError(t, rErr)
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Equal(t, "duplicate_table", r.FailReason())
	assert.Equal(t, "", store.GetCurrentProd().ReleaseID(), "nothing may be promoted")
}

// A model copied verbatim into a second service produces a duplicate
// unique_id whose content_hash matches current_prod for BOTH claimants, so
// DerivedChangedNodeIDs finds nothing changed and validationIDs is empty —
// the route that promotes directly via the len(validationIDs)==0
// short-circuit further down in handleParseOK. This pins the gate above that
// specific route, independent of the bootstrap branch: unlike
// TestHandleParsedManifest_DuplicateTableRejectsBootstrap, this release is
// NOT a bootstrap, so it only reaches this route, never the bootstrap one.
func TestHandleParsedManifest_DuplicateTablePinsNothingToValidateShortCircuit(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"marketing": "sha-m"})
	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", release.Topology{
		{UniqueID: "analytics.orders", ServiceName: "finance", OriginalFilePath: "models/orders.sql", ContentHash: "h1"},
	}, time.Unix(50, 0).UTC()))

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "analytics.orders", ServiceName: "finance", OriginalFilePath: "models/orders.sql", ContentHash: "h1"},
			{UniqueID: "analytics.orders", ServiceName: "marketing", OriginalFilePath: "models/orders_copy.sql", ContentHash: "h1"},
		},
	}))

	r, rErr := store.GetRelease("rA")
	require.NoError(t, rErr)
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Equal(t, "duplicate_table", r.FailReason())
	assert.Equal(t, "prev", store.GetCurrentProd().ReleaseID(), "current_prod must not move")
}

// A three-way collision cannot be fixed by one rename proposal: renaming the
// target still leaves the relation claimed by the other two. It still
// rejects, naming and failing every claimant, but contributes no per_node
// entry — remediation would otherwise build evidence and open a trigger for a
// proposal that could never clear the gate on its own.
func TestHandleParsedManifest_DuplicateTableThreeWayEmitsNoPerNodeEntry(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"marketing": "sha-m"})

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "analytics.orders", ServiceName: "finance", OriginalFilePath: "models/orders.sql"},
			{UniqueID: "analytics.orders", ServiceName: "marketing", OriginalFilePath: "models/orders.sql"},
			{UniqueID: "analytics.orders", ServiceName: "sales", OriginalFilePath: "models/orders.sql"},
		},
	}))

	r, rErr := store.GetRelease("rA")
	require.NoError(t, rErr)
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Equal(t, "duplicate_table", r.FailReason())
	assert.Contains(t, r.FailDetail(), "finance (models/orders.sql)")
	assert.Contains(t, r.FailDetail(), "marketing (models/orders.sql)")
	assert.Contains(t, r.FailDetail(), "sales (models/orders.sql)")
	assert.Equal(t, []string{"analytics.orders"}, r.FailingNodes(),
		"all three claimants share this unique_id here, so it appears once, deduplicated")

	entry := findEntry(t, store, streams.ReleaseRejectedV1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	perNode, ok := payload["per_node"].([]any)
	require.True(t, ok, "per_node must be present, even though empty")
	assert.Empty(t, perNode, "a three-way collision must not propose a rename")
}

// A release mixing a two-way and a three-way collision proposes a rename only
// for the fixable one; the three-way claim still rejects the release and is
// fully named in the detail, it just contributes no per_node entry.
func TestHandleParsedManifest_DuplicateTableMixedTwoAndThreeWayEmitsOnlyTheTwoWayPerNodeEntry(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"marketing": "sha-m"})

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology: release.Topology{
			// Two-way collision: analytics.customers.
			{UniqueID: "analytics.customers", ServiceName: "finance", OriginalFilePath: "models/customers.sql"},
			{UniqueID: "analytics.customers", ServiceName: "marketing", OriginalFilePath: "models/customers.sql"},
			// Three-way collision: analytics.orders.
			{UniqueID: "analytics.orders", ServiceName: "finance", OriginalFilePath: "models/orders.sql"},
			{UniqueID: "analytics.orders", ServiceName: "marketing", OriginalFilePath: "models/orders.sql"},
			{UniqueID: "analytics.orders", ServiceName: "sales", OriginalFilePath: "models/orders.sql"},
		},
	}))

	entry := findEntry(t, store, streams.ReleaseRejectedV1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	perNode, ok := payload["per_node"].([]any)
	require.True(t, ok)
	require.Len(t, perNode, 1, "only the two-way claim proposes a rename")
	node := perNode[0].(map[string]any)
	assert.Equal(t, "analytics.customers", node["node_id"])
}

// Two nodes with the same unique_id but different resolved relations (an
// alias override on one of them) is an identity collision, not a relation
// one: unique_id is the identity key for every downstream lookup keyed on
// it, so this must reject even though the two nodes write different
// warehouse tables. It emits no per_node entry — no rename a fixer can
// express changes a unique_id, so a proposal here could never clear the
// gate — but it still names both claimants and both unique_ids in the
// detail and failing_nodes so an operator can resolve it by hand.
func TestHandleParsedManifest_DuplicateTableIdentityCollisionRejectsWithNoPerNodeEntry(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"marketing": "sha-m"})

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "analytics.orders", ResolvedRelationID: "analytics.orders_finance",
				ServiceName: "finance", OriginalFilePath: "models/orders.sql"},
			{UniqueID: "analytics.orders", ResolvedRelationID: "analytics.orders_marketing",
				ServiceName: "marketing", OriginalFilePath: "models/orders_v2.sql"},
		},
	}))

	r, rErr := store.GetRelease("rA")
	require.NoError(t, rErr)
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Equal(t, "duplicate_table", r.FailReason())
	assert.Contains(t, r.FailDetail(), "unique_id analytics.orders is declared by")
	assert.Contains(t, r.FailDetail(), "finance (models/orders.sql)")
	assert.Contains(t, r.FailDetail(), "marketing (models/orders_v2.sql)")
	assert.Equal(t, []string{"analytics.orders"}, r.FailingNodes(),
		"both claimants share this unique_id, so it appears once, deduplicated")

	entry := findEntry(t, store, streams.ReleaseRejectedV1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	perNode, ok := payload["per_node"].([]any)
	require.True(t, ok, "per_node must be present, even though empty")
	assert.Empty(t, perNode, "an identity collision must not propose a rename")
}

// The relation_id field on a per_node entry carries the contested relation
// itself, distinct from node_id (the target claimant's own unique_id) —
// remediation needs the relation for its classification signature and the
// remediation agent needs it for the rename prompt, and in the alias case
// the two differ.
func TestHandleParsedManifest_DuplicateTablePayloadCarriesRelationID(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"marketing": "sha-m"})

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "analytics.orders_v1", ResolvedRelationID: "analytics.orders",
				ServiceName: "finance", OriginalFilePath: "models/orders_v1.sql"},
			{UniqueID: "analytics.orders_v2", ResolvedRelationID: "analytics.orders",
				ServiceName: "marketing", OriginalFilePath: "models/orders_v2.sql"},
		},
	}))

	entry := findEntry(t, store, streams.ReleaseRejectedV1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	perNode, ok := payload["per_node"].([]any)
	require.True(t, ok)
	require.Len(t, perNode, 1)
	node := perNode[0].(map[string]any)
	assert.Equal(t, "analytics.orders_v2", node["node_id"], "the target's own unique_id")
	assert.Equal(t, "analytics.orders", node["relation_id"], "the contested relation, distinct from node_id")
}

// A release can trip a relation collision and an identity collision on
// different pairs of nodes at once. Both are rejected and both are named in
// error_detail; only the relation collision proposes a rename.
func TestHandleParsedManifest_DuplicateTableRelationAndIdentityBothNamedOnlyRelationGetsPerNode(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"marketing": "sha-m"})

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology: release.Topology{
			// Relation collision: different unique_ids, same resolved relation.
			{UniqueID: "analytics.orders_v1", ResolvedRelationID: "analytics.orders",
				ServiceName: "finance", OriginalFilePath: "models/orders_v1.sql"},
			{UniqueID: "analytics.orders_v2", ResolvedRelationID: "analytics.orders",
				ServiceName: "marketing", OriginalFilePath: "models/orders_v2.sql"},
			// Identity collision: same unique_id, different resolved relations.
			{UniqueID: "analytics.customers", ResolvedRelationID: "analytics.customers_finance",
				ServiceName: "finance", OriginalFilePath: "models/customers.sql"},
			{UniqueID: "analytics.customers", ResolvedRelationID: "analytics.customers_sales",
				ServiceName: "sales", OriginalFilePath: "models/customers.sql"},
		},
	}))

	r, rErr := store.GetRelease("rA")
	require.NoError(t, rErr)
	assert.Equal(t, pipeline.StatusRejected, r.Status())
	assert.Contains(t, r.FailDetail(), "analytics.orders is produced by",
		"relation collision named as a relation")
	assert.Contains(t, r.FailDetail(), "unique_id analytics.customers is declared by",
		"identity collision named as an identity")

	entry := findEntry(t, store, streams.ReleaseRejectedV1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	perNode, ok := payload["per_node"].([]any)
	require.True(t, ok)
	require.Len(t, perNode, 1, "only the relation collision proposes a rename")
	node := perNode[0].(map[string]any)
	assert.Equal(t, "analytics.orders", node["relation_id"])
}

// A clean topology is unaffected: the gate must not reject a release whose
// relations each have a single producer. Asserted against the specific
// StatusValidating outcome (not merely "not Rejected") and against a real
// validation.requested:v1 entry, so a gate that over-rejects anything —
// including deleting the gate check itself, which would leave the status
// unconstrained rather than pinned to Validating — cannot pass this test
// unnoticed.
func TestHandleParsedManifest_DistinctRelationsPassTheGate(t *testing.T) {
	deps, store := seedToParsing(t, "rOK", map[string]string{"marketing": "sha-m"})

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rOK",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "analytics.orders", SchemaName: "analytics", TableName: "orders",
				ServiceName: "finance", OriginalFilePath: "models/orders.sql", ContentHash: "h1"},
			{UniqueID: "marketing.orders", SchemaName: "marketing", TableName: "orders",
				ServiceName: "marketing", OriginalFilePath: "models/orders.sql", ContentHash: "h2"},
		},
	}))

	r, rErr := store.GetRelease("rOK")
	require.NoError(t, rErr)
	assert.Equal(t, pipeline.StatusValidating, r.Status())

	entry := findEntry(t, store, streams.ValidationRequestedV1)
	assert.NotNil(t, entry, "a clean topology must still request validation")
}

// --- shadow changed-set derivation ---
//
// A remediation that spans two services repairs the whole failing set in one
// attempt but submits one shadow release per edited service (a release is one
// service's delta). Each shadow assembles the OTHER edited service's node
// UNCHANGED — still carrying its not-yet-fixed failure, byte-identical to the
// rejected candidate. A shadow that names the rejected release it verifies must
// therefore re-validate a node only if the fix changed it relative to BOTH
// current_prod AND that rejected candidate — the intersection of the two diffs.
// The sibling's still-broken node differs from current_prod but matches the
// rejected candidate, so it is excluded; an unrelated node another release
// promoted since the rejection matches current_prod, so it is excluded too;
// only the fix's own edit, which departs from both, is validated.

const (
	shadowEID = "svc2.ftable_e" // service-2's broken/fixed node
	shadowGID = "svc3.ftable_g" // service-3's broken node (the sibling failure)
	shadowUID = "shared.u"      // an unchanged upstream present in production
)

// seedReleaseInParsing installs a run already advanced to Parsing so a
// HandleParsedManifest call exercises handleParseOK directly. Rehydrate pins
// exactly the fields the changed-set baseline reads (Kind, VerifiesReleaseID,
// ChangedService) without driving the receive/advance/compile pipeline.
func seedReleaseInParsing(store *fakeStore, id, service string, shadow bool, verifiesID string) {
	kind := pipeline.KindCandidate
	if shadow {
		kind = pipeline.KindVerification
	}
	store.SeedRelease(pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:                id,
		Kind:              kind,
		Status:            pipeline.StatusParsing,
		ImageTags:         map[string]string{service: "img-" + service},
		ChangedService:    service,
		VerifiesReleaseID: verifiesID,
		ManifestKind:      release.ManifestKindDbt,
		RemediationRound:  1,
		CreatedAt:         time.Unix(90, 0).UTC(),
		Transitions:       []pipeline.Transition{{To: pipeline.StatusParsing, At: time.Unix(90, 0).UTC()}},
	}))
}

// seedRejectedOriginal installs the rejected release a shadow verifies, carrying
// the candidate topology the sibling's unfixed failure matches (and so is
// excluded by) in the intersection.
func seedRejectedOriginal(store *fakeStore, id, service string, candidate release.Topology) {
	store.SeedRelease(pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:                id,
		Status:            pipeline.StatusRejected,
		ImageTags:         map[string]string{service: "img-" + service},
		ChangedService:    service,
		CandidateTopology: candidate,
		ManifestKind:      release.ManifestKindDbt,
		RemediationRound:  1,
		CreatedAt:         time.Unix(80, 0).UTC(),
		Transitions:       []pipeline.Transition{{To: pipeline.StatusRejected, At: time.Unix(85, 0).UTC()}},
	}))
}

// shadowProdTopo is production BEFORE the rejected release: it advanced past
// neither broken node, holding only the unchanged upstream.
func shadowProdTopo() release.Topology {
	return release.Topology{{UniqueID: shadowUID, ServiceName: "shared", ContentHash: "h_u"}}
}

// shadowRejectedCandidate is the rejected release's candidate: its own service-2
// delta (broken ftable_e) plus service-3's ftable_g assembled unchanged from that
// service's production pointer (also broken).
func shadowRejectedCandidate() release.Topology {
	return release.Topology{
		{UniqueID: shadowUID, ServiceName: "shared", ContentHash: "h_u"},
		{UniqueID: shadowEID, ServiceName: "service-2", NodeType: "dbt-model", ContentHash: "e_broken"},
		{UniqueID: shadowGID, ServiceName: "service-3", NodeType: "dbt-model", ContentHash: "g_broken"},
	}
}

// shadowParsedTopo is the service-2 shadow's assembled+parsed topology: the FIXED
// ftable_e (new hash) plus service-3's ftable_g still assembled UNCHANGED
// (identical to the rejected candidate's hash).
func shadowParsedTopo() release.Topology {
	return release.Topology{
		{UniqueID: shadowUID, ServiceName: "shared", ContentHash: "h_u"},
		{UniqueID: shadowEID, ServiceName: "service-2", NodeType: "dbt-model", ContentHash: "e_fixed"},
		{UniqueID: shadowGID, ServiceName: "service-3", NodeType: "dbt-model", ContentHash: "g_broken"},
	}
}

// TestHandleParsedManifest_OK_ShadowBaselinesOnVerifiedCandidate: a shadow
// verifying a rejected release re-validates only the nodes its fix changed
// relative to both current_prod and the rejected candidate. The fixed node is
// validated; the sibling's still-broken node, byte-identical to the rejected
// candidate, is not.
func TestHandleParsedManifest_OK_ShadowBaselinesOnVerifiedCandidate(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", shadowProdTopo(), time.Unix(50, 0).UTC()))
	seedRejectedOriginal(store, "orig", "service-2", shadowRejectedCandidate())
	seedReleaseInParsing(store, "shadow2", "service-2", true, "orig")

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "shadow2",
		Status:    "ok",
		Topology:  shadowParsedTopo(),
	}))

	r, err := store.GetRelease("shadow2")
	require.NoError(t, err)
	validIDs := r.ValidationNodeIDs()
	assert.Contains(t, validIDs, shadowEID,
		"the fix's own edit departs from the rejected candidate -> validated")
	assert.NotContains(t, validIDs, shadowGID,
		"the sibling's still-broken node matches the rejected candidate -> NOT re-validated")
	assert.NotContains(t, validIDs, shadowUID,
		"an unchanged upstream is neither changed nor pulled into the build set")
}

// TestHandleParsedManifest_OK_NonShadowDiffsAgainstProdOnly pins that the
// two-way intersection is confined to the shadow-with-verifies path: a
// production release with the identical topology still diffs against
// current_prod alone, so both nodes absent from production read as changed.
func TestHandleParsedManifest_OK_NonShadowDiffsAgainstProdOnly(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", shadowProdTopo(), time.Unix(50, 0).UTC()))
	seedReleaseInParsing(store, "prodrel", "service-2", false, "")

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "prodrel",
		Status:    "ok",
		Topology:  shadowParsedTopo(),
	}))

	r, err := store.GetRelease("prodrel")
	require.NoError(t, err)
	validIDs := r.ValidationNodeIDs()
	assert.Contains(t, validIDs, shadowEID, "a production release diffs against prod only")
	assert.Contains(t, validIDs, shadowGID,
		"absent from production, the sibling node is changed for a non-shadow release")
}

// TestHandleParsedManifest_OK_ShadowWithoutVerifiesDiffsAgainstProdOnly pins that
// a shadow that names no verified release keeps today's behavior (diff against
// current_prod), so the two-way intersection is genuinely gated on
// VerifiesReleaseID.
func TestHandleParsedManifest_OK_ShadowWithoutVerifiesDiffsAgainstProdOnly(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", shadowProdTopo(), time.Unix(50, 0).UTC()))
	seedReleaseInParsing(store, "shadownv", "service-2", true, "")

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "shadownv",
		Status:    "ok",
		Topology:  shadowParsedTopo(),
	}))

	r, err := store.GetRelease("shadownv")
	require.NoError(t, err)
	validIDs := r.ValidationNodeIDs()
	assert.Contains(t, validIDs, shadowEID)
	assert.Contains(t, validIDs, shadowGID,
		"a shadow naming no verified release still diffs against prod, so the sibling is changed")
}

// TestHandleParsedManifest_OK_ShadowFallsBackWhenVerifiedReleaseUnreadable pins
// the graceful degradation: a shadow whose verified release cannot be read
// diffs against production instead of failing, so the sibling node is validated
// (a weaker but running verification), mirroring assembleFor.
func TestHandleParsedManifest_OK_ShadowFallsBackWhenVerifiedReleaseUnreadable(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", shadowProdTopo(), time.Unix(50, 0).UTC()))
	// "missing" is never seeded, so RunRepo().Get returns (nil, nil).
	seedReleaseInParsing(store, "shadowmiss", "service-2", true, "missing")

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "shadowmiss",
		Status:    "ok",
		Topology:  shadowParsedTopo(),
	}))

	r, err := store.GetRelease("shadowmiss")
	require.NoError(t, err)
	validIDs := r.ValidationNodeIDs()
	assert.Contains(t, validIDs, shadowEID)
	assert.Contains(t, validIDs, shadowGID,
		"an unreadable verified release falls back to a production diff, so the sibling is changed")
}

// TestHandleParsedManifest_OK_ShadowFallsBackWhenVerifiedCandidateEmpty pins the
// second fallback: a verified release that never parsed far enough to hold a
// candidate topology yields nothing to intersect against, so the shadow diffs
// against production alone.
func TestHandleParsedManifest_OK_ShadowFallsBackWhenVerifiedCandidateEmpty(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", shadowProdTopo(), time.Unix(50, 0).UTC()))
	seedRejectedOriginal(store, "origempty", "service-2", nil) // no candidate topology
	seedReleaseInParsing(store, "shadowempty", "service-2", true, "origempty")

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "shadowempty",
		Status:    "ok",
		Topology:  shadowParsedTopo(),
	}))

	r, err := store.GetRelease("shadowempty")
	require.NoError(t, err)
	validIDs := r.ValidationNodeIDs()
	assert.Contains(t, validIDs, shadowEID)
	assert.Contains(t, validIDs, shadowGID,
		"an empty verified candidate falls back to a production diff, so the sibling is changed")
}

// TestHandleParsedManifest_OK_ShadowRestoringProductionAfterCompileRejectionIsValidated
// pins the compile-lane verification path within the parse leg. The verified
// release was rejected at compile, so it never parsed and holds no candidate
// topology; the shadow therefore diffs against production alone. The fix
// restores the broken model to exactly its promoted content, so that diff is
// empty — and because the shadow's topology is production's own code, the fix
// is proven (the shadow already compiled it), so the shadow ends validated
// rather than rejected as unmeasured.
func TestHandleParsedManifest_OK_ShadowRestoringProductionAfterCompileRejectionIsValidated(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	prod := release.Topology{
		{UniqueID: "core.read_order", ServiceName: "core", NodeType: "dbt-model", ContentHash: "h_read_order"},
	}
	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", prod, time.Unix(50, 0).UTC()))
	seedRejectedOriginal(store, "origcompile", "core", nil) // rejected at compile: no candidate topology
	seedReleaseInParsing(store, "shadowfix", "core", true, "origcompile")

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "shadowfix",
		Status:    "ok",
		Topology:  prod, // the fix restored the model to its promoted content
	}))

	r, err := store.GetRelease("shadowfix")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusPassed, r.Status(),
		"a fix that restores a compile-broken model to its promoted content is proven, not unmeasured")
	assert.Empty(t, r.FailReason())
	assert.Len(t, r.CandidateTopology(), 1, "the parsed topology is persisted on the validated shadow")
	for _, e := range outboxEntries(store) {
		assert.NotEqual(t, streams.ReleaseRejectedV1, e.StreamName, "a proven fix must not be reported as a failed attempt")
		assert.NotEqual(t, streams.ReleasePromotedV1, e.StreamName, "a shadow release never promotes")
	}
	assert.Equal(t, "prev", store.GetCurrentProd().ReleaseID(),
		"a validated shadow release must not advance current prod")
}

// shadowHID is a node owned by neither the shadow's own service nor the
// sibling failure: an unrelated node that another release promoted to
// production AFTER the rejection this shadow verifies.
const shadowHID = "svc4.ftable_h"

// TestHandleParsedManifest_OK_ShadowBaselinePrefersNewerProdOverRejectedCandidate
// covers current_prod advancing past a node between the rejection and this
// shadow's parse (an unrelated service promoting in the meantime). The shadow
// assembles that node at its live production hash, so it matches current_prod
// and is absent from the current_prod diff — the intersection excludes it even
// though it still differs from the rejected candidate's stale copy. Without
// that, a live, already-promoted node would be dragged into a fix-only shadow.
func TestHandleParsedManifest_OK_ShadowBaselinePrefersNewerProdOverRejectedCandidate(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	// current_prod already carries H's NEW hash: some other release promoted it
	// after the rejection below.
	prod := release.Topology{
		{UniqueID: shadowUID, ServiceName: "shared", ContentHash: "h_u"},
		{UniqueID: shadowHID, ServiceName: "service-4", NodeType: "dbt-model", ContentHash: "h_new"},
	}
	// The rejected candidate is a point-in-time snapshot from before that
	// promotion: it still holds H's OLD hash.
	rejectedCandidate := release.Topology{
		{UniqueID: shadowUID, ServiceName: "shared", ContentHash: "h_u"},
		{UniqueID: shadowEID, ServiceName: "service-2", NodeType: "dbt-model", ContentHash: "e_broken"},
		{UniqueID: shadowGID, ServiceName: "service-3", NodeType: "dbt-model", ContentHash: "g_broken"},
		{UniqueID: shadowHID, ServiceName: "service-4", NodeType: "dbt-model", ContentHash: "h_old"},
	}
	// The shadow assembles H from current_prod (its NEW hash), same as any
	// other unrelated, already-live node.
	parsed := release.Topology{
		{UniqueID: shadowUID, ServiceName: "shared", ContentHash: "h_u"},
		{UniqueID: shadowEID, ServiceName: "service-2", NodeType: "dbt-model", ContentHash: "e_fixed"},
		{UniqueID: shadowGID, ServiceName: "service-3", NodeType: "dbt-model", ContentHash: "g_broken"},
		{UniqueID: shadowHID, ServiceName: "service-4", NodeType: "dbt-model", ContentHash: "h_new"},
	}

	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", prod, time.Unix(50, 0).UTC()))
	seedRejectedOriginal(store, "origpromoted", "service-2", rejectedCandidate)
	seedReleaseInParsing(store, "shadowpromoted", "service-2", true, "origpromoted")

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "shadowpromoted",
		Status:    "ok",
		Topology:  parsed,
	}))

	r, err := store.GetRelease("shadowpromoted")
	require.NoError(t, err)
	validIDs := r.ValidationNodeIDs()
	assert.Contains(t, validIDs, shadowEID,
		"the fix's own edit still departs from the rejected candidate -> validated")
	assert.NotContains(t, validIDs, shadowGID,
		"the sibling's still-broken node matches the rejected candidate -> NOT re-validated")
	assert.NotContains(t, validIDs, shadowHID,
		"an unrelated node promoted to production since the rejection matches current_prod, so the intersection excludes it over the stale rejected candidate -> NOT re-validated")
}

// shadowSID is an ESTABLISHED sibling node: the sibling service's failing node
// MODIFIES a node that already exists in production, so current_prod holds it at
// its pre-rejection hash rather than it being absent. This is the case the
// absent-sibling fixture (shadowGID, new in production) does not cover.
const shadowSID = "svc3.ftable_s"

// TestHandleParsedManifest_OK_ShadowExcludesEstablishedSiblingModification is
// the established-sibling case: a two-service fix where the sibling service's
// failing node edits an EXISTING production node. current_prod holds that node
// at its pre-rejection hash (s_old), the rejected candidate holds the sibling's
// still-broken edit (s_broken), and this shadow (for the OTHER service)
// assembles the sibling unchanged from the rejected candidate (s_broken). The
// sibling differs from current_prod, so a current_prod-only or current_prod-wins
// baseline would re-validate it and re-fail the shadow on a node its fix was
// never about — sinking the good fix. Diffing against BOTH current_prod and the
// rejected candidate and keeping only the intersection excludes it: it matches
// the rejected candidate, so it is absent from the second diff. The fix's own
// node, which departs from both, is still validated.
func TestHandleParsedManifest_OK_ShadowExcludesEstablishedSiblingModification(t *testing.T) {
	deps, store := newDeps(time.Unix(100, 0).UTC())
	deps.Bucket = "continuo"

	// current_prod holds the sibling node S at its pre-rejection hash: the
	// sibling's failure MODIFIES this existing node rather than adding it.
	prod := release.Topology{
		{UniqueID: shadowUID, ServiceName: "shared", ContentHash: "h_u"},
		{UniqueID: shadowSID, ServiceName: "service-3", NodeType: "dbt-model", ContentHash: "s_old"},
	}
	// The rejected candidate carries this service's own broken delta (e_broken)
	// plus the sibling's still-broken modification (s_broken).
	rejectedCandidate := release.Topology{
		{UniqueID: shadowUID, ServiceName: "shared", ContentHash: "h_u"},
		{UniqueID: shadowEID, ServiceName: "service-2", NodeType: "dbt-model", ContentHash: "e_broken"},
		{UniqueID: shadowSID, ServiceName: "service-3", NodeType: "dbt-model", ContentHash: "s_broken"},
	}
	// This shadow fixes service-2 (e_fixed) and assembles the sibling UNCHANGED
	// from the rejected candidate (s_broken), because service-3's fix ships on a
	// separate shadow.
	parsed := release.Topology{
		{UniqueID: shadowUID, ServiceName: "shared", ContentHash: "h_u"},
		{UniqueID: shadowEID, ServiceName: "service-2", NodeType: "dbt-model", ContentHash: "e_fixed"},
		{UniqueID: shadowSID, ServiceName: "service-3", NodeType: "dbt-model", ContentHash: "s_broken"},
	}

	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", prod, time.Unix(50, 0).UTC()))
	seedRejectedOriginal(store, "origestab", "service-2", rejectedCandidate)
	seedReleaseInParsing(store, "shadowestab", "service-2", true, "origestab")

	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "shadowestab",
		Status:    "ok",
		Topology:  parsed,
	}))

	r, err := store.GetRelease("shadowestab")
	require.NoError(t, err)
	validIDs := r.ValidationNodeIDs()
	assert.Contains(t, validIDs, shadowEID,
		"the fix's own edit departs from both current_prod and the rejected candidate -> validated")
	assert.NotContains(t, validIDs, shadowSID,
		"the sibling's still-broken modification differs from current_prod (which holds it at its pre-rejection hash) but matches the rejected candidate, so the intersection excludes it -> NOT re-validated")
	assert.NotContains(t, validIDs, shadowUID,
		"an unchanged upstream is neither changed nor pulled into the build set")
}
