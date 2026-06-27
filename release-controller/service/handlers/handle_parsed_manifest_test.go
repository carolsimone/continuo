package handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedToParsing advances a release from Received to Parsing via ReceiveCandidate + AdvanceQueue.
// Returns the deps and fakeStore for further assertions or handler calls.
// seedToParsing advances a release from Received to Parsing via ReceiveCandidate + AdvanceQueue.
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
		store.SeedServiceProd(release.NewServiceProd(s, releaseID+"-prev", "s3://continuo/"+s+"/prev/manifest.json", tg, time.Unix(0, 0)))
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
	assert.Equal(t, release.StatusValidating, r.Status())

	validIDs := r.ValidationNodeIDs()
	assert.Contains(t, validIDs, "a")
	assert.Contains(t, validIDs, "b")

	entries := outboxEntries(store)
	require.Len(t, entries, 2) // ReleaseRequested + ValidationRequested

	second := entries[1]
	assert.Equal(t, streams.ValidationRequestedV1, second.StreamName)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(second.Payload, &payload))
	assert.Equal(t, "rA", payload["release_id"])
	assert.Equal(t, "validation", payload["mode"])
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
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "parse_failed", r.RejectReason())

	entries := outboxEntries(store)
	require.Len(t, entries, 2) // ReleaseRequested + ReleaseRejected

	second := entries[1]
	assert.Equal(t, streams.ReleaseRejectedV1, second.StreamName)
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
	assert.Equal(t, release.StatusSeedBuilding, r.Status())

	// seed.build.requested:v1 must be emitted; validation.requested must not.
	entries := outboxEntries(store)
	require.Len(t, entries, 2) // ReleaseRequested + SeedBuildRequested
	require.Equal(t, streams.SeedBuildRequestedV1, entries[1].StreamName)

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
	require.NoError(t, json.Unmarshal(entries[1].Payload, &payload))

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
		ReleaseID: "rA",
		Status:    "ok",
		Topology:  topo,
	}))

	r, err := store.GetRelease("rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusPromoted, r.Status(), "nothing to validate -> promote directly")

	entries := outboxEntries(store)
	for _, e := range entries {
		assert.NotEqual(t, streams.ValidationRequestedV1, e.StreamName, "must not emit an empty validation request")
	}
	last := entries[len(entries)-1]
	assert.Equal(t, streams.ReleasePromotedV1, last.StreamName, "promotes directly")

	cp := store.GetCurrentProd()
	assert.Equal(t, "rA", cp.ReleaseID(), "current prod advanced to this release")
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
	assert.Equal(t, release.StatusRejected, r.Status(), "release must be Rejected")
	assert.Equal(t, "unbuildable_cross_service_upstream", r.RejectReason())

	entries := outboxEntries(store)
	var rejectedEntry *pkgoutbox.Entry
	for _, e := range entries {
		assert.NotEqual(t, streams.ValidationRequestedV1, e.StreamName, "must NOT emit validation.requested:v1")
		if e.StreamName == streams.ReleaseRejectedV1 {
			rejectedEntry = e
		}
	}
	require.NotNil(t, rejectedEntry, "release.rejected:v1 outbox entry must be created")

	var payload map[string]string
	require.NoError(t, json.Unmarshal(rejectedEntry.Payload, &payload))
	assert.Equal(t, "unbuildable_cross_service_upstream", payload["reason"])
	assert.Equal(t, "rA", payload["release_id"])
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
	assert.Equal(t, release.StatusRejected, r.Status(),
		"a downstream node with an unbuildable upstream must reject the whole release")
	assert.Equal(t, "unbuildable_cross_service_upstream", r.RejectReason())
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
	store.SeedServiceProd(release.NewServiceProd("svc-b", "prev", "s3://continuo/svc-b/prev/manifest.json", "sha-b", time.Unix(0, 0)))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "sha-a", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

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
	assert.Equal(t, release.StatusValidating, r.Status(), "cross-service upstream present in candidate must transition to Validating")

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

// TestHandleParseOK_EmitsCandidateSQLURIPerNode verifies each node's
// candidate_sql_uri (the S3 URI of the compiled SQL rewritten to the candidate
// schema by manifest-controller) is carried into the validation.requested:v1
// payload under the key "candidate_sql_uri", where the executor fetches the
// SQL to build the empty candidate table.
func TestHandleParseOK_EmitsCandidateSQLURIPerNode(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a"})

	topo := release.Topology{
		{UniqueID: "a1", ServiceName: "svc-a", ContentHash: "h_a1",
			CandidateSQLURI: "s3://continuo/svc-a/rA/candidate_a1.sql"},
		{UniqueID: "a2", ServiceName: "svc-a", ContentHash: "h_a2", UpstreamUniqueIDs: []string{"a1"},
			CandidateSQLURI: "s3://continuo/svc-a/rA/candidate_a2.sql"},
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
		UniqueID        string `json:"unique_id"`
		CandidateSQLURI string `json:"candidate_sql_uri"`
		ValidationOp    string `json:"validation_op"`
		ProdSchema      string `json:"prod_schema"`
	}
	require.NoError(t, json.Unmarshal(rawPayload["nodes"], &nodes))
	got := map[string]string{}
	op := map[string]string{}
	prodSchema := map[string]string{}
	for _, n := range nodes {
		got[n.UniqueID] = n.CandidateSQLURI
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
	store.SeedServiceProd(release.NewServiceProd("svc-b", "prev", "s3://continuo/svc-b/prev/manifest.json", "tag-beta", time.Unix(0, 0)))
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		Service: "svc-a", ReleaseID: "rA", ImageTag: "tag-alpha", Repo: "acme/demo", CommitSHA: "deadbeef",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))

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
		ReleaseID: "rBoot",
		Status:    "ok",
		Topology:  topo,
	}))

	r, err := store.GetRelease("rBoot")
	require.NoError(t, err)
	assert.Equal(t, release.StatusPromoted, r.Status())
	// The candidate topology is recorded — promoteToProduction reads it to seed
	// current_prod and the release.promoted:v1 payload; an empty one would
	// silently produce an empty prod snapshot.
	assert.Len(t, r.CandidateTopology(), 2)

	// current_prod seeded to this release.
	assert.Equal(t, "rBoot", store.GetCurrentProd().ReleaseID())

	// Exactly ReleaseRequested + ReleasePromoted; NO validation_requested, NO rejection.
	entries := outboxEntries(store)
	require.Len(t, entries, 2)
	assert.Equal(t, streams.ReleasePromotedV1, entries[1].StreamName)

	// Assert the changed service's service_prod row carries the correct values.
	// Changed service is the lexically-first service ("svc-a", tag "sha-a").
	sp := store.GetServiceProd("svc-a")
	require.NotNil(t, sp)
	assert.Equal(t, "rBoot", sp.ReleaseID())
	assert.Equal(t, "s3://continuo/svc-a/rBoot/manifest.json", sp.ManifestS3Key())
	assert.Equal(t, "sha-a", sp.ImageTag())
}

// TestHandleParsedManifest_AssignsPerNodeValidationOp asserts:
// A changed model gets build_from_sql; its unchanged upstream (pulled in by the
// ancestors closure) gets clone_from_prod with prod_schema = its schema_name.
func TestHandleParsedManifest_AssignsPerNodeValidationOp(t *testing.T) {
	deps, store := seedToParsing(t, "rel-op-1", map[string]string{"shop": "sha-shop"})

	// prod has upstream "model.core.dim" (unchanged); candidate adds changed
	// "model.shop.orders" depending on it.
	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", release.Topology{
		{UniqueID: "model.core.dim", ServiceName: "shop", NodeType: "dbt-model", SchemaName: "analytics", TableName: "dim", ContentHash: "hash-dim-OLD"},
	}, time.Unix(50, 0).UTC()))

	topo := release.Topology{
		{UniqueID: "model.core.dim", ServiceName: "shop", NodeType: "dbt-model", SchemaName: "analytics", TableName: "dim", ContentHash: "hash-dim-OLD"},
		{UniqueID: "model.shop.orders", ServiceName: "shop", NodeType: "dbt-model", SchemaName: "shop", TableName: "orders", ContentHash: "hash-orders-NEW",
			UpstreamUniqueIDs: []string{"model.core.dim"}},
	}

	err := handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rel-op-1", Status: "ok", Topology: topo,
	})
	require.NoError(t, err)

	nodes := decodeValidationRequestedNodes(t, store)
	byID := indexNodesByUniqueID(t, nodes)

	assert.Equal(t, "build_from_sql", byID["model.shop.orders"]["validation_op"])
	assert.Equal(t, "", byID["model.shop.orders"]["prod_schema"])

	assert.Equal(t, "clone_from_prod", byID["model.core.dim"]["validation_op"])
	assert.Equal(t, "analytics", byID["model.core.dim"]["prod_schema"])
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
	assert.Equal(t, release.StatusSeedBuilding, r.Status())

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

// decodeSeedBuildSeeds is a helper that decodes the "seeds" field from a
// seed.build.requested:v1 outbox entry's payload.
func decodeSeedBuildSeeds(t *testing.T, entry *pkgoutbox.Entry) []map[string]any {
	t.Helper()
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entry.Payload, &raw))
	var seeds []map[string]any
	require.NoError(t, json.Unmarshal(raw["seeds"], &seeds))
	return seeds
}
