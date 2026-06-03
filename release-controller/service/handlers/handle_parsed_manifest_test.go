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
func seedToParsing(t *testing.T, releaseID string, imageTags map[string]string) (*handlers.Deps, *fakeStore) {
	t.Helper()
	deps, store := newDeps(time.Unix(100, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID:    releaseID,
		ImageTags:    imageTags,
		ManifestsURI: "s3://continuo/releases/" + releaseID + "/manifests/",
	}))
	require.NoError(t, handlers.AdvanceQueue(context.Background(), deps))
	return deps, store
}

// seedToParsingBootstrap is seedToParsing for a bootstrap release.
func seedToParsingBootstrap(t *testing.T, releaseID string, imageTags map[string]string) (*handlers.Deps, *fakeStore) {
	t.Helper()
	deps, store := newDeps(time.Unix(100, 0).UTC())
	require.NoError(t, handlers.ReceiveCandidate(context.Background(), deps, handlers.ReceiveCandidateInput{
		ReleaseID:    releaseID,
		ImageTags:    imageTags,
		ManifestsURI: "s3://continuo/releases/" + releaseID + "/manifests/",
		Bootstrap:    true,
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
// the validation.requested:v1 payload carries a `nodes` array of full per-node
// objects (unique_id, service_name, schema_name, table_name, image_tag) in
// topological order. executor-controller needs these to build candidate dbt
// jobs without re-deriving fields from the unique_id string.
// Single-service topology is used to avoid cross-service upstream rejection on
// bootstrap (no prod snapshot).
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

	entries := outboxEntries(store)
	require.Len(t, entries, 2)
	require.Equal(t, streams.ValidationRequestedV1, entries[1].StreamName)

	var payload struct {
		ReleaseID      string   `json:"release_id"`
		Mode           string   `json:"mode"`
		NodeIDsInOrder []string `json:"node_ids_in_order"`
		Nodes          []struct {
			UniqueID    string `json:"unique_id"`
			ServiceName string `json:"service_name"`
			NodeType    string `json:"node_type"`
			SchemaName  string `json:"schema_name"`
			TableName   string `json:"table_name"`
			ImageTag    string `json:"image_tag"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(entries[1].Payload, &payload))

	assert.Equal(t, "rA", payload.ReleaseID)
	assert.Equal(t, "validation", payload.Mode)
	require.Len(t, payload.Nodes, 2, "nodes array carries one entry per validation node")

	// The order must match node_ids_in_order (topo sort: a before b).
	assert.Equal(t, payload.NodeIDsInOrder[0], payload.Nodes[0].UniqueID)
	assert.Equal(t, payload.NodeIDsInOrder[1], payload.Nodes[1].UniqueID)

	byID := map[string]int{}
	for i, n := range payload.Nodes {
		byID[n.UniqueID] = i
	}
	a := payload.Nodes[byID["a"]]
	b := payload.Nodes[byID["b"]]

	assert.Equal(t, "svc-a", a.ServiceName)
	assert.Equal(t, "dbt-model", a.NodeType, "node_type is forwarded from the candidate topology")
	assert.Equal(t, "schema_a", a.SchemaName)
	assert.Equal(t, "table_a", a.TableName)
	assert.Equal(t, "sha-a", a.ImageTag, "image_tag was joined from Release.ImageTags before publishing")

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
	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", "s3://continuo/releases/prev/manifests/", release.Topology{
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

	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", "s3://continuo/releases/prev/manifests/", release.Topology{
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
	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", "s3://continuo/releases/prev/manifests/", release.Topology{
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
	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", "s3://continuo/releases/prev/manifests/", release.Topology{
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
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a", "svc-b": "sha-b"})

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
	imageTags := map[string]string{
		"svc-a": "tag-alpha",
		"svc-b": "tag-beta",
	}
	deps, store := seedToParsing(t, "rA", imageTags)

	// Seed prod with "a" (unchanged hash) so "a" is not in the changed set; "b"
	// is the only changed node and its upstream "a" is present in the candidate
	// topology, so no unbuildable-upstream rejection fires.
	store.SeedCurrentProd(release.RehydrateCurrentProd("prev", "s3://continuo/releases/prev/manifests/", release.Topology{
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
}
