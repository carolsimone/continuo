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

func TestHandleParsedManifest_OK_TransitionsToValidating(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{"svc-a": "sha-a", "svc-b": "sha-b"})

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-b", UpstreamUniqueIDs: []string{"a"}},
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
func TestHandleParsedManifest_OK_ValidationRequestedCarriesPerNodeFields(t *testing.T) {
	deps, store := seedToParsing(t, "rA", map[string]string{
		"svc-a": "sha-a",
		"svc-b": "sha-b",
	})

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", NodeType: "dbt-model", SchemaName: "schema_a", TableName: "table_a"},
		{UniqueID: "b", ServiceName: "svc-b", NodeType: "dbt-seed", SchemaName: "schema_b", TableName: "table_b", UpstreamUniqueIDs: []string{"a"}},
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

	assert.Equal(t, "svc-b", b.ServiceName)
	assert.Equal(t, "dbt-seed", b.NodeType, "node_type is forwarded from the candidate topology")
	assert.Equal(t, "schema_b", b.SchemaName)
	assert.Equal(t, "table_b", b.TableName)
	assert.Equal(t, "sha-b", b.ImageTag)
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
	assert.NotContains(t, validIDs, "a", "a unchanged and not downstream of a changed node")
	assert.NotContains(t, validIDs, "c", "c unchanged and not downstream of a changed node")
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

func TestHandleParsedManifest_ImageTagJoinedIntoTopology(t *testing.T) {
	imageTags := map[string]string{
		"svc-a": "tag-alpha",
		"svc-b": "tag-beta",
	}
	deps, store := seedToParsing(t, "rA", imageTags)

	topo := release.Topology{
		{UniqueID: "a", ServiceName: "svc-a", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", ServiceName: "svc-b", UpstreamUniqueIDs: []string{"a"}},
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
