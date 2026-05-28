package neo4jinfra_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wipeReleaseFixtures removes all :Table nodes and the :Meta {key:'current_release'}
// singleton so each test starts from a clean slate.
func wipeReleaseFixtures(t *testing.T, client neo4jinfra.Neo4jClient) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `MATCH (n:Table) DETACH DELETE n`, nil)
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
	res2, err := s.Run(ctx, `MATCH (m:Meta {key:'current_release'}) DELETE m`, nil)
	require.NoError(t, err)
	_, err = res2.Consume(ctx)
	require.NoError(t, err)
}

func newReleaseRepo(client neo4jinfra.Neo4jClient) *neo4jinfra.ReleasePromotionRepository {
	return neo4jinfra.NewReleasePromotionRepository(
		client,
		slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
	)
}

// TestReleasePromotionRepository_FirstPromotionCreatesNodesEdgesAndMeta tests
// the happy path: an empty graph receives 3 nodes with edges, and the :Meta
// singleton is created.
func TestReleasePromotionRepository_FirstPromotionCreatesNodesEdgesAndMeta(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t) // skips if Neo4j is unreachable
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	repo := newReleaseRepo(client)
	nodes := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "a", SchemaName: "public", TableName: "orders", ServiceName: "svc", ImageTag: "sha:1", Schedule: "daily", UpstreamUniqueIDs: []string{}},
		{UniqueID: "b", SchemaName: "public", TableName: "customers", ServiceName: "svc", ImageTag: "sha:1", Schedule: "daily", UpstreamUniqueIDs: []string{"a"}},
		{UniqueID: "c", SchemaName: "public", TableName: "items", ServiceName: "svc", ImageTag: "sha:1", Schedule: "daily", UpstreamUniqueIDs: []string{"a"}},
	}
	changed, err := repo.PromoteRelease(ctx, "rel-1", nodes, time.Now().UTC())
	require.NoError(t, err)
	assert.True(t, changed)

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)

	// Assert exactly 3 :Table nodes with the expected unique_ids and release_id.
	res, err := s.Run(ctx, `MATCH (t:Table) RETURN t.unique_id AS uid, t.release_id AS rid ORDER BY uid`, nil)
	require.NoError(t, err)
	var uids []string
	for res.Next(ctx) {
		uid, _ := res.Record().Get("uid")
		rid, _ := res.Record().Get("rid")
		uids = append(uids, uid.(string))
		assert.Equal(t, "rel-1", rid)
	}
	require.NoError(t, res.Err())
	assert.Equal(t, []string{"a", "b", "c"}, uids)

	// Assert 2 :DEPENDS_ON edges: b→a and c→a.
	edgeRes, err := s.Run(ctx,
		`MATCH (a:Table)-[:DEPENDS_ON]->(b:Table) RETURN a.unique_id AS from_uid, b.unique_id AS to_uid ORDER BY from_uid`,
		nil,
	)
	require.NoError(t, err)
	type edge struct{ from, to string }
	var edges []edge
	for edgeRes.Next(ctx) {
		f, _ := edgeRes.Record().Get("from_uid")
		to, _ := edgeRes.Record().Get("to_uid")
		edges = append(edges, edge{f.(string), to.(string)})
	}
	require.NoError(t, edgeRes.Err())
	assert.Equal(t, []edge{{"b", "a"}, {"c", "a"}}, edges)

	// Assert :Meta singleton has the correct release_id.
	metaRes, err := s.Run(ctx, `MATCH (m:Meta {key:'current_release'}) RETURN m.release_id AS rid`, nil)
	require.NoError(t, err)
	require.True(t, metaRes.Next(ctx))
	rid, _ := metaRes.Record().Get("rid")
	assert.Equal(t, "rel-1", rid)
}

// TestReleasePromotionRepository_RedeliveryWithSameReleaseIDIsNoOp tests that
// calling PromoteRelease twice with the same release_id returns (false, nil) on
// the second call and leaves the topology unchanged.
func TestReleasePromotionRepository_RedeliveryWithSameReleaseIDIsNoOp(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	repo := newReleaseRepo(client)
	nodes := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "a", SchemaName: "p", TableName: "t", ServiceName: "s", ImageTag: "x", Schedule: "d", UpstreamUniqueIDs: []string{}},
	}
	_, err := repo.PromoteRelease(ctx, "rel-1", nodes, time.Now().UTC())
	require.NoError(t, err)

	// Second call with same release_id but an empty topology — must be a no-op.
	changed, err := repo.PromoteRelease(ctx, "rel-1", []topology.ReleasePromotedTopologyNode{}, time.Now().UTC())
	require.NoError(t, err)
	assert.False(t, changed)

	// Topology unchanged: still 1 node.
	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `MATCH (t:Table) RETURN count(t) AS n`, nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	n, _ := res.Record().Get("n")
	assert.Equal(t, int64(1), n)
}

// TestReleasePromotionRepository_NewReleaseTruncatesOldTopology tests that a
// second PromoteRelease call with a different release_id deletes the old :Table
// nodes and :DEPENDS_ON edges, replacing them with the new topology.
func TestReleasePromotionRepository_NewReleaseTruncatesOldTopology(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	repo := newReleaseRepo(client)
	first := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "a", SchemaName: "p", TableName: "ta", ServiceName: "s", ImageTag: "x", Schedule: "d"},
		{UniqueID: "b", SchemaName: "p", TableName: "tb", ServiceName: "s", ImageTag: "x", Schedule: "d"},
	}
	_, err := repo.PromoteRelease(ctx, "rel-1", first, time.Now().UTC())
	require.NoError(t, err)

	second := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "c", SchemaName: "p", TableName: "tc", ServiceName: "s", ImageTag: "y", Schedule: "d"},
	}
	changed, err := repo.PromoteRelease(ctx, "rel-2", second, time.Now().UTC())
	require.NoError(t, err)
	assert.True(t, changed)

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)

	// Old nodes (a, b) must be gone; only c remains.
	res, err := s.Run(ctx, `MATCH (t:Table) RETURN t.unique_id AS uid ORDER BY uid`, nil)
	require.NoError(t, err)
	var uids []string
	for res.Next(ctx) {
		uid, _ := res.Record().Get("uid")
		uids = append(uids, uid.(string))
	}
	require.NoError(t, res.Err())
	assert.Equal(t, []string{"c"}, uids)

	// :Meta must reflect the new release_id.
	metaRes, err := s.Run(ctx, `MATCH (m:Meta {key:'current_release'}) RETURN m.release_id AS rid`, nil)
	require.NoError(t, err)
	require.True(t, metaRes.Next(ctx))
	rid, _ := metaRes.Record().Get("rid")
	assert.Equal(t, "rel-2", rid)
}

// TestReleasePromotionRepository_EmptyTopologyStillUpdatesMeta tests that
// PromoteRelease with an empty nodes slice returns (true, nil), writes no :Table
// nodes, and still creates/updates the :Meta singleton.
func TestReleasePromotionRepository_EmptyTopologyStillUpdatesMeta(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	repo := newReleaseRepo(client)
	changed, err := repo.PromoteRelease(ctx, "rel-empty", nil, time.Now().UTC())
	require.NoError(t, err)
	assert.True(t, changed)

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)

	// Zero :Table nodes.
	res, err := s.Run(ctx, `MATCH (t:Table) RETURN count(t) AS n`, nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	n, _ := res.Record().Get("n")
	assert.Equal(t, int64(0), n)

	// :Meta singleton holds the new release_id.
	metaRes, err := s.Run(ctx, `MATCH (m:Meta {key:'current_release'}) RETURN m.release_id AS rid`, nil)
	require.NoError(t, err)
	require.True(t, metaRes.Next(ctx))
	rid, _ := metaRes.Record().Get("rid")
	assert.Equal(t, "rel-empty", rid)
}

// TestReleasePromotionRepository_UnknownUpstreamSkipsEdgeButCreatesNode tests
// the spec §6.4 behaviour: a node referencing an upstream unique_id that is not
// in the topology slice produces no :DEPENDS_ON edge and no error. The node
// itself is created normally.
func TestReleasePromotionRepository_UnknownUpstreamSkipsEdgeButCreatesNode(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	repo := newReleaseRepo(client)
	nodes := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "a", SchemaName: "p", TableName: "t", ServiceName: "s", ImageTag: "x", Schedule: "d", UpstreamUniqueIDs: []string{"dangling"}},
	}
	changed, err := repo.PromoteRelease(ctx, "rel-1", nodes, time.Now().UTC())
	require.NoError(t, err)
	assert.True(t, changed)

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)

	// The node itself must exist.
	nodeRes, err := s.Run(ctx, `MATCH (t:Table) RETURN t.unique_id AS uid`, nil)
	require.NoError(t, err)
	var uids []string
	for nodeRes.Next(ctx) {
		uid, _ := nodeRes.Record().Get("uid")
		uids = append(uids, uid.(string))
	}
	require.NoError(t, nodeRes.Err())
	assert.Equal(t, []string{"a"}, uids)

	// No :DEPENDS_ON edges because 'dangling' was not created.
	edgeRes, err := s.Run(ctx, `MATCH (a:Table)-[:DEPENDS_ON]->(b:Table) RETURN count(*) AS n`, nil)
	require.NoError(t, err)
	require.True(t, edgeRes.Next(ctx))
	n, _ := edgeRes.Record().Get("n")
	assert.Equal(t, int64(0), n, "dangling upstream must not produce an edge")
}
