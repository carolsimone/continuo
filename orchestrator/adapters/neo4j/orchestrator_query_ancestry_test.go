package neo4jinfra_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// seedAncestryDAG creates: c -> b -> a  and  c -> d -> a (a diamond at "a"),
// plus an orphan node "z" with no provenance reachable c -> z? no: keep z as a
// 1-hop upstream of c with NULL provenance to test nulls-last.
// :DEPENDS_ON points downstream -> upstream.
func seedAncestryDAG(t *testing.T, client neo4jinfra.Neo4jClient) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	_, err := s.Run(ctx, `MATCH (n:Table) DETACH DELETE n`, nil)
	require.NoError(t, err)
	res, err := s.Run(ctx, `
		CREATE (a:Table {unique_id:'a', schema_name:'p', table_name:'a', service_name:'s', node_type:'dbt-model', active:true,
		                 original_file_path:'models/a.sql', last_commit_sha:'a1', last_repo:'acme/demo', last_changed_at: datetime('2026-06-01T00:00:00Z'), last_release_id:'r1'})
		CREATE (b:Table {unique_id:'b', schema_name:'p', table_name:'b', service_name:'s', node_type:'dbt-model', active:true,
		                 original_file_path:'models/b.sql', last_commit_sha:'b9', last_repo:'acme/demo', last_changed_at: datetime('2026-06-20T00:00:00Z'), last_release_id:'r2'})
		CREATE (d:Table {unique_id:'d', schema_name:'p', table_name:'d', service_name:'s', node_type:'dbt-model', active:true,
		                 original_file_path:'models/d.sql', last_commit_sha:'d5', last_repo:'acme/demo', last_changed_at: datetime('2026-06-10T00:00:00Z'), last_release_id:'r3'})
		CREATE (z:Table {unique_id:'z', schema_name:'p', table_name:'z', service_name:'s', node_type:'dbt-seed', active:true,
		                 original_file_path:'seeds/z.csv'})
		CREATE (c:Table {unique_id:'c', schema_name:'p', table_name:'c', service_name:'s', node_type:'dbt-model', active:true,
		                 original_file_path:'models/c.sql', last_commit_sha:'c2', last_repo:'acme/demo', last_changed_at: datetime('2026-06-22T00:00:00Z'), last_release_id:'r4'})
		CREATE (c)-[:DEPENDS_ON]->(b)
		CREATE (c)-[:DEPENDS_ON]->(d)
		CREATE (c)-[:DEPENDS_ON]->(z)
		CREATE (b)-[:DEPENDS_ON]->(a)
		CREATE (d)-[:DEPENDS_ON]->(a)
	`, nil)
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

func newQueryRepo(client neo4jinfra.Neo4jClient) *neo4jinfra.OrchestratorQueryRepository {
	return neo4jinfra.NewOrchestratorQueryRepository(client, testLogger())
}

func TestGetNodeAncestry_DirectionRecencyDedupAndFilePath(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	seedAncestryDAG(t, client)
	t.Cleanup(func() {
		s := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer s.Close(ctx)
		_, _ = s.Run(ctx, `MATCH (n:Table) DETACH DELETE n`, nil)
	})
	repo := newQueryRepo(client)

	// Full ancestry of "c": c(0), b(1), d(1), z(1), a(2 via b or d, shallowest=2).
	got, err := repo.GetNodeAncestry(ctx, "c", 0)
	require.NoError(t, err)

	byID := map[string]*domain.NodeAncestor{}
	for _, n := range got {
		byID[n.UniqueID] = n
	}
	// Direction guard: upstreams present, no descendants (c has none above it).
	require.Contains(t, byID, "a")
	require.Contains(t, byID, "b")
	require.Contains(t, byID, "d")
	require.Contains(t, byID, "z")
	require.Contains(t, byID, "c")
	assert.Equal(t, 0, byID["c"].Depth)
	assert.Equal(t, 1, byID["b"].Depth)
	// Diamond dedup: "a" appears once at shallowest depth 2.
	count := 0
	for _, n := range got {
		if n.UniqueID == "a" {
			count++
		}
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, 2, byID["a"].Depth)
	// file_path surfaced.
	assert.Equal(t, "models/b.sql", byID["b"].FilePath)
	// Recency order: changed_at DESC, nulls last. c(06-22) > b(06-20) > d(06-10) > a(06-01) > z(nil).
	var order []string
	for _, n := range got {
		order = append(order, n.UniqueID)
	}
	assert.Equal(t, []string{"c", "b", "d", "a", "z"}, order)
	assert.Nil(t, byID["z"].LastChangedAt)
	assert.Equal(t, "", byID["z"].LastCommitSHA)
}

func TestGetNodeAncestry_MaxDepth(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	seedAncestryDAG(t, client)
	t.Cleanup(func() {
		s := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer s.Close(ctx)
		_, _ = s.Run(ctx, `MATCH (n:Table) DETACH DELETE n`, nil)
	})
	repo := newQueryRepo(client)

	got, err := repo.GetNodeAncestry(ctx, "c", 1) // node + direct upstreams only
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, n := range got {
		ids[n.UniqueID] = true
	}
	assert.True(t, ids["c"] && ids["b"] && ids["d"] && ids["z"])
	assert.False(t, ids["a"], "a is 2 hops up; excluded at max_depth=1")
}

func TestGetNodeAncestry_NotFoundAndNoAncestors(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	seedAncestryDAG(t, client)
	t.Cleanup(func() {
		s := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer s.Close(ctx)
		_, _ = s.Run(ctx, `MATCH (n:Table) DETACH DELETE n`, nil)
	})
	repo := newQueryRepo(client)

	_, err := repo.GetNodeAncestry(ctx, "does-not-exist", 0)
	require.True(t, errors.Is(err, domain.ErrNodeNotFound))

	// "a" is a root: only itself, depth 0.
	got, err := repo.GetNodeAncestry(ctx, "a", 0)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "a", got[0].UniqueID)
	assert.Equal(t, 0, got[0].Depth)
}

