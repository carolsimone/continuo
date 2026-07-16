package neo4jinfra_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
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
	// Also clean up any :Run nodes created during tests.
	res3, err := s.Run(ctx, `MATCH (n:Run) DETACH DELETE n`, nil)
	require.NoError(t, err)
	_, err = res3.Consume(ctx)
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

	// Assert exactly 3 :Table nodes with the expected unique_ids, release_id,
	// and schedule_name (the Neo4j property name; wire DTO field stays "schedule").
	res, err := s.Run(ctx, `MATCH (t:Table) RETURN t.unique_id AS uid, t.release_id AS rid, t.schedule_name AS sname ORDER BY uid`, nil)
	require.NoError(t, err)
	var uids []string
	for res.Next(ctx) {
		uid, _ := res.Record().Get("uid")
		rid, _ := res.Record().Get("rid")
		sname, _ := res.Record().Get("sname")
		uids = append(uids, uid.(string))
		assert.Equal(t, "rel-1", rid)
		assert.Equal(t, "daily", sname)
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

// TestReleasePromotionRepository_RetiresTablesWithoutUniqueID verifies that a
// promotion retires any :Table node carrying no unique_id, rather than leaving
// it active beside the unique_id-keyed nodes the release creates. A node with no
// incoming :Run reference is orphan-deleted; one referenced by a :Run is kept
// (retired) so run history survives. This guards the retire query's
// `unique_id IS NULL` clause: a node whose `unique_id IN $list` evaluates to NULL
// would otherwise never match the negation and would stay active.
func TestReleasePromotionRepository_RetiresTablesWithoutUniqueID(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t) // skips if Neo4j is unreachable
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	// Seed two active :Table nodes that carry no unique_id property (keyed only
	// by schema/table). One is referenced by a :Run-[:EXECUTES] edge; the other
	// is unreferenced.
	wSession := client.NewSession(ctx, neo4j.AccessModeWrite)
	seedRes, err := wSession.Run(ctx, `
		CREATE (:Table {schema_name:'public', table_name:'noid_orphan', service_name:'svc', schedule_name:'daily', active:true})
		CREATE (referenced:Table {schema_name:'public', table_name:'noid_referenced', service_name:'svc', schedule_name:'daily', active:true})
		CREATE (r:Run {run_id:'legacy-run'})
		CREATE (r)-[:EXECUTES]->(referenced)
	`, nil)
	require.NoError(t, err)
	_, err = seedRes.Consume(ctx)
	require.NoError(t, err)
	wSession.Close(ctx)

	repo := newReleaseRepo(client)
	nodes := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "public.new_tbl", SchemaName: "public", TableName: "new_tbl", ServiceName: "svc", ImageTag: "sha:1", Schedule: "daily", UpstreamUniqueIDs: []string{}},
	}
	changed, err := repo.PromoteRelease(ctx, "rel-upgrade", nodes, time.Now().UTC())
	require.NoError(t, err)
	assert.True(t, changed)

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)

	// No unique_id-less :Table may remain active after the promotion.
	activeNoID, err := s.Run(ctx,
		`MATCH (t:Table) WHERE t.unique_id IS NULL AND COALESCE(t.active, true) RETURN count(t) AS n`, nil)
	require.NoError(t, err)
	require.True(t, activeNoID.Next(ctx))
	n, _ := activeNoID.Record().Get("n")
	assert.Equal(t, int64(0), n, "no unique_id-less :Table may remain active after promotion")

	// The unreferenced node is orphan-deleted.
	orphanRes, err := s.Run(ctx, `MATCH (t:Table {table_name:'noid_orphan'}) RETURN count(t) AS n`, nil)
	require.NoError(t, err)
	require.True(t, orphanRes.Next(ctx))
	on, _ := orphanRes.Record().Get("n")
	assert.Equal(t, int64(0), on, "unreferenced unique_id-less node must be orphan-deleted")

	// The run-referenced node survives as retired so run history is preserved.
	refRes, err := s.Run(ctx,
		`MATCH (:Run {run_id:'legacy-run'})-[:EXECUTES]->(t:Table {table_name:'noid_referenced'}) RETURN t.active AS active`, nil)
	require.NoError(t, err)
	require.True(t, refRes.Next(ctx), "run-referenced unique_id-less node must survive (retired)")
	active, _ := refRes.Record().Get("active")
	assert.Equal(t, false, active, "run-referenced node must be retired, not active")

	// The new unique_id-keyed node is active.
	newRes, err := s.Run(ctx, `MATCH (t:Table {unique_id:'public.new_tbl'}) RETURN t.active AS active`, nil)
	require.NoError(t, err)
	require.True(t, newRes.Next(ctx))
	na, _ := newRes.Record().Get("active")
	assert.Equal(t, true, na)
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
// second PromoteRelease call with a different release_id retires unreferenced
// old :Table nodes and replaces them with the new topology. Nodes that carry no
// :Run-[:EXECUTES] reference are deleted by the orphan-cleanup step.
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

	// Old nodes (a, b) had no Run references so the orphan-cleanup step deletes
	// them. Only c should remain.
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

// TestReleasePromotionRepository_PreservesRunExecutesEdgesWhenRetiringTables
// verifies that a :Table removed from the new topology is NOT deleted when a
// :Run-[:EXECUTES] edge still points at it. The node is retired (active=false)
// but remains in the graph so that run history is intact.
func TestReleasePromotionRepository_PreservesRunExecutesEdgesWhenRetiringTables(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	// Promote release rA with a single :Table node "a".
	repo := newReleaseRepo(client)
	nodesA := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "a", SchemaName: "p", TableName: "ta", ServiceName: "s", ImageTag: "x", Schedule: "daily", UpstreamUniqueIDs: []string{}},
	}
	_, err := repo.PromoteRelease(ctx, "rA", nodesA, time.Now().UTC())
	require.NoError(t, err)

	// Manually create a :Run node and a :Run-[:EXECUTES]->:Table{unique_id:"a"} edge,
	// simulating a run that was launched against the rA topology.
	wSession := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err = wSession.Run(ctx, `
		MATCH (t:Table {unique_id: 'a'})
		CREATE (r:Run {run_id: 'test-run'})-[:EXECUTES]->(t)
	`, nil)
	require.NoError(t, err)
	wSession.Close(ctx)

	// Promote release rB with a different :Table node "b" (no "a").
	nodesB := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "b", SchemaName: "p", TableName: "tb", ServiceName: "s", ImageTag: "y", Schedule: "daily", UpstreamUniqueIDs: []string{}},
	}
	changed, err := repo.PromoteRelease(ctx, "rB", nodesB, time.Now().UTC())
	require.NoError(t, err)
	assert.True(t, changed)

	rSession := client.NewSession(ctx, neo4j.AccessModeRead)
	defer rSession.Close(ctx)

	// :Table "a" must still exist (referenced by the Run) but be retired.
	tableRes, err := rSession.Run(ctx, `
		MATCH (t:Table {unique_id: 'a'})
		RETURN t.active AS active, t.retired_at AS retired_at
	`, nil)
	require.NoError(t, err)
	require.True(t, tableRes.Next(ctx), ":Table{unique_id:'a'} must still exist after rB promotion")
	active, _ := tableRes.Record().Get("active")
	retiredAt, _ := tableRes.Record().Get("retired_at")
	assert.Equal(t, false, active, ":Table 'a' must be inactive (retired)")
	assert.NotNil(t, retiredAt, ":Table 'a' must have a non-null retired_at")

	// The :Run-[:EXECUTES]->:Table edge must still resolve.
	edgeRes, err := rSession.Run(ctx, `
		MATCH (:Run {run_id: 'test-run'})-[:EXECUTES]->(t:Table {unique_id: 'a'})
		RETURN count(*) AS n
	`, nil)
	require.NoError(t, err)
	require.True(t, edgeRes.Next(ctx))
	edgeCount, _ := edgeRes.Record().Get("n")
	assert.Equal(t, int64(1), edgeCount, ":Run-[:EXECUTES]->:Table edge must survive the swap")

	// :Table "b" must exist and be active.
	bRes, err := rSession.Run(ctx, `MATCH (t:Table {unique_id: 'b'}) RETURN t.active AS active`, nil)
	require.NoError(t, err)
	require.True(t, bRes.Next(ctx), ":Table{unique_id:'b'} must exist after rB promotion")
	bActive, _ := bRes.Record().Get("active")
	assert.Equal(t, true, bActive, ":Table 'b' must be active")

	// :Meta must reflect the new release_id.
	metaRes, err := rSession.Run(ctx, `MATCH (m:Meta {key:'current_release'}) RETURN m.release_id AS rid`, nil)
	require.NoError(t, err)
	require.True(t, metaRes.Next(ctx))
	rid, _ := metaRes.Record().Get("rid")
	assert.Equal(t, "rB", rid)
}

// TestReleasePromotionRepository_StampsProvenanceOnlyOnChangedNodes verifies
// that PromoteRelease writes last_commit_sha/last_repo/last_changed_at/
// last_release_id onto nodes flagged Changed, and leaves an unchanged node's
// provenance untouched across a subsequent promotion.
func TestReleasePromotionRepository_StampsProvenanceOnlyOnChangedNodes(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t) // skips if Neo4j is unreachable
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	repo := newReleaseRepo(client)

	// rel-1: both a and b are new → both changed, stamped with commit c1.
	t1 := time.Date(2026, 6, 18, 9, 0, 0, 0, time.UTC)
	first := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "a", SchemaName: "p", TableName: "ta", ServiceName: "s", ImageTag: "x", Schedule: "d",
			Changed: true, LastCommitSHA: "c1", LastRepo: "acme/demo", LastChangedAt: t1},
		{UniqueID: "b", SchemaName: "p", TableName: "tb", ServiceName: "s", ImageTag: "x", Schedule: "d", UpstreamUniqueIDs: []string{"a"},
			Changed: true, LastCommitSHA: "c1", LastRepo: "acme/demo", LastChangedAt: t1},
	}
	_, err := repo.PromoteRelease(ctx, "rel-1", first, time.Now().UTC())
	require.NoError(t, err)

	// rel-2: a unchanged (keeps c1), b changed → stamped with commit c2.
	t2 := time.Date(2026, 6, 18, 10, 0, 0, 0, time.UTC)
	second := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "a", SchemaName: "p", TableName: "ta", ServiceName: "s", ImageTag: "x", Schedule: "d",
			Changed: false},
		{UniqueID: "b", SchemaName: "p", TableName: "tb", ServiceName: "s", ImageTag: "y", Schedule: "d", UpstreamUniqueIDs: []string{"a"},
			Changed: true, LastCommitSHA: "c2", LastRepo: "acme/demo", LastChangedAt: t2},
	}
	_, err = repo.PromoteRelease(ctx, "rel-2", second, time.Now().UTC())
	require.NoError(t, err)

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)

	res, err := s.Run(ctx, `
		MATCH (t:Table)
		RETURN t.unique_id AS uid, t.last_commit_sha AS sha, t.last_repo AS repo, t.last_release_id AS rid, t.last_changed_at AS changed_at
		ORDER BY uid
	`, nil)
	require.NoError(t, err)
	got := map[string]map[string]any{}
	for res.Next(ctx) {
		uid, _ := res.Record().Get("uid")
		sha, _ := res.Record().Get("sha")
		repoV, _ := res.Record().Get("repo")
		rid, _ := res.Record().Get("rid")
		changedAt, _ := res.Record().Get("changed_at")
		got[uid.(string)] = map[string]any{"sha": sha, "repo": repoV, "rid": rid, "changed_at": changedAt}
	}
	require.NoError(t, res.Err())

	// a kept rel-1's provenance (unchanged in rel-2).
	assert.Equal(t, "c1", got["a"]["sha"])
	assert.Equal(t, "acme/demo", got["a"]["repo"])
	assert.Equal(t, "rel-1", got["a"]["rid"])
	gotA := got["a"]["changed_at"].(time.Time)
	assert.True(t, t1.Equal(gotA), "a keeps rel-1 timestamp; got %v", gotA)

	// b was re-stamped with rel-2's provenance.
	assert.Equal(t, "c2", got["b"]["sha"])
	assert.Equal(t, "acme/demo", got["b"]["repo"])
	assert.Equal(t, "rel-2", got["b"]["rid"])
	gotB := got["b"]["changed_at"].(time.Time)
	assert.True(t, t2.Equal(gotB), "b gets rel-2 timestamp; got %v", gotB)
}

// TestReleasePromotionRepository_DeletesOrphanedTablesWithNoRunReferences
// verifies that a :Table removed from the new topology IS deleted when no
// :Run-[:EXECUTES] edge points at it. The orphan-cleanup step removes it.
func TestReleasePromotionRepository_DeletesOrphanedTablesWithNoRunReferences(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	// Promote release rA with a single :Table node "a". No Run is created.
	repo := newReleaseRepo(client)
	nodesA := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "a", SchemaName: "p", TableName: "ta", ServiceName: "s", ImageTag: "x", Schedule: "daily", UpstreamUniqueIDs: []string{}},
	}
	_, err := repo.PromoteRelease(ctx, "rA", nodesA, time.Now().UTC())
	require.NoError(t, err)

	// Promote release rB without "a" in the topology. Since no Run references "a",
	// the orphan-cleanup step must delete it.
	nodesB := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "b", SchemaName: "p", TableName: "tb", ServiceName: "s", ImageTag: "y", Schedule: "daily", UpstreamUniqueIDs: []string{}},
	}
	changed, err := repo.PromoteRelease(ctx, "rB", nodesB, time.Now().UTC())
	require.NoError(t, err)
	assert.True(t, changed)

	rSession := client.NewSession(ctx, neo4j.AccessModeRead)
	defer rSession.Close(ctx)

	// :Table "a" must be gone (no Run referenced it).
	aRes, err := rSession.Run(ctx, `MATCH (t:Table {unique_id: 'a'}) RETURN count(t) AS n`, nil)
	require.NoError(t, err)
	require.True(t, aRes.Next(ctx))
	aCount, _ := aRes.Record().Get("n")
	assert.Equal(t, int64(0), aCount, ":Table 'a' must be deleted by orphan-cleanup (no Run references it)")

	// :Table "b" must exist.
	bRes, err := rSession.Run(ctx, `MATCH (t:Table {unique_id: 'b'}) RETURN count(t) AS n`, nil)
	require.NoError(t, err)
	require.True(t, bRes.Next(ctx))
	bCount, _ := bRes.Record().Get("n")
	assert.Equal(t, int64(1), bCount, ":Table 'b' must exist after rB promotion")
}

// TestPromotion_PersistsTestCount verifies that PromoteRelease writes
// test_count onto the resulting :Table node, so that later readers (and
// eventually single-node-run gating) can see how many dbt tests a node
// carries.
func TestPromotion_PersistsTestCount(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t) // skips if Neo4j is unreachable
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	repo := newReleaseRepo(client)
	nodes := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "a", SchemaName: "p", TableName: "ta", ServiceName: "s", ImageTag: "x", Schedule: "d",
			TestCount: 2, UpstreamUniqueIDs: []string{}},
	}
	changed, err := repo.PromoteRelease(ctx, "rel-1", nodes, time.Now().UTC())
	require.NoError(t, err)
	assert.True(t, changed)

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `MATCH (t:Table {unique_id:'a'}) RETURN t.test_count AS test_count`, nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	tc, _ := res.Record().Get("test_count")
	assert.Equal(t, int64(2), tc)
}

func TestPromotion_PersistsRuntimeManifestPin(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t) // skips if Neo4j is unreachable
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	repo := newReleaseRepo(client)
	ref := pkgModel.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://artifacts/s/rel-1/partial_parse.msgpack",
		RuntimeManifestSHA256:             "1111111111111111111111111111111111111111111111111111111111111111",
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: "2222222222222222222222222222222222222222222222222222222222222222",
	}
	nodes := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "p.ta", SchemaName: "p", TableName: "ta", ServiceName: "s", ImageTag: "x", Schedule: "d",
			UpstreamUniqueIDs: []string{}, DBTUniqueID: "model.finance.ta", RuntimeManifestRef: ref},
	}
	changed, err := repo.PromoteRelease(ctx, "rel-1", nodes, time.Now().UTC())
	require.NoError(t, err)
	assert.True(t, changed)

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `
		MATCH (t:Table {unique_id:'p.ta'})
		RETURN t.dbt_unique_id AS dbt_unique_id,
		       t.runtime_manifest_uri AS uri,
		       t.runtime_manifest_sha256 AS sha,
		       t.runtime_manifest_dbt_version AS ver,
		       t.runtime_manifest_parse_context_sha256 AS ctx`, nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	rec := res.Record()
	// The graph keys on "schema.table"; the dbt id is a separate identity.
	dbtUID, _ := rec.Get("dbt_unique_id")
	assert.Equal(t, "model.finance.ta", dbtUID)
	uri, _ := rec.Get("uri")
	assert.Equal(t, ref.RuntimeManifestURI, uri)
	sha, _ := rec.Get("sha")
	assert.Equal(t, ref.RuntimeManifestSHA256, sha)
	ver, _ := rec.Get("ver")
	assert.Equal(t, ref.RuntimeManifestDBTVersion, ver)
	pctx, _ := rec.Get("ctx")
	assert.Equal(t, ref.RuntimeManifestParseContextSHA256, pctx)
}

func TestReleasePromotionRepository_SetsOriginalFilePathUnconditionally(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t) // skips if Neo4j unreachable
	wipeReleaseFixtures(t, client)
	t.Cleanup(func() { wipeReleaseFixtures(t, client) })

	repo := newReleaseRepo(client)
	// node "a" is unchanged (Changed:false) — file_path must STILL be set.
	nodes := []topology.ReleasePromotedTopologyNode{
		{UniqueID: "a", SchemaName: "p", TableName: "ta", ServiceName: "s", ImageTag: "x", Schedule: "d",
			OriginalFilePath: "models/a.sql", Changed: false},
	}
	_, err := repo.PromoteRelease(ctx, "rel-1", nodes, time.Now().UTC())
	require.NoError(t, err)

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `MATCH (t:Table {unique_id:'a'}) RETURN t.original_file_path AS fp`, nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	fp, _ := res.Record().Get("fp")
	assert.Equal(t, "models/a.sql", fp)
}
