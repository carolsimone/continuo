//go:build integration

package neo4jinfra

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/catalog/service/export/graphschema"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// swapFixture gives one test its own tenant on the test Neo4j, and promotes
// releases for it through the unit of work, as the topology group does. Every
// node the swap writes carries the tenant, and so does every node a test
// seeds; the cleanup DETACH DELETEs the tenant's nodes of every label.
type swapFixture struct {
	t      *testing.T
	client Neo4jClient
	uow    uow.UnitOfWork
	tenant string
	seq    int64
}

func newSwapFixture(t *testing.T) *swapFixture {
	t.Helper()
	client := integrationClient(t)
	return newSwapFixtureOn(t, client, NewUnitOfWork(client, quietLogger()))
}

func newSwapFixtureOn(t *testing.T, client Neo4jClient, u uow.UnitOfWork) *swapFixture {
	t.Helper()
	f := &swapFixture{t: t, client: client, uow: u, tenant: "swap-test-" + uuid.NewString()}
	t.Cleanup(func() { deleteTenantNodes(t, client, f.tenant) })
	return f
}

// another returns a fixture for a second tenant on the same graph.
func (f *swapFixture) another() *swapFixture {
	return newSwapFixtureOn(f.t, f.client, f.uow)
}

// deleteTenantNodes DETACH DELETEs every node of tenant, whatever its label.
func deleteTenantNodes(t *testing.T, client Neo4jClient, tenant string) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = s.Close(ctx) }()
	for _, label := range graphschema.Labels() {
		stmt := fmt.Sprintf("MATCH (n:%s {tenant_id: $tenant_id}) DETACH DELETE n", label)
		res, err := s.Run(ctx, stmt, map[string]any{"tenant_id": tenant})
		if err == nil {
			_, err = res.Consume(ctx)
		}
		if err != nil {
			t.Errorf("cleanup %q: %v", stmt, err)
		}
	}
}

// promote applies a promotion of releaseID under the next seq, so each
// promotion of a test is newer than the one before it, as release-controller
// numbers them.
func (f *swapFixture) promote(releaseID string, nodes []topology.ReleasePromotedTopologyNode, now time.Time) topology.PromotionOutcome {
	f.t.Helper()
	f.seq++
	return f.promoteAt(releaseID, f.seq, nodes, now)
}

// promoteAt applies a promotion of releaseID at seq in one Write under a fresh
// message id, and returns what the swap did.
func (f *swapFixture) promoteAt(releaseID string, seq int64, nodes []topology.ReleasePromotedTopologyNode, now time.Time) topology.PromotionOutcome {
	f.t.Helper()
	outcome, err := f.tryPromoteAt(f.marker(), releaseID, seq, nodes, now)
	require.NoError(f.t, err)
	return outcome
}

func (f *swapFixture) tryPromoteAt(m uow.Marker, releaseID string, seq int64, nodes []topology.ReleasePromotedTopologyNode, now time.Time) (topology.PromotionOutcome, error) {
	var outcome topology.PromotionOutcome
	err := f.uow.Write(context.Background(), m, true, func(ctx context.Context, tx uow.Tx) error {
		var err error
		outcome, err = tx.Topology().PromoteRelease(ctx, f.tenant, releaseID, seq, nodes, now)
		return err
	})
	return outcome, err
}

// marker names a fresh message of the topology group for the tenant.
func (f *swapFixture) marker() uow.Marker {
	return uow.Marker{TenantID: f.tenant, Group: streams.CatalogReleaseTopology, MessageID: uuid.NewString()}
}

// exec runs one write statement with $tenant_id bound to the fixture's tenant.
func (f *swapFixture) exec(cypher string, params map[string]any) {
	f.t.Helper()
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = s.Close(ctx) }()
	res, err := s.Run(ctx, cypher, f.params(params))
	require.NoError(f.t, err)
	_, err = res.Consume(ctx)
	require.NoError(f.t, err)
}

// rows runs one read statement with $tenant_id bound to the fixture's tenant
// and returns its records.
func (f *swapFixture) rows(cypher string, params map[string]any) []*neo4j.Record {
	f.t.Helper()
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = s.Close(ctx) }()
	res, err := s.Run(ctx, cypher, f.params(params))
	require.NoError(f.t, err)
	recs, err := res.Collect(ctx)
	require.NoError(f.t, err)
	return recs
}

func (f *swapFixture) params(params map[string]any) map[string]any {
	bound := map[string]any{"tenant_id": f.tenant}
	for k, v := range params {
		bound[k] = v
	}
	return bound
}

// count runs a statement that returns one integer column n.
func (f *swapFixture) count(cypher string, params map[string]any) int64 {
	f.t.Helper()
	recs := f.rows(cypher, params)
	require.Len(f.t, recs, 1)
	n, _ := recs[0].Get("n")
	return n.(int64)
}

// column runs a statement that returns one string column v, in its order.
func (f *swapFixture) column(cypher string, params map[string]any) []string {
	f.t.Helper()
	out := []string{}
	for _, rec := range f.rows(cypher, params) {
		v, _ := rec.Get("v")
		out = append(out, v.(string))
	}
	return out
}

// node returns the properties of the tenant's :Node uniqueID, or nil when the
// tenant has none.
func (f *swapFixture) node(uniqueID string) map[string]any {
	f.t.Helper()
	recs := f.rows(`MATCH (n:Node {tenant_id: $tenant_id, unique_id: $uid}) RETURN properties(n) AS p`, map[string]any{"uid": uniqueID})
	require.LessOrEqual(f.t, len(recs), 1, "one :Node per tenant and unique_id")
	if len(recs) == 0 {
		return nil
	}
	p, _ := recs[0].Get("p")
	return p.(map[string]any)
}

// nodeIDs lists the unique_ids of the tenant's :Node, sorted.
func (f *swapFixture) nodeIDs() []string {
	return f.column(`MATCH (n:Node {tenant_id: $tenant_id}) RETURN n.unique_id AS v ORDER BY v`, nil)
}

// activeIDs lists the unique_ids of the tenant's live :Node, sorted.
func (f *swapFixture) activeIDs() []string {
	return f.column(`MATCH (n:Node {tenant_id: $tenant_id}) WHERE coalesce(n.active, true) RETURN n.unique_id AS v ORDER BY v`, nil)
}

// edges lists the tenant's :DEPENDS_ON as "downstream->upstream", sorted.
func (f *swapFixture) edges() []string {
	return f.column(`
		MATCH (a:Node {tenant_id: $tenant_id})-[:DEPENDS_ON]->(b:Node {tenant_id: $tenant_id})
		RETURN a.unique_id + '->' + b.unique_id AS v ORDER BY v`, nil)
}

// liveMeta returns the tenant's live release and promotion seq, and checks the
// tenant has one current_release node whose lock property never outlives the
// swap's transaction.
func (f *swapFixture) liveMeta() (string, int64) {
	f.t.Helper()
	recs := f.rows(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'current_release'}) RETURN properties(m) AS p`, nil)
	require.Len(f.t, recs, 1, "one current_release node per tenant")
	v, _ := recs[0].Get("p")
	p := v.(map[string]any)
	assert.NotContains(f.t, p, "_lock", "the lock property never outlives the swap's transaction")
	rid, _ := p["release_id"].(string)
	seq, _ := p["promotion_seq"].(int64)
	return rid, seq
}

// snapshot renders every node of the tenant but its processed-message markers,
// and every relationship from one, as sorted lines, so two snapshots compare
// equal exactly when the promotion between them wrote nothing else.
func (f *swapFixture) snapshot() []string {
	f.t.Helper()
	var lines []string
	for _, rec := range f.rows(`
		MATCH (n {tenant_id: $tenant_id}) WHERE NOT n:ProcessedMessage
		RETURN labels(n) AS l, properties(n) AS p`, nil) {
		l, _ := rec.Get("l")
		p, _ := rec.Get("p")
		lines = append(lines, fmt.Sprint("node ", l, p))
	}
	for _, rec := range f.rows(`
		MATCH (a {tenant_id: $tenant_id})-[r]->(b)
		RETURN properties(a) AS a, type(r) AS t, properties(r) AS p, properties(b) AS b`, nil) {
		a, _ := rec.Get("a")
		typ, _ := rec.Get("t")
		p, _ := rec.Get("p")
		b, _ := rec.Get("b")
		lines = append(lines, fmt.Sprint("rel ", a, " -", typ, p, "-> ", b))
	}
	slices.Sort(lines)
	return lines
}

// processed reports whether the topology group's marker of m is recorded.
func (f *swapFixture) processed(m uow.Marker) bool {
	f.t.Helper()
	ok, err := f.uow.Processed(context.Background(), m)
	require.NoError(f.t, err)
	return ok
}

func modelNode(uniqueID string, upstreams ...string) topology.ReleasePromotedTopologyNode {
	if upstreams == nil {
		upstreams = []string{}
	}
	return topology.ReleasePromotedTopologyNode{
		UniqueID: uniqueID, SchemaName: "analytics", TableName: uniqueID, ServiceName: "svc",
		NodeType: "dbt-model", ContentHash: "sha256:" + uniqueID, ImageTag: "img", Schedule: "daily",
		UpstreamUniqueIDs: upstreams,
	}
}

func releaseOf(nodes ...topology.ReleasePromotedTopologyNode) []topology.ReleasePromotedTopologyNode {
	return nodes
}

func TestPromoteRelease_FirstPromotionCreatesNodesEdgesAndMeta(t *testing.T) {
	f := newSwapFixture(t)

	outcome := f.promote("rel-1", releaseOf(
		modelNode("a"),
		modelNode("b", "a"),
		modelNode("c", "a"),
	), time.Now().UTC())
	assert.Equal(t, topology.PromotionApplied, outcome)

	assert.Equal(t, []string{"a", "b", "c"}, f.activeIDs())
	for _, id := range []string{"a", "b", "c"} {
		n := f.node(id)
		assert.Equal(t, "rel-1", n["release_id"], "node %s", id)
		assert.Equal(t, "daily", n["schedule_name"], "the graph names the schedule schedule_name")
		assert.Equal(t, true, n["active"])
		assert.NotContains(t, n, "retired_at")
	}
	assert.Equal(t, []string{"b->a", "c->a"}, f.edges())
	rid, seq := f.liveMeta()
	assert.Equal(t, "rel-1", rid)
	assert.Equal(t, int64(1), seq)
}

// TestPromoteRelease_KeepsDroppedNode is design §2.2's example: r7 carries
// table_d, r8 drops it. table_d stays, retired once, with its last version,
// its failure and its fix PR's edit; only its outgoing :DEPENDS_ON goes, and
// nothing is deleted.
func TestPromoteRelease_KeepsDroppedNode(t *testing.T) {
	f := newSwapFixture(t)
	const (
		a = "analytics.table_a"
		b = "analytics.table_b"
		d = "analytics.table_d"
	)
	r7At := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	require.Equal(t, topology.PromotionApplied, f.promoteAt("r7", 7, releaseOf(modelNode(a), modelNode(b, a), modelNode(d, a)), r7At))

	// table_d's history: its current version, a failure, and the fix PR that
	// edited it.
	f.exec(`
		MATCH (d:Node {tenant_id: $tenant_id, unique_id: $d})
		CREATE (v:NodeVersion {tenant_id: $tenant_id, unique_id: $d, content_hash: 'd4e1', version_seq: 1})
		CREATE (d)-[:CURRENT {release_id: 'r7'}]->(v)
		CREATE (rej:Rejection {tenant_id: $tenant_id, release_id: 'r6', node_id: $d, category: 'compile', reason: 'syntax'})
		CREATE (d)-[:FAILED {release_id: 'r6'}]->(rej)
		CREATE (p:Proposal {tenant_id: $tenant_id, proposal_id: 'prop-d'})
		CREATE (rej)-[:PROPOSED {service: 'svc'}]->(p)
		CREATE (p)-[:EDITED {path: 'models/table_d.sql'}]->(d)`, map[string]any{"d": d})
	nodesBefore := f.count(`MATCH (n {tenant_id: $tenant_id}) WHERE NOT n:ProcessedMessage RETURN count(n) AS n`, nil)
	propsBefore := f.node(d)

	r8At := r7At.Add(13 * 24 * time.Hour)
	require.Equal(t, topology.PromotionApplied, f.promoteAt("r8", 8, releaseOf(modelNode(a), modelNode(b, a)), r8At))

	dropped := f.node(d)
	require.NotNil(t, dropped, "a dropped node stays in the graph")
	assert.Equal(t, false, dropped["active"])
	retiredAt, ok := dropped["retired_at"].(time.Time)
	require.True(t, ok, "retired_at is a datetime, got %T", dropped["retired_at"])
	assert.True(t, r8At.Equal(retiredAt), "retired_at is the time of the release that dropped it: %v", retiredAt)
	for _, prop := range []string{"release_id", "schema_name", "table_name", "content_hash", "schedule_name"} {
		assert.Equal(t, propsBefore[prop], dropped[prop], "a retired node keeps its %s", prop)
	}
	edgeOf := func(cypher string) int64 { return f.count(cypher, map[string]any{"d": d}) }
	assert.Equal(t, int64(1), edgeOf(`MATCH (:Node {tenant_id: $tenant_id, unique_id: $d})-[r:CURRENT]->(:NodeVersion {tenant_id: $tenant_id}) RETURN count(r) AS n`), ":CURRENT kept")
	assert.Equal(t, int64(1), edgeOf(`MATCH (:Node {tenant_id: $tenant_id, unique_id: $d})-[r:FAILED]->(:Rejection {tenant_id: $tenant_id}) RETURN count(r) AS n`), ":FAILED kept")
	assert.Equal(t, int64(1), edgeOf(`MATCH (:Proposal {tenant_id: $tenant_id})-[r:EDITED]->(:Node {tenant_id: $tenant_id, unique_id: $d}) RETURN count(r) AS n`), ":EDITED kept")
	assert.Zero(t, edgeOf(`MATCH (:Node {tenant_id: $tenant_id, unique_id: $d})-[r:DEPENDS_ON]->() RETURN count(r) AS n`), "a retired node loses its outgoing :DEPENDS_ON")
	assert.Equal(t, []string{"analytics.table_b->analytics.table_a"}, f.edges())
	assert.Equal(t, nodesBefore, f.count(`MATCH (n {tenant_id: $tenant_id}) WHERE NOT n:ProcessedMessage RETURN count(n) AS n`, nil), "nothing is deleted")
	assert.Equal(t, []string{a, b}, f.activeIDs())

	// A later release that still leaves table_d out does not move retired_at.
	require.Equal(t, topology.PromotionApplied, f.promoteAt("r9", 9, releaseOf(modelNode(a), modelNode(b, a)), r8At.Add(24*time.Hour)))
	again, _ := f.node(d)["retired_at"].(time.Time)
	assert.True(t, r8At.Equal(again), "retired_at is set once, at the retirement: %v", again)
}

// A node a release carries again comes back live; dropped again later, it is
// stamped with the new retirement.
func TestPromoteRelease_ReturningNodeIsLiveAgain(t *testing.T) {
	f := newSwapFixture(t)
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	f.promote("r1", releaseOf(modelNode("a"), modelNode("b", "a")), base)
	f.promote("r2", releaseOf(modelNode("a")), base.Add(time.Hour))
	require.Equal(t, false, f.node("b")["active"])

	f.promote("r3", releaseOf(modelNode("a"), modelNode("b", "a")), base.Add(2*time.Hour))
	back := f.node("b")
	assert.Equal(t, true, back["active"])
	assert.NotContains(t, back, "retired_at", "a live node has no retired_at")
	assert.Equal(t, "r3", back["release_id"])
	assert.Equal(t, []string{"b->a"}, f.edges())

	droppedAgain := base.Add(3 * time.Hour)
	f.promote("r4", releaseOf(modelNode("a")), droppedAgain)
	got, _ := f.node("b")["retired_at"].(time.Time)
	assert.True(t, droppedAgain.Equal(got), "a second retirement is stamped with its own time: %v", got)
}

// A retired node stays in the graph, so an upstream the release names that is
// only a retired node must draw no edge: :DEPENDS_ON joins live nodes only.
func TestPromoteRelease_UpstreamOnARetiredNodeDrawsNoEdge(t *testing.T) {
	f := newSwapFixture(t)
	now := time.Now().UTC()
	f.promote("r1", releaseOf(modelNode("gone"), modelNode("a")), now)
	f.promote("r2", releaseOf(modelNode("a")), now)
	require.Equal(t, false, f.node("gone")["active"])

	f.promote("r3", releaseOf(modelNode("a"), modelNode("b", "a", "gone")), now)
	assert.Equal(t, []string{"b->a"}, f.edges(), "no edge to the retired node")
	assert.Equal(t, false, f.node("gone")["active"], "naming a retired node as an upstream does not revive it")
}

// A :Node with no unique_id cannot be one the release carries: the swap
// retires it rather than leave it live beside the release's nodes.
func TestPromoteRelease_RetiresNodesWithoutUniqueID(t *testing.T) {
	f := newSwapFixture(t)
	f.exec(`CREATE (:Node {tenant_id: $tenant_id, schema_name: 'public', table_name: 'no_id', active: true})`, nil)

	require.Equal(t, topology.PromotionApplied, f.promote("rel-1", releaseOf(modelNode("public.new_tbl")), time.Now().UTC()))

	assert.Zero(t, f.count(`MATCH (n:Node {tenant_id: $tenant_id}) WHERE n.unique_id IS NULL AND coalesce(n.active, true) RETURN count(n) AS n`, nil),
		"no :Node without a unique_id stays live")
	assert.Equal(t, int64(1), f.count(`MATCH (n:Node {tenant_id: $tenant_id, table_name: 'no_id'}) RETURN count(n) AS n`, nil),
		"the node is retired, not deleted")
	assert.Equal(t, []string{"public.new_tbl"}, f.activeIDs())
}

// A node retired by an earlier promotion is not matched by the retire step
// of a later one, with or without a unique_id: its retired_at stays the time
// of its retirement however many promotions leave it out.
func TestPromoteRelease_ANodeRetiredEarlierKeepsItsRetiredAt(t *testing.T) {
	f := newSwapFixture(t)
	f.exec(`CREATE (:Node {tenant_id: $tenant_id, schema_name: 'public', table_name: 'no_id', active: true})`, nil)
	retiredAt := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	f.promote("rel-1", releaseOf(modelNode("public.a"), modelNode("public.b")), retiredAt.Add(-time.Hour))
	f.promote("rel-2", releaseOf(modelNode("public.a")), retiredAt)

	noID := func() map[string]any {
		recs := f.rows(`MATCH (n:Node {tenant_id: $tenant_id, table_name: 'no_id'}) RETURN properties(n) AS p`, nil)
		require.Len(t, recs, 1)
		p, _ := recs[0].Get("p")
		return p.(map[string]any)
	}
	for i := range 2 {
		f.promote(fmt.Sprintf("rel-%d", 3+i), releaseOf(modelNode("public.a")), retiredAt.Add(time.Duration(i+1)*24*time.Hour))
		for name, props := range map[string]map[string]any{"public.b": f.node("public.b"), "the node with no unique_id": noID()} {
			assert.Equal(t, false, props["active"], name)
			got, ok := props["retired_at"].(time.Time)
			require.True(t, ok, "%s: retired_at is a datetime, got %T", name, props["retired_at"])
			if name == "public.b" {
				assert.True(t, retiredAt.Equal(got), "%s: retired_at is the time it was retired, got %v", name, got)
			} else {
				assert.True(t, retiredAt.Add(-time.Hour).Equal(got), "%s: retired_at is the time it was retired, got %v", name, got)
			}
		}
	}
}

// An empty release retires every node of the tenant and still becomes the
// live promotion.
func TestPromoteRelease_EmptyReleaseRetiresEveryNodeAndRecordsTheMeta(t *testing.T) {
	f := newSwapFixture(t)
	f.promote("rel-1", releaseOf(modelNode("a"), modelNode("b", "a")), time.Now().UTC())

	assert.Equal(t, topology.PromotionApplied, f.promote("rel-empty", nil, time.Now().UTC()))

	assert.Empty(t, f.activeIDs())
	assert.Equal(t, []string{"a", "b"}, f.nodeIDs(), "retired, not deleted")
	assert.Empty(t, f.edges())
	rid, seq := f.liveMeta()
	assert.Equal(t, "rel-empty", rid)
	assert.Equal(t, int64(2), seq)
}

// An upstream the graph does not hold (a dependency outside this topology)
// draws no edge and fails nothing.
func TestPromoteRelease_UnknownUpstreamSkipsEdgeButCreatesNode(t *testing.T) {
	f := newSwapFixture(t)
	assert.Equal(t, topology.PromotionApplied, f.promote("rel-1", releaseOf(modelNode("a", "dangling")), time.Now().UTC()))
	assert.Equal(t, []string{"a"}, f.nodeIDs())
	assert.Empty(t, f.edges())
}

// A promotion of one tenant neither reads nor changes another tenant's nodes,
// even under the same unique_ids.
func TestPromoteRelease_LeavesOtherTenantsAlone(t *testing.T) {
	a := newSwapFixture(t)
	b := a.another()
	now := time.Now().UTC()
	b.promote("b-1", releaseOf(modelNode("x"), modelNode("y", "x")), now)
	before := b.snapshot()

	a.promote("a-1", releaseOf(modelNode("x"), modelNode("z", "x")), now)
	a.promote("a-2", releaseOf(modelNode("z")), now)

	assert.Equal(t, before, b.snapshot(), "tenant b's graph is untouched")
	assert.Equal(t, []string{"z"}, a.activeIDs())
	assert.Equal(t, []string{"x", "z"}, a.nodeIDs())
	rid, seq := b.liveMeta()
	assert.Equal(t, "b-1", rid)
	assert.Equal(t, int64(1), seq)
}

// Code-version provenance lives on :NodeVersion; the swap stamps no last_*
// properties on a node.
func TestPromoteRelease_DoesNotStampLastProvenance(t *testing.T) {
	f := newSwapFixture(t)
	f.promote("rel-1", releaseOf(modelNode("a")), time.Now().UTC())
	n := f.node("a")
	for _, k := range []string{"last_commit_sha", "last_repo", "last_changed_at", "last_release_id"} {
		assert.NotContains(t, n, k)
	}
}

func TestPromoteRelease_PersistsTestCountAndOriginalFilePath(t *testing.T) {
	f := newSwapFixture(t)
	node := modelNode("a")
	node.TestCount = 2
	node.OriginalFilePath = "models/a.sql"
	f.promote("rel-1", releaseOf(node), time.Now().UTC())

	n := f.node("a")
	assert.Equal(t, int64(2), n["test_count"])
	assert.Equal(t, "models/a.sql", n["original_file_path"])
}

// A node promoted with a secret_ref stores it, and a later release that drops
// it clears the property rather than storing an empty string or keeping the
// stale ref.
func TestPromoteRelease_SetsAndClearsSecretRef(t *testing.T) {
	f := newSwapFixture(t)
	withRef := modelNode("analytics.fx")
	withRef.NodeType = "python-api"
	withRef.SecretRef = "continuo-api-fx"
	f.promote("rel-1", releaseOf(withRef), time.Now().UTC())
	assert.Equal(t, "continuo-api-fx", f.node("analytics.fx")["secret_ref"])

	withoutRef := withRef
	withoutRef.SecretRef = ""
	f.promote("rel-2", releaseOf(withoutRef), time.Now().UTC())
	assert.NotContains(t, f.node("analytics.fx"), "secret_ref", "an empty secret_ref clears the property")
}

// :Node.content_hash is the hash the promotion carried, refreshed in place on
// every release, changed or not: version ingestion compares against it.
func TestPromoteRelease_RefreshesContentHash(t *testing.T) {
	f := newSwapFixture(t)
	a := modelNode("a")
	a.ContentHash = "sha256:v1"
	b := modelNode("b")
	b.ContentHash = "sha256:bbb"
	f.promote("rel-1", releaseOf(a, b), time.Now().UTC())
	assert.Equal(t, "sha256:v1", f.node("a")["content_hash"])
	assert.Equal(t, "sha256:bbb", f.node("b")["content_hash"])

	a.ContentHash = "sha256:v2"
	f.promote("rel-2", releaseOf(a, b), time.Now().UTC())
	assert.Equal(t, "sha256:v2", f.node("a")["content_hash"])
	assert.Equal(t, "sha256:bbb", f.node("b")["content_hash"])
}
