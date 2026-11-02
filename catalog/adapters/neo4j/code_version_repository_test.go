//go:build integration

package neo4jinfra

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests of this file write code versions for the swap fixture's tenant
// through the unit of work, one batch per Write, as the versions group does.

// writeBatch writes in for the fixture's tenant in one final Write under a
// fresh message of the versions group, and returns what it wrote.
func (f *swapFixture) writeBatch(in codeversion.WriteInput) codeversion.WriteResult {
	f.t.Helper()
	var res codeversion.WriteResult
	m := uow.Marker{TenantID: f.tenant, Group: streams.CatalogReleaseVersions, MessageID: uuid.NewString()}
	err := f.uow.Write(context.Background(), m, true, func(ctx context.Context, tx uow.Tx) error {
		var err error
		res, err = tx.CodeVersions().WriteBatch(ctx, f.tenant, in)
		return err
	})
	require.NoError(f.t, err)
	return res
}

// seedLiveNode creates a live :Node of the tenant, as a swap would have.
func (f *swapFixture) seedLiveNode(uniqueID string) {
	f.t.Helper()
	f.exec(`CREATE (:Node {tenant_id: $tenant_id, unique_id: $uid, schema_name: 'live', table_name: $uid,
		service_name: 'svc', node_type: 'dbt-model', content_hash: 'sha256:live', release_id: 'r-live', active: true})`,
		map[string]any{"uid": uniqueID})
}

// seedRejection records an open failure of nodeID: a :Rejection with no
// [:RESOLVED_BY].
func (f *swapFixture) seedRejection(releaseID, nodeID string, at time.Time) {
	f.t.Helper()
	f.exec(`CREATE (:Rejection {tenant_id: $tenant_id, release_id: $release_id, node_id: $node_id, at: $at})`,
		map[string]any{"release_id": releaseID, "node_id": nodeID, "at": at})
}

// current returns the content_hash of uniqueID's :CURRENT version, or "" when
// it has none.
func (f *swapFixture) current(uniqueID string) string {
	f.t.Helper()
	hashes := f.column(`
		MATCH (:Node {tenant_id: $tenant_id, unique_id: $uid})-[:CURRENT]->(v:NodeVersion {tenant_id: $tenant_id})
		RETURN v.content_hash AS v`, map[string]any{"uid": uniqueID})
	require.LessOrEqual(f.t, len(hashes), 1, "a node has at most one :CURRENT")
	if len(hashes) == 0 {
		return ""
	}
	return hashes[0]
}

// unitCurrent returns the checksum of unitID's :CURRENT version, or "".
func (f *swapFixture) unitCurrent(unitID string) string {
	f.t.Helper()
	sums := f.column(`
		MATCH (:CodeUnit {tenant_id: $tenant_id, unit_id: $unit_id})-[:CURRENT]->(v:CodeUnitVersion {tenant_id: $tenant_id})
		RETURN v.checksum AS v`, map[string]any{"unit_id": unitID})
	require.LessOrEqual(f.t, len(sums), 1, "a unit has at most one :CURRENT")
	if len(sums) == 0 {
		return ""
	}
	return sums[0]
}

// versionCount counts uniqueID's :NodeVersion nodes.
func (f *swapFixture) versionCount(uniqueID string) int64 {
	f.t.Helper()
	return f.count(`MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: $uid}) RETURN count(v) AS n`,
		map[string]any{"uid": uniqueID})
}

// unitWatermark returns the code_version_promotion_seq of unitID's :CodeUnit.
func (f *swapFixture) unitWatermark(unitID string) any {
	f.t.Helper()
	recs := f.rows(`MATCH (cu:CodeUnit {tenant_id: $tenant_id, unit_id: $unit_id}) RETURN properties(cu) AS p`,
		map[string]any{"unit_id": unitID})
	require.Len(f.t, recs, 1)
	p, _ := recs[0].Get("p")
	props := p.(map[string]any)
	assert.NotContains(f.t, props, "code_version_promoted_at", "the watermark is the promotion seq, not a time")
	return props["code_version_promotion_seq"]
}

// versionOf is one bundle node's code state.
func versionOf(uniqueID, hash string, refs ...codeversion.UnitRef) codeversion.NodeVersion {
	return codeversion.NodeVersion{
		UniqueID: uniqueID, ContentHash: hash, SourceHash: "s", SharedCodeHash: "m", ConfigHash: "c",
		Runtime: "dbt", RawCode: "select 1", CompiledCode: "select 1", ConfigJSON: `{"materialized":"table"}`,
		UnitRefs: refs,
	}
}

// promotedAtOf is the time promotion seq was promoted: later seqs are later.
func promotedAtOf(seq int64) time.Time {
	return time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Hour)
}

// batchOf is one batch of promotion seq of releaseID carrying nodes, with the
// artifact's entry of each: a dbt model the swap writes.
func batchOf(releaseID string, seq int64, nodes ...codeversion.NodeVersion) codeversion.WriteInput {
	topo := make(map[string]topology.ReleasePromotedTopologyNode, len(nodes))
	for _, n := range nodes {
		topo[n.UniqueID] = modelNode(n.UniqueID)
	}
	return codeversion.WriteInput{
		ReleaseID: releaseID, PromotionSeq: seq, Repo: "org/svc", CommitSHA: "sha-" + releaseID,
		PromotedAt: promotedAtOf(seq), Nodes: nodes, Topology: topo,
	}
}

// withUnits adds the shared-code versions the batch's nodes reference.
func withUnits(in codeversion.WriteInput, units ...codeversion.CodeUnitVersion) codeversion.WriteInput {
	in.Units = units
	return in
}

// ── the watermark ────────────────────────────────────────────────────────────

// :CURRENT moves when the node's watermark equals the promotion's seq: the
// first promotion of a node, and each newer one.
func TestWriteBatch_WatermarkEqualsSeqMovesCurrent(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.revenue")

	res := f.writeBatch(batchOf("r7", 7, versionOf("analytics.revenue", "sha256:v7")))
	assert.Equal(t, 1, res.NodeVersionsCreated)
	assert.Equal(t, 1, res.CurrentPointersMoved)
	assert.Equal(t, "sha256:v7", f.current("analytics.revenue"))
	assert.Equal(t, int64(7), f.node("analytics.revenue")["code_version_promotion_seq"])

	res = f.writeBatch(batchOf("r8", 8, versionOf("analytics.revenue", "sha256:v8")))
	assert.Equal(t, 1, res.CurrentPointersMoved)
	assert.Equal(t, "sha256:v8", f.current("analytics.revenue"))
	node := f.node("analytics.revenue")
	assert.Equal(t, int64(8), node["code_version_promotion_seq"])
	assert.NotContains(t, node, "code_version_promoted_at", "the watermark is the promotion seq, not a time")

	edge := f.rows(`MATCH (:Node {tenant_id: $tenant_id, unique_id: 'analytics.revenue'})-[c:CURRENT]->() RETURN properties(c) AS p`, nil)
	require.Len(t, edge, 1)
	p, _ := edge[0].Get("p")
	assert.Equal(t, "r8", p.(map[string]any)["release_id"])
	assert.True(t, promotedAtOf(8).Equal(p.(map[string]any)["promoted_at"].(time.Time)))
}

// A promotion older than the watermark records its version and leaves the
// pointer and the watermark alone (watermark 8, promotion 7).
func TestWriteBatch_OlderSeqLeavesCurrent(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.revenue")
	f.writeBatch(batchOf("r8", 8, versionOf("analytics.revenue", "sha256:new")))

	res := f.writeBatch(batchOf("r7", 7, versionOf("analytics.revenue", "sha256:old")))
	assert.Equal(t, 1, res.NodeVersionsCreated, "the older promotion's version is recorded")
	assert.Equal(t, 0, res.CurrentPointersMoved, "but it does not become current")
	assert.Equal(t, "sha256:new", f.current("analytics.revenue"))
	assert.Equal(t, int64(8), f.node("analytics.revenue")["code_version_promotion_seq"])
	assert.Equal(t, int64(2), f.versionCount("analytics.revenue"))
	assert.Zero(t, f.count(`MATCH (:NodeVersion {tenant_id: $tenant_id})-[p:PREVIOUS]->() RETURN count(p) AS n`, nil),
		"no :PREVIOUS chain is written")
}

// A promotion that finds the node's code already current writes no version
// but still advances the watermark, so an intermediate promotion arriving
// later cannot drag :CURRENT back onto code a newer promotion superseded.
func TestWriteBatch_MatchedNodeAdvancesWatermarkEvenWhenCodeUnchanged(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.revenue")
	f.writeBatch(batchOf("r1", 1, versionOf("analytics.revenue", "sha256:a")))

	res := f.writeBatch(batchOf("r3", 3, versionOf("analytics.revenue", "sha256:a")))
	assert.Zero(t, res.NodeVersionsCreated)
	assert.Equal(t, int64(3), f.node("analytics.revenue")["code_version_promotion_seq"], "the watermark advances on unchanged code")

	res = f.writeBatch(batchOf("r2", 2, versionOf("analytics.revenue", "sha256:b")))
	assert.Equal(t, 1, res.NodeVersionsCreated, "the intermediate version is recorded")
	assert.Zero(t, res.CurrentPointersMoved, "but it does not become current")
	assert.Equal(t, "sha256:a", f.current("analytics.revenue"))
}

// The shared-code unit has its own watermark, the promotion seq too: an older
// promotion records the unit's version and leaves its :CURRENT.
func TestWriteBatch_CodeUnitWatermarkUsesSeq(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.revenue")
	unit := func(checksum string) codeversion.CodeUnitVersion {
		return codeversion.CodeUnitVersion{UnitID: "svc:macro.cents", Checksum: checksum, Source: "macro " + checksum}
	}
	ref := func(checksum string) codeversion.UnitRef {
		return codeversion.UnitRef{UnitID: "svc:macro.cents", Checksum: checksum}
	}

	f.writeBatch(withUnits(batchOf("r5", 5, versionOf("analytics.revenue", "sha256:a5", ref("c5"))), unit("c5")))
	assert.Equal(t, "c5", f.unitCurrent("svc:macro.cents"))
	assert.Equal(t, int64(5), f.unitWatermark("svc:macro.cents"))

	res := f.writeBatch(withUnits(batchOf("r4", 4, versionOf("analytics.revenue", "sha256:a4", ref("c4"))), unit("c4")))
	assert.Equal(t, 1, res.UnitVersionsCreated, "the older promotion's unit version is recorded")
	assert.Equal(t, "c5", f.unitCurrent("svc:macro.cents"), "an older promotion leaves the unit's :CURRENT")
	assert.Equal(t, int64(5), f.unitWatermark("svc:macro.cents"))

	f.writeBatch(withUnits(batchOf("r6", 6, versionOf("analytics.revenue", "sha256:a6", ref("c6"))), unit("c6")))
	assert.Equal(t, "c6", f.unitCurrent("svc:macro.cents"))
	assert.Equal(t, int64(6), f.unitWatermark("svc:macro.cents"))
	assert.Equal(t, int64(1), f.count(`MATCH (cu:CodeUnit {tenant_id: $tenant_id}) RETURN count(cu) AS n`, nil), "one :CodeUnit per unit id")
}

// A promotion whose node is unchanged still advances the watermark of the
// shared code that node uses. r3 carries a and its macro as r1 did; r2,
// arriving late, changed the macro, and must not make its version current
// while r3's is the one running.
func TestWriteBatch_UnchangedNodeAdvancesItsUnitsWatermark(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.revenue")
	unit := func(checksum string) codeversion.CodeUnitVersion {
		return codeversion.CodeUnitVersion{UnitID: "svc:macro.cents", Checksum: checksum, Source: "macro " + checksum}
	}
	ref := func(checksum string) codeversion.UnitRef {
		return codeversion.UnitRef{UnitID: "svc:macro.cents", Checksum: checksum}
	}
	f.writeBatch(withUnits(batchOf("r1", 1, versionOf("analytics.revenue", "sha256:a1", ref("c1"))), unit("c1")))

	res := f.writeBatch(withUnits(batchOf("r3", 3, versionOf("analytics.revenue", "sha256:a1", ref("c1"))), unit("c1")))
	assert.Equal(t, codeversion.WriteResult{}, res, "unchanged code writes nothing")
	assert.Equal(t, int64(3), f.unitWatermark("svc:macro.cents"), "the unit's watermark advances with its unchanged node")

	f.writeBatch(withUnits(batchOf("r2", 2, versionOf("analytics.revenue", "sha256:a2", ref("c2"))), unit("c2")))
	assert.Equal(t, "c1", f.unitCurrent("svc:macro.cents"), "the late promotion does not take the unit's pointer")
	assert.Equal(t, "sha256:a1", f.current("analytics.revenue"))
	assert.Equal(t, int64(3), f.unitWatermark("svc:macro.cents"))
}

// The pointer guard is in the write statement, not in a read before it: an
// older promotion replayed again and again never takes the pointer.
func TestWriteBatch_PointerGuardIsEnforcedInTheWrite(t *testing.T) {
	f := newSwapFixture(t)
	f.writeBatch(batchOf("r2", 2, versionOf("analytics.revenue", "sha256:v2")))

	for i := 0; i < 3; i++ {
		res := f.writeBatch(batchOf("r1", 1, versionOf("analytics.revenue", "sha256:v1")))
		assert.Zero(t, res.CurrentPointersMoved, "attempt %d must not move the pointer", i)
	}
	assert.Equal(t, int64(1), f.count(`MATCH (:Node {tenant_id: $tenant_id, unique_id: 'analytics.revenue'})-[c:CURRENT]->() RETURN count(c) AS n`, nil))
	assert.Equal(t, "sha256:v2", f.current("analytics.revenue"))
}

// ── nodes the graph does not hold ────────────────────────────────────────────

// A bundle node the tenant's graph does not hold yet is created inactive from
// its artifact entry, with the release's id and no retired_at, before its
// version is written. A node the graph holds keeps the properties the swap
// gave it.
func TestWriteBatch_MissingNodeIsCreatedInactiveFromTheArtifact(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.table_a")
	in := batchOf("r8", 8, versionOf("analytics.table_a", "sha256:a8"), versionOf("analytics.table_e", "sha256:e8"))
	in.Topology["analytics.table_e"] = topology.ReleasePromotedTopologyNode{
		UniqueID: "analytics.table_e", SchemaName: "analytics", TableName: "table_e", ServiceName: "svc",
		NodeType: "dbt-model", ContentHash: "sha256:e8", TestCount: 2, ImageTag: "img-8", Schedule: "daily",
		UpstreamUniqueIDs: []string{"analytics.table_a"}, OriginalFilePath: "models/table_e.sql",
	}
	liveBefore := f.node("analytics.table_a")

	res := f.writeBatch(in)
	assert.Equal(t, 1, res.NodesCreated)
	assert.Equal(t, 2, res.CurrentPointersMoved)

	e := f.node("analytics.table_e")
	require.NotNil(t, e)
	for key, want := range map[string]any{
		"active": false, "release_id": "r8", "schema_name": "analytics", "table_name": "table_e",
		"service_name": "svc", "node_type": "dbt-model", "content_hash": "sha256:e8", "test_count": int64(2),
		"image_tag": "img-8", "schedule_name": "daily", "original_file_path": "models/table_e.sql",
		"code_version_promotion_seq": int64(8),
	} {
		assert.Equal(t, want, e[key], "table_e's %s", key)
	}
	assert.NotContains(t, e, "retired_at", "a node that was never live has no retired_at")
	assert.NotContains(t, e, "secret_ref")
	assert.Equal(t, "sha256:e8", f.current("analytics.table_e"))
	assert.Empty(t, f.edges(), "an inactive node gets no :DEPENDS_ON")

	a := f.node("analytics.table_a")
	for _, key := range []string{"active", "release_id", "schema_name", "content_hash"} {
		assert.Equal(t, liveBefore[key], a[key], "the live node keeps its %s", key)
	}

	again := f.writeBatch(in)
	assert.Zero(t, again.NodesCreated, "a replay creates no node")
	assert.Equal(t, []string{"analytics.table_a", "analytics.table_e"}, f.nodeIDs())
}

// A batch node with no entry in in.Topology creates no :Node and no version:
// in.Topology holds the nodes the release's topology swap writes, which leaves
// out a dbt-test.
func TestWriteBatch_BatchNodeWithoutTopologyEntryCreatesNoNode(t *testing.T) {
	f := newSwapFixture(t)
	in := batchOf("r1", 1, versionOf("analytics.orders", "sha256:o1"), versionOf("test.not_null_orders_id", "sha256:t1"))
	delete(in.Topology, "test.not_null_orders_id")

	res := f.writeBatch(in)
	assert.Equal(t, 1, res.NodesCreated)
	assert.Equal(t, 1, res.NodeVersionsCreated)
	assert.Equal(t, []string{"analytics.orders"}, f.nodeIDs(), "the node with no topology entry creates no :Node")
	assert.Zero(t, f.versionCount("test.not_null_orders_id"), "and no version")
}

// A bundle node the release's artifact does not carry is skipped: no node is
// created for it, and a node the graph already holds gets no version and no
// watermark from this release.
func TestWriteBatch_BundleNodeAbsentFromArtifactIsSkipped(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.held")
	in := batchOf("r1", 1, versionOf("analytics.orders", "sha256:o1"))
	in.Nodes = append(in.Nodes, versionOf("analytics.unknown", "sha256:u1"), versionOf("analytics.held", "sha256:h1"))

	res := f.writeBatch(in)
	assert.Equal(t, 1, res.NodeVersionsCreated)
	assert.Equal(t, []string{"analytics.held", "analytics.orders"}, f.nodeIDs(), "no node for the bundle node the artifact lacks")
	assert.Zero(t, f.versionCount("analytics.unknown"))
	assert.Zero(t, f.versionCount("analytics.held"))
	assert.NotContains(t, f.node("analytics.held"), "code_version_promotion_seq")
}

// ── idempotence and isolation ────────────────────────────────────────────────

// Every write MERGEs by identity, so a batch written again leaves the graph
// as it was: a replay after a crash finds what the first run wrote.
func TestWriteBatch_ReplayLeavesTheSameGraph(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.revenue")
	f.seedRejection("r0", "analytics.revenue", promotedAtOf(0))
	ref := codeversion.UnitRef{UnitID: "svc:macro.cents", Checksum: "c1"}
	in := withUnits(batchOf("r1", 1,
		versionOf("analytics.revenue", "sha256:r1", ref),
		versionOf("analytics.orders", "sha256:o1", ref)),
		codeversion.CodeUnitVersion{UnitID: "svc:macro.cents", Checksum: "c1", Source: "macro"})

	first := f.writeBatch(in)
	require.Equal(t, 1, first.RejectionsResolved)
	before := f.snapshot()

	again := f.writeBatch(in)
	assert.Equal(t, codeversion.WriteResult{}, again, "a replay writes nothing")
	assert.Equal(t, before, f.snapshot())
}

// A write of one tenant neither reads nor changes another tenant's nodes,
// even under the same unique_ids.
func TestWriteBatch_LeavesOtherTenantsAlone(t *testing.T) {
	a := newSwapFixture(t)
	b := a.another()
	b.writeBatch(batchOf("r1", 1, versionOf("analytics.revenue", "sha256:b1")))
	before := b.snapshot()

	a.writeBatch(batchOf("r1", 1, versionOf("analytics.revenue", "sha256:a1")))
	a.writeBatch(batchOf("r2", 2, versionOf("analytics.revenue", "sha256:a2")))

	assert.Equal(t, before, b.snapshot(), "tenant b's graph is untouched")
	assert.Equal(t, "sha256:a2", a.current("analytics.revenue"))
	assert.Equal(t, int64(2), a.versionCount("analytics.revenue"))
}

// ── the version ──────────────────────────────────────────────────────────────

func TestWriteBatch_FirstWriteRecordsTheVersion(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.revenue")

	res := f.writeBatch(batchOf("rel-1", 1, versionOf("analytics.revenue", "sha256:v1")))
	assert.Equal(t, codeversion.WriteResult{NodeVersionsCreated: 1, CurrentPointersMoved: 1}, res)

	recs := f.rows(`MATCH (:Node {tenant_id: $tenant_id, unique_id: 'analytics.revenue'})-[:CURRENT]->(v:NodeVersion {tenant_id: $tenant_id})
		RETURN properties(v) AS p`, nil)
	require.Len(t, recs, 1)
	p, _ := recs[0].Get("p")
	v := p.(map[string]any)
	for key, want := range map[string]any{
		"tenant_id": f.tenant, "unique_id": "analytics.revenue", "content_hash": "sha256:v1", "version_seq": int64(1),
		"release_id": "rel-1", "commit_sha": "sha-rel-1", "repo": "org/svc", "config_json": `{"materialized":"table"}`,
		"raw_code": "select 1", "compiled_code": "select 1", "runtime": "dbt", "healed": false, "backfilled": false,
	} {
		assert.Equal(t, want, v[key], "version %s", key)
	}
	assert.True(t, promotedAtOf(1).Equal(v["promoted_at"].(time.Time)))
}

// Versions grow with edits, not with promotions.
func TestWriteBatch_UnchangedHashWritesNothing(t *testing.T) {
	f := newSwapFixture(t)
	f.writeBatch(batchOf("rel-1", 1, versionOf("analytics.revenue", "sha256:v1")))

	res := f.writeBatch(batchOf("rel-2", 2, versionOf("analytics.revenue", "sha256:v1")))
	assert.Zero(t, res.NodeVersionsCreated)
	assert.Zero(t, res.CurrentPointersMoved)
	assert.Equal(t, int64(1), f.versionCount("analytics.revenue"))
}

// A revert points the node back at the version that already exists rather
// than duplicating it, and the version keeps its sequence number.
func TestWriteBatch_RevertReusesTheExistingVersion(t *testing.T) {
	f := newSwapFixture(t)
	for i, h := range []string{"sha256:v1", "sha256:v2", "sha256:v1"} {
		seq := int64(i + 1)
		f.writeBatch(batchOf("rel-"+strconv.FormatInt(seq, 10), seq, versionOf("analytics.revenue", h)))
	}

	assert.Equal(t, int64(2), f.versionCount("analytics.revenue"), "the reverted-to version is reused, not duplicated")
	assert.Equal(t, "sha256:v1", f.current("analytics.revenue"))
	seqs := f.column(`MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: 'analytics.revenue'})
		RETURN v.content_hash + '=' + toString(v.version_seq) AS v ORDER BY v`, nil)
	assert.Equal(t, []string{"sha256:v1=1", "sha256:v2=2"}, seqs)
}

func TestWriteBatch_WritesSharedCodeAndUsesCodeEdges(t *testing.T) {
	f := newSwapFixture(t)
	res := f.writeBatch(withUnits(batchOf("rel-1", 1, versionOf("analytics.revenue", "sha256:v1",
		codeversion.UnitRef{UnitID: "svc:m1", Checksum: "u1"},
		codeversion.UnitRef{UnitID: "svc:m2", Checksum: "u2"})),
		codeversion.CodeUnitVersion{UnitID: "svc:m1", Checksum: "u1", Source: "macro one"},
		codeversion.CodeUnitVersion{UnitID: "svc:m2", Checksum: "u2", Source: "macro two"}))
	assert.Equal(t, 2, res.UnitVersionsCreated)

	assert.Equal(t, int64(2), f.count(`
		MATCH (:Node {tenant_id: $tenant_id, unique_id: 'analytics.revenue'})-[:CURRENT]->(:NodeVersion {tenant_id: $tenant_id})
		      -[:USES_CODE]->(u:CodeUnitVersion {tenant_id: $tenant_id})
		RETURN count(u) AS n`, nil))
	assert.Equal(t, "u1", f.unitCurrent("svc:m1"))
	assert.Equal(t, []string{"macro one"}, f.column(`
		MATCH (:CodeUnit {tenant_id: $tenant_id, unit_id: 'svc:m1'})-[:CURRENT]->(v:CodeUnitVersion {tenant_id: $tenant_id})
		RETURN v.source AS v`, nil))
}

func TestWriteBatch_SharedCodeUnitChangeMovesItsCurrent(t *testing.T) {
	f := newSwapFixture(t)
	f.writeBatch(withUnits(batchOf("rel-1", 1, versionOf("analytics.revenue", "sha256:v1",
		codeversion.UnitRef{UnitID: "svc:m1", Checksum: "u1"})),
		codeversion.CodeUnitVersion{UnitID: "svc:m1", Checksum: "u1", Source: "one"}))
	f.writeBatch(withUnits(batchOf("rel-2", 2, versionOf("analytics.revenue", "sha256:v2",
		codeversion.UnitRef{UnitID: "svc:m1", Checksum: "u2"})),
		codeversion.CodeUnitVersion{UnitID: "svc:m1", Checksum: "u2", Source: "two"}))

	assert.Equal(t, "u2", f.unitCurrent("svc:m1"))
	assert.Equal(t, int64(2), f.count(`MATCH (v:CodeUnitVersion {tenant_id: $tenant_id, unit_id: 'svc:m1'}) RETURN count(v) AS n`, nil))
	assert.Equal(t, int64(1), f.count(`MATCH (c:CodeUnit {tenant_id: $tenant_id, unit_id: 'svc:m1'}) RETURN count(c) AS n`, nil),
		"one :CodeUnit per unit id")
}

// ── the case base ────────────────────────────────────────────────────────────

// A version that becomes current resolves every failure of its node recorded
// before the promotion and still open; a failure recorded after it stays
// open, and a resolved one keeps its fix.
func TestWriteBatch_LinksOpenRejectionsToTheNewVersion(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.revenue")
	f.seedRejection("rel-early", "analytics.revenue", promotedAtOf(0))
	f.seedRejection("rel-late", "analytics.revenue", promotedAtOf(2))
	resolvedBy := func(releaseID string) []string {
		return f.column(`MATCH (:Rejection {tenant_id: $tenant_id, release_id: $release_id, node_id: 'analytics.revenue'})
			-[:RESOLVED_BY]->(v:NodeVersion {tenant_id: $tenant_id}) RETURN v.content_hash AS v`, map[string]any{"release_id": releaseID})
	}

	res := f.writeBatch(batchOf("rel-1", 1, versionOf("analytics.revenue", "sha256:v1")))
	assert.Equal(t, 1, res.RejectionsResolved)
	assert.Equal(t, []string{"sha256:v1"}, resolvedBy("rel-early"))
	assert.Empty(t, resolvedBy("rel-late"), "a rejection dated after the promotion is not linked")

	again := f.writeBatch(batchOf("rel-1", 1, versionOf("analytics.revenue", "sha256:v1")))
	assert.Zero(t, again.RejectionsResolved, "a replay links nothing again")

	later := f.writeBatch(batchOf("rel-3", 3, versionOf("analytics.revenue", "sha256:v3")))
	assert.Equal(t, 1, later.RejectionsResolved, "only the still-open rejection is linked")
	assert.Equal(t, []string{"sha256:v1"}, resolvedBy("rel-early"), "a resolved rejection keeps its fix")
	assert.Equal(t, []string{"sha256:v3"}, resolvedBy("rel-late"))
}

// The open-rejection guard looks at [:RESOLVED_BY] edges to a :NodeVersion
// only: a merged fix PR's [:RESOLVED_BY] to a :Proposal does not stop the
// promotion's own link.
func TestWriteBatch_ForwardLinkGuardIsPerNodeVersionFamily(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.revenue")
	f.seedRejection("rel-proposal", "analytics.revenue", promotedAtOf(0))
	f.exec(`MATCH (rej:Rejection {tenant_id: $tenant_id, release_id: 'rel-proposal'})
		MERGE (p:Proposal {tenant_id: $tenant_id, proposal_id: 'prop-1'})
		MERGE (rej)-[:RESOLVED_BY]->(p)`, nil)

	res := f.writeBatch(batchOf("rel-1", 1, versionOf("analytics.revenue", "sha256:v1")))
	assert.Equal(t, 1, res.RejectionsResolved)
	assert.Equal(t, int64(2), f.count(`
		MATCH (:Rejection {tenant_id: $tenant_id, release_id: 'rel-proposal'})-[r:RESOLVED_BY]->() RETURN count(r) AS n`, nil),
		"the rejection carries the proposal's edge and the version's")
}

// A promotion older than the node's watermark is not the code the node runs,
// so it resolves nothing, even when it is later than the rejection.
func TestWriteBatch_StaleRedeliveryCannotLinkAnOpenRejection(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.watermark")
	f.writeBatch(batchOf("rel-base", 1, versionOf("analytics.watermark", "sha256:base")))
	f.seedRejection("rel-target", "analytics.watermark", promotedAtOf(1).Add(30*time.Minute))
	f.writeBatch(batchOf("rel-samehash", 3, versionOf("analytics.watermark", "sha256:base")))

	res := f.writeBatch(batchOf("rel-stale", 2, versionOf("analytics.watermark", "sha256:reverted")))
	assert.Zero(t, res.CurrentPointersMoved)
	assert.Zero(t, res.RejectionsResolved)
	assert.Zero(t, f.count(`MATCH (:Rejection {tenant_id: $tenant_id, release_id: 'rel-target'})-[r:RESOLVED_BY]->() RETURN count(r) AS n`, nil))
}

// The [:RESOLVED_BY] edge carries the resolving promotion's time and release:
// the precedent detail diffs from it, because a reverted-to version keeps its
// own earlier promoted_at.
func TestWriteBatch_StampsResolvedByEdgeWithPromotionMetadata(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.revenue")
	f.seedRejection("rel-early", "analytics.revenue", promotedAtOf(0))

	require.Equal(t, 1, f.writeBatch(batchOf("rel-1", 1, versionOf("analytics.revenue", "sha256:v1"))).RejectionsResolved)

	recs := f.rows(`MATCH (:Rejection {tenant_id: $tenant_id, release_id: 'rel-early'})-[rb:RESOLVED_BY]->() RETURN properties(rb) AS p`, nil)
	require.Len(t, recs, 1)
	p, _ := recs[0].Get("p")
	edge := p.(map[string]any)
	assert.Equal(t, "rel-1", edge["release_id"])
	at, ok := edge["promoted_at"].(time.Time)
	require.True(t, ok, "promoted_at is a datetime, got %T", edge["promoted_at"])
	assert.True(t, promotedAtOf(1).Equal(at))
}
