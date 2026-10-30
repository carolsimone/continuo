//go:build integration

package neo4jinfra

import (
	"context"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oldSchemaStatements are the constraints and indexes of the graph before it
// had tenants: no tenant_id anywhere, constraints on :Table and :Run, and the
// current names over their old properties. node_unique reuses a current name
// for a definition without tenant_id, which CREATE ... IF NOT EXISTS alone
// would keep.
var oldSchemaStatements = []string{
	"CREATE CONSTRAINT run_id_unique FOR (r:Run) REQUIRE r.run_id IS UNIQUE",
	"CREATE CONSTRAINT table_uid_unique FOR (t:Table) REQUIRE t.unique_id IS UNIQUE",
	"CREATE CONSTRAINT meta_key_unique FOR (m:Meta) REQUIRE m.key IS UNIQUE",
	"CREATE INDEX table_fqn FOR (t:Table) ON (t.service_name, t.schema_name, t.table_name)",
	"CREATE INDEX table_schedule FOR (t:Table) ON (t.schedule_name)",
	"CREATE INDEX run_schedule FOR (r:Run) ON (r.schedule_name)",
	"CREATE CONSTRAINT node_version_unique FOR (v:NodeVersion) REQUIRE (v.unique_id, v.content_hash) IS UNIQUE",
	"CREATE INDEX node_version_uid FOR (v:NodeVersion) ON (v.unique_id)",
	"CREATE CONSTRAINT code_unit_unique FOR (c:CodeUnit) REQUIRE c.unit_id IS UNIQUE",
	"CREATE CONSTRAINT code_unit_version_unique FOR (v:CodeUnitVersion) REQUIRE (v.unit_id, v.checksum) IS UNIQUE",
	"CREATE INDEX code_unit_version_unit_id FOR (v:CodeUnitVersion) ON (v.unit_id)",
	"CREATE CONSTRAINT rejection_unique FOR (r:Rejection) REQUIRE (r.release_id, r.node_id) IS UNIQUE",
	"CREATE INDEX rejection_node_id FOR (r:Rejection) ON (r.node_id)",
	"CREATE INDEX rejection_category_reason FOR (r:Rejection) ON (r.category, r.reason)",
	"CREATE CONSTRAINT error_signature_unique FOR (s:ErrorSignature) REQUIRE s.signature IS UNIQUE",
	"CREATE INDEX error_signature_category_reason FOR (s:ErrorSignature) ON (s.category, s.reason)",
	"CREATE CONSTRAINT proposal_unique FOR (p:Proposal) REQUIRE p.proposal_id IS UNIQUE",
	"CREATE CONSTRAINT pull_request_unique FOR (p:PullRequest) REQUIRE (p.proposal_id, p.service) IS UNIQUE",
	"CREATE CONSTRAINT node_unique FOR (n:Node) REQUIRE n.unique_id IS UNIQUE",
}

// bulkTables is how many plain :Table nodes the old shape adds beside the
// e2e DAG's, more than one batch of the migration's batched statements.
const bulkTables = 1001

// seedOldShapeCypher writes the graph as it was before tenants: :Table nodes
// without a tenant (table_d retired, with its failure, its fix PR's edit, its
// last version and a dependency), run history on :Run, the :TopologyRoot,
// :Meta at promotion_seq 7, versions on the code_version_promoted_at
// watermark, and the case base. Every node carries the test's marker.
const seedOldShapeCypher = `
CREATE (a:Table {unique_id: $m + '.table_a', schema_name: 'analytics', table_name: 'table_a', service_name: 'svc',
                 schedule_name: 'daily', active: true, release_id: 'r7',
                 code_version_promoted_at: datetime('2026-10-20T09:00:00Z'), test_marker: $m})
CREATE (b:Table {unique_id: $m + '.table_b', schema_name: 'analytics', table_name: 'table_b', service_name: 'svc',
                 schedule_name: 'daily', active: true, release_id: 'r7', test_marker: $m})
CREATE (d:Table {unique_id: $m + '.table_d', schema_name: 'analytics', table_name: 'table_d', service_name: 'svc',
                 schedule_name: 'daily', active: false, retired_at: datetime('2026-10-25T10:00:00Z'), release_id: 'r6',
                 code_version_promoted_at: datetime('2026-10-18T09:00:00Z'), test_marker: $m})
CREATE (b)-[:DEPENDS_ON]->(a)
CREATE (d)-[:DEPENDS_ON]->(a)
CREATE (:Run {run_id: $m + '-run-1', schedule_name: 'daily', terminal_status: 'succeeded', test_marker: $m})-[:EXECUTES]->(a)
CREATE (:Run {run_id: $m + '-run-2', schedule_name: 'daily', terminal_status: 'failed', test_marker: $m})-[:EXECUTES]->(d)
CREATE (:TopologyRoot {id: 'singleton', promotion_seq: 7, test_marker: $m})
CREATE (:Meta {key: 'current_release', release_id: 'r7', promotion_seq: 7, test_marker: $m})
CREATE (va:NodeVersion {unique_id: a.unique_id, content_hash: '9f2c', version_seq: 3,
                        promoted_at: datetime('2026-10-20T09:00:00Z'), test_marker: $m})
CREATE (vd:NodeVersion {unique_id: d.unique_id, content_hash: 'd4e1', version_seq: 1,
                        promoted_at: datetime('2026-10-18T09:00:00Z'), test_marker: $m})
CREATE (a)-[:CURRENT {promoted_at: datetime('2026-10-20T09:00:00Z'), release_id: 'r7'}]->(va)
CREATE (d)-[:CURRENT {promoted_at: datetime('2026-10-18T09:00:00Z'), release_id: 'r6'}]->(vd)
CREATE (cu:CodeUnit {unit_id: $m + '.svc.macro.cents', code_version_promoted_at: datetime('2026-10-20T09:00:00Z'), test_marker: $m})
CREATE (cuv:CodeUnitVersion {unit_id: cu.unit_id, checksum: 'c0de', test_marker: $m})
CREATE (cu)-[:CURRENT {promoted_at: datetime('2026-10-20T09:00:00Z'), release_id: 'r7'}]->(cuv)
CREATE (va)-[:USES_CODE]->(cuv)
CREATE (rej:Rejection {release_id: 'r6', node_id: d.unique_id, category: 'compile', reason: 'syntax',
                       at: datetime('2026-10-17T09:00:00Z'), test_marker: $m})
CREATE (sig:ErrorSignature {signature: $m + '-signature', category: 'compile', reason: 'syntax', test_marker: $m})
CREATE (rej)-[:HAS_SIGNATURE]->(sig)
CREATE (d)-[:FAILED {release_id: 'r6'}]->(rej)
CREATE (prop:Proposal {proposal_id: $m + '-proposal', test_marker: $m})
CREATE (rej)-[:PROPOSED {service: 'svc'}]->(prop)
CREATE (pr:PullRequest {proposal_id: prop.proposal_id, service: 'svc', state: 'merged', test_marker: $m})
CREATE (prop)-[:HAS_PR]->(pr)
CREATE (prop)-[:EDITED {path: 'models/table_d.sql'}]->(d)
CREATE (rej)-[:RESOLVED_BY]->(vd)
WITH 1 AS seeded
UNWIND range(1, $bulk) AS i
CREATE (:Table {unique_id: $m + '.bulk.' + toString(i), schema_name: 'bulk', table_name: 'bulk_' + toString(i),
                service_name: 'svc', active: true, release_id: 'r7', test_marker: $m})`

// oldShapeFixture seeds the old shape under its test's marker on a graph
// whose constraints and indexes it reset to the old ones. Its cleanup deletes
// the test's nodes and then runs InitSchema, which brings the schema back to
// the catalog's whatever state the test left it in.
type oldShapeFixture struct {
	t       *testing.T
	client  Neo4jClient
	marker  string
	lookups []string
}

func newOldShapeFixture(t *testing.T) *oldShapeFixture {
	t.Helper()
	f := &oldShapeFixture{t: t, client: integrationClient(t), marker: t.Name()}
	f.lookups = readGraphSchema(t, f.client).lookups
	t.Cleanup(func() {
		if err := InitSchema(context.Background(), f.client, quietLogger()); err != nil {
			t.Errorf("cleanup: restore the schema: %v", err)
		}
	})
	t.Cleanup(f.deleteNodes)
	f.deleteNodes()

	f.dropSchema()
	f.exec(oldSchemaStatements...)
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = s.Close(ctx) }()
	res, err := s.Run(ctx, seedOldShapeCypher, map[string]any{"m": f.marker, "bulk": bulkTables})
	if err == nil {
		_, err = res.Consume(ctx)
	}
	require.NoError(t, err, "seed the old shape (a node another test left behind can break an old constraint)")
	return f
}

func (f *oldShapeFixture) deleteNodes() {
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = s.Close(ctx) }()
	res, err := s.Run(ctx, `MATCH (n) WHERE n.test_marker = $m DETACH DELETE n`, map[string]any{"m": f.marker})
	if err == nil {
		_, err = res.Consume(ctx)
	}
	if err != nil {
		f.t.Errorf("delete the test's nodes: %v", err)
	}
}

// dropSchema drops every constraint and every index but the LOOKUP ones.
func (f *oldShapeFixture) dropSchema() {
	state := readGraphSchema(f.t, f.client)
	require.Empty(f.t, state.others, "the test graph has only RANGE and LOOKUP indexes to reset")
	var stmts []string
	for name := range state.constraints {
		stmts = append(stmts, "DROP CONSTRAINT `"+name+"`")
	}
	for name, owner := range state.rangeOwners {
		if owner == "" {
			stmts = append(stmts, "DROP INDEX `"+name+"`")
		}
	}
	f.exec(stmts...)
}

// exec runs each statement in its own auto-commit transaction.
func (f *oldShapeFixture) exec(stmts ...string) {
	f.t.Helper()
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = s.Close(ctx) }()
	for _, stmt := range stmts {
		res, err := s.Run(ctx, stmt, nil)
		if err == nil {
			_, err = res.Consume(ctx)
		}
		require.NoError(f.t, err, stmt)
	}
}

func (f *oldShapeFixture) rows(cypher string) []map[string]any {
	f.t.Helper()
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = s.Close(ctx) }()
	res, err := s.Run(ctx, cypher, map[string]any{"m": f.marker})
	require.NoError(f.t, err, cypher)
	recs, err := res.Collect(ctx)
	require.NoError(f.t, err, cypher)
	out := make([]map[string]any, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.AsMap())
	}
	return out
}

func (f *oldShapeFixture) one(cypher string) map[string]any {
	f.t.Helper()
	rows := f.rows(cypher)
	require.Len(f.t, rows, 1, cypher)
	return rows[0]
}

// counts reads a two-column result, key and n, as a map.
func (f *oldShapeFixture) counts(cypher string) map[string]int64 {
	f.t.Helper()
	out := map[string]int64{}
	for _, row := range f.rows(cypher) {
		out[row["key"].(string)] = row["n"].(int64)
	}
	return out
}

func (f *oldShapeFixture) labelCounts() map[string]int64 {
	return f.counts(`MATCH (n) WHERE n.test_marker = $m UNWIND labels(n) AS key RETURN key, count(*) AS n`)
}

func (f *oldShapeFixture) relationshipCounts() map[string]int64 {
	return f.counts(`MATCH (s)-[r]->() WHERE s.test_marker = $m RETURN type(r) AS key, count(r) AS n`)
}

// assertMigrated checks the seeded graph has the shape the migration leaves:
// every node in the default tenant, :Table relabelled :Node, no run history
// or :TopologyRoot anywhere, the watermark on the live promotion_seq, and the
// retired node kept with its history but without its dependencies.
func (f *oldShapeFixture) assertMigrated(when string) {
	t := f.t
	t.Helper()

	assert.Equal(t, map[string]int64{
		"Node": 3 + bulkTables, "Meta": 1, "NodeVersion": 2, "CodeUnit": 1, "CodeUnitVersion": 1,
		"Rejection": 1, "ErrorSignature": 1, "Proposal": 1, "PullRequest": 1,
	}, f.labelCounts(), "%s: the test's nodes per label", when)
	for _, label := range []string{"Table", "Run", "TopologyRoot"} {
		row := f.one(fmt.Sprintf("MATCH (n:%s) RETURN count(n) AS n", label))
		assert.Zero(t, row["n"], "%s: the graph holds no :%s", when, label)
	}
	assert.Equal(t, map[string]int64{
		"CURRENT": 3, "DEPENDS_ON": 1, "USES_CODE": 1, "HAS_SIGNATURE": 1, "FAILED": 1,
		"PROPOSED": 1, "HAS_PR": 1, "EDITED": 1, "RESOLVED_BY": 1,
	}, f.relationshipCounts(), "%s: the test's relationships per type", when)

	tenants := f.one(`MATCH (n) WHERE n.test_marker = $m
		RETURN count(n) AS total, count(CASE WHEN n.tenant_id = 'default' THEN 1 END) AS in_default`)
	assert.Equal(t, tenants["total"], tenants["in_default"], "%s: every node is in the default tenant", when)

	watermarks := f.one(`MATCH (n) WHERE n.test_marker = $m AND (n:Node OR n:CodeUnit)
		RETURN count(n) AS total,
		       count(CASE WHEN n.code_version_promotion_seq = 7 THEN 1 END) AS on_live_seq,
		       count(n.code_version_promoted_at) AS with_promoted_at`)
	assert.Equal(t, int64(3+bulkTables+1), watermarks["total"], when)
	assert.Equal(t, watermarks["total"], watermarks["on_live_seq"],
		"%s: every :Node and :CodeUnit has the live promotion_seq as its watermark", when)
	assert.Zero(t, watermarks["with_promoted_at"], "%s: no code_version_promoted_at is left", when)

	meta := f.one(`MATCH (m:Meta) WHERE m.test_marker = $m RETURN properties(m) AS p`)["p"].(map[string]any)
	assert.Equal(t, "default", meta["tenant_id"], when)
	assert.Equal(t, "current_release", meta["key"], when)
	assert.Equal(t, "r7", meta["release_id"], when)
	assert.Equal(t, int64(7), meta["promotion_seq"], when)

	a := f.one(`MATCH (n:Node {unique_id: $m + '.table_a'}) RETURN properties(n) AS p`)["p"].(map[string]any)
	for key, want := range map[string]any{
		"tenant_id": "default", "schema_name": "analytics", "table_name": "table_a", "service_name": "svc",
		"schedule_name": "daily", "active": true, "release_id": "r7", "code_version_promotion_seq": int64(7),
	} {
		assert.Equal(t, want, a[key], "%s: table_a keeps %s", when, key)
	}

	retired := f.one(`MATCH (d:Node {unique_id: $m + '.table_d'})
		RETURN d.active AS active, d.retired_at IS NOT NULL AS retired,
		       COUNT { (d)-[:CURRENT]->(:NodeVersion) } AS current,
		       COUNT { (d)-[:FAILED]->(:Rejection) } AS failed,
		       COUNT { (:Proposal)-[:EDITED]->(d) } AS edited,
		       COUNT { (d)-[:DEPENDS_ON]->() } AS depends_on`)
	assert.Equal(t, false, retired["active"], when)
	assert.Equal(t, true, retired["retired"], when)
	assert.Equal(t, int64(1), retired["current"], "%s: the retired node keeps :CURRENT", when)
	assert.Equal(t, int64(1), retired["failed"], "%s: the retired node keeps :FAILED", when)
	assert.Equal(t, int64(1), retired["edited"], "%s: the retired node keeps :EDITED", when)
	assert.Equal(t, int64(0), retired["depends_on"], "%s: the retired node has no outgoing :DEPENDS_ON", when)

	live := f.one(`MATCH (b:Node {unique_id: $m + '.table_b'})
		RETURN COUNT { (b)-[:DEPENDS_ON]->(:Node {unique_id: $m + '.table_a'}) } AS depends_on`)
	assert.Equal(t, int64(1), live["depends_on"], "%s: a live node keeps its :DEPENDS_ON", when)
}

// TestMigrate_OldShape runs InitSchema twice on the graph as it was before
// tenants. The first run migrates it; the second finds nothing to change.
// After each, the data has the migrated shape and the constraints and
// indexes are exactly the catalog's.
func TestMigrate_OldShape(t *testing.T) {
	ctx := context.Background()
	f := newOldShapeFixture(t)

	for run := 1; run <= 2; run++ {
		require.NoError(t, InitSchema(ctx, f.client, quietLogger()), "run %d", run)
		f.assertMigrated(fmt.Sprintf("after run %d", run))
		assertTargetSchema(t, f.client, f.lookups)
	}
}

// TestMigrate_ResumesAfterPartialRun stops a migration after its third step,
// as a crash would: run history deleted, :Table relabelled, every node in the
// default tenant, the watermarks and the old constraints still as they were.
// The next InitSchema resumes and leaves the same graph as a run that never
// stopped.
func TestMigrate_ResumesAfterPartialRun(t *testing.T) {
	ctx := context.Background()
	f := newOldShapeFixture(t)
	require.Greater(t, bulkTables, migrationBatchRows, "the seed spans more than one batch")

	require.NoError(t, runMigration(ctx, f.client, quietLogger(), startupMigration[:3]))

	assert.Equal(t, map[string]int64{
		"Node": 3 + bulkTables, "Meta": 1, "NodeVersion": 2, "CodeUnit": 1, "CodeUnitVersion": 1,
		"Rejection": 1, "ErrorSignature": 1, "Proposal": 1, "PullRequest": 1,
	}, f.labelCounts(), "the partial run relabelled :Table and deleted the run history")
	tenants := f.one(`MATCH (n) WHERE n.test_marker = $m
		RETURN count(n) AS total, count(CASE WHEN n.tenant_id = 'default' THEN 1 END) AS in_default`)
	assert.Equal(t, tenants["total"], tenants["in_default"], "the partial run set the tenant")
	watermarks := f.one(`MATCH (n) WHERE n.test_marker = $m AND (n:Node OR n:CodeUnit)
		RETURN count(n.code_version_promoted_at) AS with_promoted_at, count(n.code_version_promotion_seq) AS with_seq`)
	assert.Equal(t, int64(3), watermarks["with_promoted_at"], "the partial run stopped before the watermark step")
	assert.Zero(t, watermarks["with_seq"])
	partial := readGraphSchema(t, f.client)
	assert.Equal(t, schemaEntry{"Table", []string{"unique_id"}}, partial.constraints["table_uid_unique"], "the old constraints are still in place")
	assert.Equal(t, schemaEntry{"Node", []string{"unique_id"}}, partial.constraints["node_unique"])

	require.NoError(t, InitSchema(ctx, f.client, quietLogger()))
	f.assertMigrated("after the resumed run")
	assertTargetSchema(t, f.client, f.lookups)
}

// TestMigrate_KeepsAnExistingWatermark runs InitSchema twice on a tenant
// whose :Node and :CodeUnit watermarks are already promotion seqs above the
// live one, alone or beside a stale code_version_promoted_at. Every start
// runs the watermark step, so it must keep each seq a node has: only a node
// with none takes the live promotion_seq, and code_version_promoted_at goes.
func TestMigrate_KeepsAnExistingWatermark(t *testing.T) {
	ctx := context.Background()
	f := newSwapFixture(t)
	f.exec(`
		CREATE (:Meta {tenant_id: $tenant_id, key: 'current_release', release_id: 'r7', promotion_seq: 7})
		CREATE (:Node {tenant_id: $tenant_id, unique_id: 'seq_alone', code_version_promotion_seq: 9})
		CREATE (:Node {tenant_id: $tenant_id, unique_id: 'seq_and_time', code_version_promotion_seq: 8,
		               code_version_promoted_at: datetime('2026-10-20T09:00:00Z')})
		CREATE (:Node {tenant_id: $tenant_id, unique_id: 'time_only', code_version_promoted_at: datetime('2026-10-20T09:00:00Z')})
		CREATE (:Node {tenant_id: $tenant_id, unique_id: 'none'})
		CREATE (:CodeUnit {tenant_id: $tenant_id, unit_id: 'seq_alone', code_version_promotion_seq: 9})
		CREATE (:CodeUnit {tenant_id: $tenant_id, unit_id: 'seq_and_time', code_version_promotion_seq: 8,
		                   code_version_promoted_at: datetime('2026-10-20T09:00:00Z')})
		CREATE (:CodeUnit {tenant_id: $tenant_id, unit_id: 'none'})`, nil)
	want := map[string]int64{"seq_alone": 9, "seq_and_time": 8, "time_only": 7, "none": 7}

	for run := 1; run <= 2; run++ {
		require.NoError(t, InitSchema(ctx, f.client, quietLogger()), "run %d", run)
		for _, label := range []string{"Node", "CodeUnit"} {
			key := map[string]string{"Node": "unique_id", "CodeUnit": "unit_id"}[label]
			got := map[string]int64{}
			for _, rec := range f.rows(fmt.Sprintf(`MATCH (n:%s {tenant_id: $tenant_id})
				RETURN n.%s AS id, n.code_version_promotion_seq AS seq, n.code_version_promoted_at IS NOT NULL AS has_time`, label, key), nil) {
				id, _ := rec.Get("id")
				seq, _ := rec.Get("seq")
				hasTime, _ := rec.Get("has_time")
				assert.Equal(t, false, hasTime, "run %d: :%s %v keeps no code_version_promoted_at", run, label, id)
				got[id.(string)] = seq.(int64)
			}
			wantLabel := map[string]int64{}
			for id, seq := range want {
				if label == "CodeUnit" && id == "time_only" {
					continue
				}
				wantLabel[id] = seq
			}
			assert.Equal(t, wantLabel, got, "run %d: the :%s watermarks", run, label)
		}
	}
}
