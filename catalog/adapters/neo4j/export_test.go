//go:build integration

package neo4jinfra

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/domain/graphschema"
	"github.com/carolsimone/continuo/catalog/serialization/exportfile"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// graphFixture gives a test tenants of its own and deletes every node of
// those tenants when the test ends.
type graphFixture struct {
	t       *testing.T
	client  Neo4jClient
	tenants []string
}

func newGraphFixture(t *testing.T) *graphFixture {
	t.Helper()
	f := &graphFixture{t: t, client: integrationClient(t)}
	t.Cleanup(func() {
		for _, tenantID := range f.tenants {
			f.exec(tenantID, `MATCH (n {tenant_id: $tenant_id}) DETACH DELETE n`, nil)
		}
	})
	return f
}

// tenant returns a fresh tenant id the fixture cleans up.
func (f *graphFixture) tenant(name string) string {
	id := "b2-" + name + "-" + uuid.NewString()
	f.tenants = append(f.tenants, id)
	return id
}

// exec runs cypher in an auto-commit transaction with $tenant_id bound.
func (f *graphFixture) exec(tenantID, cypher string, params map[string]any) {
	f.t.Helper()
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = s.Close(ctx) }()
	bound := map[string]any{"tenant_id": tenantID}
	maps.Copy(bound, params)
	res, err := s.Run(ctx, cypher, bound)
	if err == nil {
		_, err = res.Consume(ctx)
	}
	require.NoError(f.t, err, cypher)
}

// query runs cypher in a read transaction with $tenant_id bound and returns
// every record.
func (f *graphFixture) query(tenantID, cypher string, params map[string]any) []*neo4j.Record {
	f.t.Helper()
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = s.Close(ctx) }()
	bound := map[string]any{"tenant_id": tenantID}
	maps.Copy(bound, params)
	res, err := s.Run(ctx, cypher, bound)
	require.NoError(f.t, err, cypher)
	recs, err := res.Collect(ctx)
	require.NoError(f.t, err, cypher)
	return recs
}

func (f *graphFixture) exporter() ports.GraphExporter {
	return NewGraphExporter(f.client, "b2-test", quietLogger())
}

func (f *graphFixture) importer() ports.GraphImporter {
	return NewGraphImporter(f.client, quietLogger())
}

// export exports tenantID's graph into an export file in memory.
func (f *graphFixture) export(tenantID string) []byte {
	f.t.Helper()
	var buf bytes.Buffer
	require.NoError(f.t, f.exporter().Export(context.Background(), tenantID, exportfile.NewWriter(&buf)))
	return buf.Bytes()
}

// restore does what a restore does with data: validate it, wipe the tenant,
// import it.
func (f *graphFixture) restore(tenantID string, data []byte) {
	f.t.Helper()
	ctx := context.Background()
	_, err := exportfile.Validate(bytes.NewReader(data), tenantID)
	require.NoError(f.t, err, "Validate")
	require.NoError(f.t, f.importer().Wipe(ctx, tenantID), "Wipe")
	r, err := exportfile.NewReader(bytes.NewReader(data))
	require.NoError(f.t, err, "NewReader")
	require.NoError(f.t, f.importer().Import(ctx, tenantID, r), "Import")
}

// records decodes every record of an export file.
func records(t *testing.T, data []byte) (export.Header, []export.NodeRecord, []export.RelRecord) {
	t.Helper()
	r, err := exportfile.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	var nodes []export.NodeRecord
	var rels []export.RelRecord
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		switch x := rec.(type) {
		case export.NodeRecord:
			nodes = append(nodes, x)
		case export.RelRecord:
			rels = append(rels, x)
		}
	}
	return r.Header(), nodes, rels
}

// dump renders every node of the tenant but its lease node, and every
// relationship that touches a node of the tenant, as sorted canonical text:
// a node by its labels and typed properties, a relationship by its type, its
// typed properties and both ends in full. Two graphs are identical when
// their dumps are equal.
func (f *graphFixture) dump(tenantID string) (nodes, rels []string) {
	f.t.Helper()
	for _, rec := range f.query(tenantID, `
MATCH (n {tenant_id: $tenant_id})
WHERE NOT (n:Meta AND coalesce(n.key, '') = 'export')
RETURN labels(n) AS labels, properties(n) AS props`, nil) {
		nodes = append(nodes, renderNode(rec.Values[0], rec.Values[1]))
	}
	for _, rec := range f.query(tenantID, `
MATCH (a)-[r]->(b)
WHERE a.tenant_id = $tenant_id OR b.tenant_id = $tenant_id
RETURN type(r), properties(r), labels(a), properties(a), labels(b), properties(b)`, nil) {
		rels = append(rels, renderNode(rec.Values[2], rec.Values[3])+
			"-["+rec.Values[0].(string)+" "+renderProps(rec.Values[1].(map[string]any))+"]->"+
			renderNode(rec.Values[4], rec.Values[5]))
	}
	sort.Strings(nodes)
	sort.Strings(rels)
	return nodes, rels
}

// leaseNode returns the properties of the tenant's lease nodes.
func (f *graphFixture) leaseNodes(tenantID string) []map[string]any {
	var out []map[string]any
	for _, rec := range f.query(tenantID, `MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'}) RETURN properties(m)`, nil) {
		out = append(out, rec.Values[0].(map[string]any))
	}
	return out
}

func renderNode(labels, props any) string {
	ls := make([]string, 0)
	for _, l := range labels.([]any) {
		ls = append(ls, l.(string))
	}
	sort.Strings(ls)
	return "(" + strings.Join(ls, ":") + " " + renderProps(props.(map[string]any)) + ")"
}

func renderProps(props map[string]any) string {
	keys := slices.Sorted(maps.Keys(props))
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, strconv.Quote(k)+": "+renderValue(props[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// renderValue writes a property value with its type: a float by its bits (a
// NaN as NaN), a datetime by its instant, its location name and its offset.
func renderValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return "s" + strconv.Quote(x)
	case bool:
		return "b:" + strconv.FormatBool(x)
	case int64:
		return "i:" + strconv.FormatInt(x, 10)
	case float64:
		if math.IsNaN(x) {
			return "f:NaN"
		}
		return fmt.Sprintf("f:%016x", math.Float64bits(x))
	case time.Time:
		_, off := x.Zone()
		return fmt.Sprintf("t:%d@%s%+d", x.UnixNano(), x.Location().String(), off)
	case []any:
		parts := make([]string, 0, len(x))
		for _, e := range x {
			parts = append(parts, renderValue(e))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	default:
		return fmt.Sprintf("?%T:%v", x, x)
	}
}

// bulkNodes is how many extra :Node nodes, chained by :DEPENDS_ON, the full
// graph holds, so the import crosses its batch size for nodes and for
// relationships.
const bulkNodes = 1201

// seedEveryLabel writes a graph that holds every label and relationship type
// of the schema: two versions of one node and versions of two nodes that
// share a content hash (composite identities), two pull requests of one
// proposal, a retired node, a node with two labels, a self-loop, two
// identical relationships between one pair and two that differ only in their
// properties, relationship properties, datetimes stored with an offset
// (datetime()) and with a zone name (a Go time in UTC), lists, a long string
// and the bulk chain. Every property value carries mark, so two tenants'
// graphs differ. The graph is in the shape the startup migration leaves:
// every :Node and :CodeUnit has its version watermark, and no retired node
// has an outgoing :DEPENDS_ON.
func (f *graphFixture) seedEveryLabel(tenantID, mark string) {
	f.t.Helper()
	utc := time.Date(2026, 10, 20, 9, 0, 0, 123456789, time.UTC)
	f.exec(tenantID, `
CREATE (a:Node {tenant_id: $tenant_id, unique_id: 'analytics.table_a', service_name: 'svc', schema_name: 'analytics',
  table_name: 'table_a', node_type: 'model', active: true, release_id: 'r7', code_version_promotion_seq: 7,
  tags: ['nightly', $mark], test_count: 2, mark: $mark})
CREATE (b:Node {tenant_id: $tenant_id, unique_id: 'analytics.table_b', active: true, release_id: 'r7', code_version_promotion_seq: 7, mark: $mark})
CREATE (d:Node {tenant_id: $tenant_id, unique_id: 'analytics.table_d', active: false, retired_at: $utc, release_id: 'r6',
  code_version_promotion_seq: 6, mark: $mark})
CREATE (x:Node:CodeUnit {tenant_id: $tenant_id, unique_id: 'analytics.multi', unit_id: 'units/multi.sql', active: true,
  code_version_promotion_seq: 7, mark: $mark})
CREATE (m:Meta {tenant_id: $tenant_id, key: 'current_release', release_id: 'r7', promotion_seq: 7, mark: $mark})
CREATE (va1:NodeVersion {tenant_id: $tenant_id, unique_id: 'analytics.table_a', content_hash: 'h1', version_seq: 1,
  promoted_at: $utc, raw_code: $long, mark: $mark})
CREATE (va2:NodeVersion {tenant_id: $tenant_id, unique_id: 'analytics.table_a', content_hash: 'h2', version_seq: 2,
  promoted_at: $utc, mark: $mark})
CREATE (vd:NodeVersion {tenant_id: $tenant_id, unique_id: 'analytics.table_d', content_hash: 'h1', version_seq: 1,
  promoted_at: $utc, mark: $mark})
CREATE (cu:CodeUnit {tenant_id: $tenant_id, unit_id: 'macros/m.sql', code_version_promotion_seq: 7, mark: $mark})
CREATE (cuv:CodeUnitVersion {tenant_id: $tenant_id, unit_id: 'macros/m.sql', checksum: 'c1', version_seq: 1, mark: $mark})
CREATE (rej:Rejection {tenant_id: $tenant_id, release_id: 'r6', node_id: 'analytics.table_d', stage: 'compile',
  category: 'compile', reason: 'syntax', at: $utc, error_excerpt: 'line 1\nline 2 – ünïcode', mark: $mark})
CREATE (sig:ErrorSignature {tenant_id: $tenant_id, signature: 'sig-1', category: 'compile', reason: 'syntax', mark: $mark})
CREATE (p:Proposal {tenant_id: $tenant_id, proposal_id: 'p1', status: 'opened', mark: $mark})
CREATE (pra:PullRequest {tenant_id: $tenant_id, proposal_id: 'p1', service: 'svc-a', pr_number: 12, mark: $mark})
CREATE (prb:PullRequest {tenant_id: $tenant_id, proposal_id: 'p1', service: 'svc-b', pr_number: 13, merged: false, mark: $mark})
CREATE (pm:ProcessedMessage {tenant_id: $tenant_id, group: $group, message_id: '1762077600000-0',
  outbox_entry_id: $outbox_entry_id, processed_at: datetime(), mark: $mark})
CREATE (a)-[:CURRENT {promoted_at: $utc, release_id: 'r7'}]->(va2)
CREATE (d)-[:CURRENT {promoted_at: $utc, release_id: 'r6'}]->(vd)
CREATE (cu)-[:CURRENT {release_id: 'r7'}]->(cuv)
CREATE (b)-[:DEPENDS_ON]->(a)
CREATE (b)-[:DEPENDS_ON]->(a)
CREATE (a)-[:DEPENDS_ON]->(a)
CREATE (x)-[:DEPENDS_ON]->(b)
CREATE (va1)-[:USES_CODE]->(cuv)
CREATE (va2)-[:USES_CODE]->(cuv)
CREATE (rej)-[:RESOLVED_BY]->(va2)
CREATE (rej)-[:RESOLVED_BY {service: 'svc-a'}]->(p)
CREATE (rej)-[:HAS_SIGNATURE]->(sig)
CREATE (d)-[:FAILED {at: $utc}]->(rej)
CREATE (rej)-[:PROPOSED]->(p)
CREATE (p)-[:HAS_PR]->(pra)
CREATE (p)-[:HAS_PR]->(prb)
CREATE (p)-[:EDITED {service: 'svc-a', merged_at: $utc}]->(a)
CREATE (p)-[:EDITED {service: 'svc-b'}]->(a)
CREATE (p)-[:EDITED {service: 'svc-a'}]->(d)
CREATE (p)-[:EDITED]->(x)`, map[string]any{
		"mark": mark, "utc": utc, "long": strings.Repeat("select 1 -- ü\n", 80000),
		"group": streams.CatalogReleaseTopology, "outbox_entry_id": uuid.NewString(),
	})
	f.exec(tenantID, `
UNWIND range(1, $n) AS i
CREATE (:Node {tenant_id: $tenant_id, unique_id: 'bulk.n' + toString(i), active: true, code_version_promotion_seq: 7, ordinal: i, mark: $mark})`,
		map[string]any{"n": bulkNodes, "mark": mark})
	f.exec(tenantID, `
MATCH (n:Node {tenant_id: $tenant_id}) WHERE n.ordinal > 1
MATCH (prev:Node {tenant_id: $tenant_id, unique_id: 'bulk.n' + toString(n.ordinal - 1)})
CREATE (n)-[:DEPENDS_ON {ordinal: n.ordinal}]->(prev)`, nil)
}

// takeLease takes the tenant's export lease through the lease adapter, as an
// export or a restore does.
func (f *graphFixture) takeLease(tenantID string, kind ports.LeaseKind, holder string) {
	f.t.Helper()
	require.NoError(f.t, NewExportLease(f.client, quietLogger()).Acquire(context.Background(), tenantID, kind, holder, time.Minute))
}

func TestExportRestore_ReturnsAnIdenticalGraph(t *testing.T) {
	f := newGraphFixture(t)
	tenantID, foreign := f.tenant("identical"), f.tenant("identical-foreign")
	f.seedEveryLabel(tenantID, "mine")
	f.seedEveryLabel(foreign, "theirs")

	f.takeLease(tenantID, ports.LeaseExport, "exporter")
	wantNodes, wantRels := f.dump(tenantID)
	foreignNodes, foreignRels := f.dump(foreign)
	data := f.export(tenantID)
	require.NoError(t, NewExportLease(f.client, quietLogger()).Release(context.Background(), tenantID, "exporter"))

	// The file holds every label and every relationship type of the schema.
	h, _, _ := records(t, data)
	for _, l := range graphschema.Labels() {
		assert.Positive(t, h.Nodes[string(l)], "the export counts no :%s", l)
	}
	for _, r := range graphschema.RelTypes() {
		assert.Positive(t, h.Relationships[string(r)], "the export counts no :%s", r)
	}
	assert.Equal(t, int64(bulkNodes+4), h.Nodes["Node"], ":Node count")

	f.takeLease(tenantID, ports.LeaseRestore, "restorer")
	f.restore(tenantID, data)

	gotNodes, gotRels := f.dump(tenantID)
	assert.Equal(t, wantNodes, gotNodes, "the restored nodes")
	assert.Equal(t, wantRels, gotRels, "the restored relationships")
	gotForeignNodes, gotForeignRels := f.dump(foreign)
	assert.Equal(t, foreignNodes, gotForeignNodes, "another tenant's nodes")
	assert.Equal(t, foreignRels, gotForeignRels, "another tenant's relationships")

	// Exporting the restored graph gives the same records.
	_, n1, r1 := records(t, data)
	_, n2, r2 := records(t, f.export(tenantID))
	assert.ElementsMatch(t, renderRecords(n1, r1), renderRecords(n2, r2), "a second export of the restored graph")
}

// renderRecords renders decoded records as canonical text.
func renderRecords(nodes []export.NodeRecord, rels []export.RelRecord) []string {
	var out []string
	for _, n := range nodes {
		labels := make([]any, 0, len(n.Labels))
		for _, l := range n.Labels {
			labels = append(labels, l)
		}
		out = append(out, renderNode(labels, n.Props))
	}
	for _, r := range rels {
		out = append(out, r.Type+" "+r.From.Label+renderValue(r.From.Key)+"->"+r.To.Label+renderValue(r.To.Key)+" "+renderProps(r.Props))
	}
	return out
}

func TestExportRestore_RoundTripsEveryPropertyType(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("types")
	withNanos := time.Date(2026, 10, 30, 2, 0, 0, 123456789, time.UTC)
	props := map[string]any{
		"text":                       "héllo 世界 🚀\nsecond line\t\"quoted\" <tag> & \\  ",
		"int_min":                    int64(math.MinInt64),
		"int_max":                    int64(math.MaxInt64),
		"float_tenth":                0.1,
		"float_negzero":              math.Copysign(0, -1),
		"float_nan":                  math.NaN(),
		"float_inf":                  math.Inf(1),
		"float_ninf":                 math.Inf(-1),
		"yes":                        true,
		"no":                         false,
		"dt_zone":                    withNanos,
		"strings":                    []any{"a", "b", ""},
		"ints":                       []any{int64(1), int64(math.MinInt64), int64(math.MaxInt64)},
		"floats":                     []any{0.5, math.Copysign(0, -1)},
		"empty":                      []any{},
		"bools":                      []any{true, false},
		"datetimes":                  []any{withNanos, withNanos.Add(time.Nanosecond)},
		"$int":                       "a property named like a typed value",
		"node":                       "a property named like a record member",
		"code_version_promotion_seq": int64(1),
	}
	f.exec(tenantID, `CREATE (n:Node {tenant_id: $tenant_id, unique_id: 'types'}) SET n += $props, n.dt_offset = datetime('2026-10-30T02:00:00.000000001Z'), n.dt_plus2 = datetime('2026-10-30T04:00:00.5+02:00')`,
		map[string]any{"props": props})
	wantNodes, _ := f.dump(tenantID)

	data := f.export(tenantID)
	f.restore(tenantID, data)

	gotNodes, _ := f.dump(tenantID)
	assert.Equal(t, wantNodes, gotNodes)

	recs := f.query(tenantID, `MATCH (n:Node {tenant_id: $tenant_id, unique_id: 'types'}) RETURN properties(n)`, nil)
	require.Len(t, recs, 1)
	got := recs[0].Values[0].(map[string]any)
	assert.Equal(t, int64(math.MinInt64), got["int_min"])
	assert.Equal(t, int64(math.MaxInt64), got["int_max"])
	assert.Equal(t, 0.1, got["float_tenth"])
	assert.True(t, math.Signbit(got["float_negzero"].(float64)) && got["float_negzero"].(float64) == 0, "-0 stays -0: %v", got["float_negzero"])
	assert.True(t, math.IsNaN(got["float_nan"].(float64)), "NaN stays NaN")
	assert.Equal(t, props["text"], got["text"])
	assert.Equal(t, true, got["yes"])
	dt := got["dt_zone"].(time.Time)
	assert.True(t, dt.Equal(withNanos) && dt.Nanosecond() == 123456789 && dt.Location().String() == "UTC", "zoned datetime %v", dt)
	off := got["dt_offset"].(time.Time)
	if name, secs := off.Zone(); name != "Offset" || secs != 0 || off.Nanosecond() != 1 {
		t.Errorf("datetime() with an offset came back as %v (%s %d)", off, name, secs)
	}
	if _, secs := got["dt_plus2"].(time.Time).Zone(); secs != 7200 {
		t.Errorf("datetime at +02:00 came back at offset %d", secs)
	}
	assert.Equal(t, []any{}, got["empty"])
	assert.Equal(t, []any{int64(1), int64(math.MinInt64), int64(math.MaxInt64)}, got["ints"])

	// The datetime stored with an offset and the one stored by zone name stay
	// two different values in Cypher, as they were.
	eq := f.query(tenantID, `MATCH (n:Node {tenant_id: $tenant_id, unique_id: 'types'})
RETURN n.dt_offset.timezone AS a, n.dt_zone.timezone AS b`, nil)
	assert.Equal(t, "Z", eq[0].Values[0])
	assert.Equal(t, "UTC", eq[0].Values[1])
}

func TestExport_SkipsLeaseNode(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("skip-lease")
	f.exec(tenantID, `CREATE (:Meta {tenant_id: $tenant_id, key: 'current_release', promotion_seq: 3}),
  (:Node {tenant_id: $tenant_id, unique_id: 'a', code_version_promotion_seq: 3})`, nil)
	f.takeLease(tenantID, ports.LeaseExport, "exporter")
	require.Len(t, f.leaseNodes(tenantID), 1)

	h, nodes, rels := records(t, f.export(tenantID))
	assert.Equal(t, map[string]int64{"Meta": 1, "Node": 1}, h.Nodes)
	assert.Empty(t, h.Relationships)
	assert.Len(t, nodes, 2)
	assert.Empty(t, rels)
	for _, n := range nodes {
		assert.False(t, n.IsLease(), "the export holds the lease node: %v", n.Props)
		assert.NotContains(t, n.Props, "holder")
	}
}

func TestExport_ForeignTenantNodesAreNotExported(t *testing.T) {
	f := newGraphFixture(t)
	mine, theirs := f.tenant("mine"), f.tenant("theirs")
	f.seedEveryLabel(mine, "mine")
	f.seedEveryLabel(theirs, "theirs")
	f.exec(theirs, `CREATE (:Node {tenant_id: $tenant_id, unique_id: 'only.theirs', code_version_promotion_seq: 1, mark: 'theirs'})`, nil)

	h, nodes, rels := records(t, f.export(mine))
	assert.Equal(t, mine, h.TenantID)
	assert.Equal(t, int64(bulkNodes+4), h.Nodes["Node"])
	for _, n := range nodes {
		assert.Equal(t, mine, n.Props["tenant_id"])
		assert.Equal(t, "mine", n.Props["mark"])
		assert.NotEqual(t, "only.theirs", n.Props["unique_id"])
	}
	var total int64
	for _, c := range h.Relationships {
		total += c
	}
	assert.Equal(t, total, int64(len(rels)))
}

// countingWriter records how many records an export wrote.
type countingWriter struct {
	headers, nodes, rels int
}

func (w *countingWriter) WriteHeader(export.Header) error   { w.headers++; return nil }
func (w *countingWriter) WriteNode(export.NodeRecord) error { w.nodes++; return nil }
func (w *countingWriter) WriteRel(export.RelRecord) error   { w.rels++; return nil }

// TestExport_RefusesAGraphItCannotRestore: a node of a label outside the
// schema, a relationship to another tenant and a property type the format
// does not carry each fail the export, so no file is written that the restore
// would refuse or load differently.
func TestExport_RefusesAGraphItCannotRestore(t *testing.T) {
	cases := []struct {
		name  string
		seed  string
		want  error
		wrote bool
	}{
		{"a label outside the schema", `CREATE (:B2Stray {tenant_id: $tenant_id})`, export.ErrOutsideSchema, false},
		{"a node with no label", `CREATE ({tenant_id: $tenant_id})`, export.ErrInvalidRecord, false},
		{"a relationship to another tenant",
			`MATCH (a:Node {tenant_id: $tenant_id, unique_id: 'a'}) CREATE (a)-[:DEPENDS_ON]->(:Node {tenant_id: $other, unique_id: 'x'})`,
			errExportUnrestorable, false},
		{"a relationship of the lease node",
			`MATCH (a:Node {tenant_id: $tenant_id, unique_id: 'a'}) MERGE (m:Meta {tenant_id: $tenant_id, key: 'export'}) CREATE (a)-[:DEPENDS_ON]->(m)`,
			errExportUnrestorable, false},
		{"a date property", `MATCH (a:Node {tenant_id: $tenant_id, unique_id: 'a'}) SET a.day = date('2026-10-30')`, export.ErrUnsupportedValue, true},
		{"an end without its identity",
			`MATCH (a:Node {tenant_id: $tenant_id, unique_id: 'a'}) CREATE (a)-[:DEPENDS_ON]->(:Node {tenant_id: $tenant_id})`,
			export.ErrInvalidRecord, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newGraphFixture(t)
			tenantID, other := f.tenant("refuse"), f.tenant("refuse-other")
			f.exec(tenantID, `CREATE (:Node {tenant_id: $tenant_id, unique_id: 'a', code_version_promotion_seq: 1})`, nil)
			f.exec(tenantID, c.seed, map[string]any{"other": other})
			w := &countingWriter{}
			err := f.exporter().Export(context.Background(), tenantID, w)
			assert.ErrorIs(t, err, c.want)
			if !c.wrote {
				assert.Zero(t, w.headers+w.nodes+w.rels, "the export wrote records before it refused")
			}
		})
	}
}

func TestExport_RefusesAnEmptyTenant(t *testing.T) {
	f := newGraphFixture(t)
	err := f.exporter().Export(context.Background(), "", &countingWriter{})
	assert.ErrorIs(t, err, errEmptyTenant)
}

// changingWriter commits a node into the tenant once the header is written,
// as a write the export lease failed to pause would.
type changingWriter struct {
	countingWriter
	f        *graphFixture
	tenantID string
}

func (w *changingWriter) WriteHeader(h export.Header) error {
	w.f.exec(w.tenantID, `CREATE (:Node {tenant_id: $tenant_id, unique_id: 'late', code_version_promotion_seq: 1})`, nil)
	return w.countingWriter.WriteHeader(h)
}

// TestExport_FailsWhenTheGraphChangesUnderIt: the stream must match the
// counts the header was written with, so an export that saw a write fails
// instead of producing a file the restore refuses.
func TestExport_FailsWhenTheGraphChangesUnderIt(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("changing")
	f.exec(tenantID, `CREATE (:Node {tenant_id: $tenant_id, unique_id: 'a', code_version_promotion_seq: 1})`, nil)
	err := f.exporter().Export(context.Background(), tenantID, &changingWriter{f: f, tenantID: tenantID})
	assert.ErrorIs(t, err, errExportChanged)
}
