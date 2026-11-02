//go:build integration

package neo4jinfra

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/serialization/exportfile"
	"github.com/carolsimone/continuo/catalog/service/export"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func exportHeader(tenantID string, nodes, rels map[string]int64) export.Header {
	return export.Header{
		FormatVersion: export.FormatVersion, TenantID: tenantID, ExportedAt: time.Now().UTC(),
		CatalogVersion: "b2-test", Nodes: nodes, Relationships: rels,
	}
}

// writeExport renders a header and records as an export file.
func writeExport(t *testing.T, h export.Header, recs ...any) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := exportfile.NewWriter(&buf)
	require.NoError(t, w.WriteHeader(h))
	for _, rec := range recs {
		switch r := rec.(type) {
		case export.NodeRecord:
			require.NoError(t, w.WriteNode(r))
		case export.RelRecord:
			require.NoError(t, w.WriteRel(r))
		}
	}
	return buf.Bytes()
}

func TestImport_KeepsTheLeaseNodeAndDoesNotLoadAnExportedOne(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("lease")
	ctx := context.Background()

	// An export that carries a lease node a crashed exporter held.
	future := time.Now().Add(24 * time.Hour).UTC()
	data := writeExport(t, exportHeader(tenantID, map[string]int64{"Meta": 2, "Node": 1}, nil),
		export.NodeRecord{Labels: []string{"Meta"}, Props: map[string]any{
			"tenant_id": tenantID, "key": "current_release", "promotion_seq": int64(4),
		}},
		export.NodeRecord{Labels: []string{"Meta"}, Props: map[string]any{
			"tenant_id": tenantID, "key": "export", "holder": "crashed-exporter", "kind": "export", "until": future,
		}},
		export.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{
			"tenant_id": tenantID, "unique_id": "a", "code_version_promotion_seq": int64(4),
		}},
	)
	f.exec(tenantID, `CREATE (:Node {tenant_id: $tenant_id, unique_id: 'stale', code_version_promotion_seq: 1})`, nil)
	f.takeLease(tenantID, ports.LeaseRestore, "restorer")
	before := f.leaseNodes(tenantID)
	require.Len(t, before, 1)

	f.restore(tenantID, data)

	after := f.leaseNodes(tenantID)
	require.Len(t, after, 1, "one lease node")
	assert.Equal(t, before[0], after[0], "the restore's lease is untouched")
	assert.Equal(t, "restorer", after[0]["holder"])
	nodes, _ := f.dump(tenantID)
	assert.Len(t, nodes, 2, "current_release and node a: %v", nodes)
	for _, n := range nodes {
		assert.NotContains(t, n, "crashed-exporter")
		assert.NotContains(t, n, "stale")
	}

	// Once the restore releases the lease, the tenant's writes go on.
	require.NoError(t, NewExportLease(f.client, quietLogger()).Release(ctx, tenantID, "restorer"))
	require.NoError(t, NewExportLease(f.client, quietLogger()).Acquire(ctx, tenantID, ports.LeaseExport, "next", time.Minute))
}

// fakeReader is a RecordReader that hands the importer records no file
// check has seen.
type fakeReader struct {
	header  export.Header
	records []any
}

func (r *fakeReader) Header() export.Header { return r.header }

func (r *fakeReader) Next() (any, error) {
	if len(r.records) == 0 {
		return nil, io.EOF
	}
	rec := r.records[0]
	r.records = r.records[1:]
	return rec, nil
}

// TestImport_RefusesALabelOrTypeOutsideTheSchema: the importer checks every
// label and type itself, so a record that reaches it without a file check
// still writes nothing.
func TestImport_RefusesALabelOrTypeOutsideTheSchema(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("refuse-schema")
	node := func(labels ...string) export.NodeRecord {
		return export.NodeRecord{Labels: labels, Props: map[string]any{"tenant_id": tenantID, "unique_id": "a"}}
	}
	nodeA := node("Node")
	rel := func(typ, fromLabel, toLabel string) export.RelRecord {
		return export.RelRecord{Type: typ, From: export.NodeRef{Label: fromLabel, Key: []any{"a"}}, To: export.NodeRef{Label: toLabel, Key: []any{"a"}}}
	}
	cases := []struct {
		name   string
		header export.Header
		recs   []any
	}{
		{"a header label", exportHeader(tenantID, map[string]int64{"Table": 1}, nil), []any{node("Table")}},
		{"a header type", exportHeader(tenantID, map[string]int64{"Node": 1}, map[string]int64{"EXECUTES": 1}), []any{nodeA, rel("EXECUTES", "Node", "Node")}},
		{"a node label", exportHeader(tenantID, map[string]int64{"Node": 1}, nil), []any{node("Evil")}},
		{"a second node label", exportHeader(tenantID, map[string]int64{"Node": 1}, nil), []any{node("Node", "Evil")}},
		{"a label that injects Cypher", exportHeader(tenantID, map[string]int64{"Node": 1}, nil),
			[]any{node("Node {tenant_id: 'other'}) DETACH DELETE (n")}},
		{"a relationship type", exportHeader(tenantID, map[string]int64{"Node": 1}, map[string]int64{"DEPENDS_ON": 1}),
			[]any{nodeA, rel("DEPENDS_ON]->(b) DETACH DELETE b //", "Node", "Node")}},
		{"a relationship end", exportHeader(tenantID, map[string]int64{"Node": 1}, map[string]int64{"DEPENDS_ON": 1}),
			[]any{nodeA, rel("DEPENDS_ON", "Node", "Table")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := f.importer().Import(context.Background(), tenantID, &fakeReader{header: c.header, records: c.recs})
			assert.ErrorIs(t, err, export.ErrOutsideSchema)
			nodes, rels := f.dump(tenantID)
			assert.Empty(t, nodes, "the importer wrote nodes")
			assert.Empty(t, rels, "the importer wrote relationships")
		})
	}
}

func TestImport_RefusesAnotherTenantsRecords(t *testing.T) {
	f := newGraphFixture(t)
	tenantID, other := f.tenant("refuse-tenant"), f.tenant("refuse-tenant-other")
	ctx := context.Background()

	err := f.importer().Import(ctx, tenantID, &fakeReader{header: exportHeader(other, nil, nil)})
	assert.ErrorIs(t, err, export.ErrTenantMismatch, "a header of another tenant")

	err = f.importer().Import(ctx, tenantID, &fakeReader{
		header:  exportHeader(tenantID, map[string]int64{"Node": 1}, nil),
		records: []any{export.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{"tenant_id": other, "unique_id": "a"}}},
	})
	assert.ErrorIs(t, err, export.ErrTenantMismatch, "a node of another tenant")
	for _, id := range []string{tenantID, other} {
		nodes, _ := f.dump(id)
		assert.Empty(t, nodes, "tenant %s", id)
	}
}

func TestImport_RefusesATenantThatHoldsNodes(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("not-wiped")
	f.exec(tenantID, `CREATE (:Node {tenant_id: $tenant_id, unique_id: 'kept', code_version_promotion_seq: 1})`, nil)
	err := f.importer().Import(context.Background(), tenantID, &fakeReader{
		header:  exportHeader(tenantID, map[string]int64{"Node": 1}, nil),
		records: []any{export.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{"tenant_id": tenantID, "unique_id": "a"}}},
	})
	assert.ErrorIs(t, err, errImportTenantNotEmpty)
	nodes, _ := f.dump(tenantID)
	assert.Len(t, nodes, 1)
}

// TestImport_RefusesWhatItCannotLoadExactly: an import whose relationship
// names no node of the export, or whose records disagree with the header's
// counts, fails instead of reporting a graph other than the export.
func TestImport_RefusesWhatItCannotLoadExactly(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("inexact")
	ctx := context.Background()
	nodeA := export.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{"tenant_id": tenantID, "unique_id": "a"}}

	err := f.importer().Import(ctx, tenantID, &fakeReader{
		header: exportHeader(tenantID, map[string]int64{"Node": 1}, map[string]int64{"DEPENDS_ON": 1}),
		records: []any{nodeA, export.RelRecord{Type: "DEPENDS_ON",
			From: export.NodeRef{Label: "Node", Key: []any{"a"}}, To: export.NodeRef{Label: "Node", Key: []any{"missing"}}}},
	})
	assert.ErrorIs(t, err, errImportUnresolvedEnd)

	require.NoError(t, f.importer().Wipe(ctx, tenantID))
	err = f.importer().Import(ctx, tenantID, &fakeReader{
		header:  exportHeader(tenantID, map[string]int64{"Node": 2}, nil),
		records: []any{nodeA},
	})
	assert.ErrorIs(t, err, errImportCountMismatch)

	require.NoError(t, f.importer().Wipe(ctx, tenantID))
	err = f.importer().Import(ctx, tenantID, &fakeReader{
		header:  exportHeader(tenantID, map[string]int64{"Node": 2}, map[string]int64{"DEPENDS_ON": 1}),
		records: []any{nodeA, export.RelRecord{Type: "DEPENDS_ON", From: export.NodeRef{Label: "Node", Key: []any{"a"}}, To: export.NodeRef{Label: "Node", Key: []any{"a"}}}, nodeA},
	})
	assert.ErrorIs(t, err, errImportOutOfOrder, "a node after a relationship")
}

// TestImport_BringsAnOlderExportUpToDate: the import ends with the startup
// migration, so an export written before a node carried its version
// watermark, or while a retired node kept its dependencies, comes up to the
// current shape.
func TestImport_BringsAnOlderExportUpToDate(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("older")
	data := writeExport(t, exportHeader(tenantID, map[string]int64{"Meta": 1, "Node": 2}, map[string]int64{"DEPENDS_ON": 1}),
		export.NodeRecord{Labels: []string{"Meta"}, Props: map[string]any{"tenant_id": tenantID, "key": "current_release", "promotion_seq": int64(9)}},
		export.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{"tenant_id": tenantID, "unique_id": "live", "active": true}},
		export.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{"tenant_id": tenantID, "unique_id": "retired", "active": false,
			"code_version_promotion_seq": int64(3)}},
		export.RelRecord{Type: "DEPENDS_ON", From: export.NodeRef{Label: "Node", Key: []any{"retired"}}, To: export.NodeRef{Label: "Node", Key: []any{"live"}}},
	)
	f.restore(tenantID, data)

	recs := f.query(tenantID, `MATCH (n:Node {tenant_id: $tenant_id, unique_id: 'live'}) RETURN n.code_version_promotion_seq`, nil)
	require.Len(t, recs, 1)
	assert.Equal(t, int64(9), recs[0].Values[0], "the live seq becomes the node's watermark")
	_, rels := f.dump(tenantID)
	assert.Empty(t, rels, "a retired node keeps no outgoing :DEPENDS_ON")
}

func TestWipe_DeletesEveryNodeOfTheTenantButTheLease(t *testing.T) {
	f := newGraphFixture(t)
	tenantID, foreign := f.tenant("wipe"), f.tenant("wipe-foreign")
	f.seedEveryLabel(tenantID, "mine")
	f.seedEveryLabel(foreign, "theirs")
	f.takeLease(tenantID, ports.LeaseRestore, "restorer")
	lease := f.leaseNodes(tenantID)
	foreignNodes, foreignRels := f.dump(foreign)

	require.NoError(t, f.importer().Wipe(context.Background(), tenantID))

	nodes, rels := f.dump(tenantID)
	assert.Empty(t, nodes)
	assert.Empty(t, rels)
	assert.Equal(t, lease, f.leaseNodes(tenantID), "the lease node stays as it was")
	gotNodes, gotRels := f.dump(foreign)
	assert.Equal(t, foreignNodes, gotNodes, "another tenant's nodes")
	assert.Equal(t, foreignRels, gotRels, "another tenant's relationships")

	assert.ErrorIs(t, f.importer().Wipe(context.Background(), ""), errEmptyTenant)
}
