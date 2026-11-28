//go:build integration

package neo4jinfra

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/serialization/backupfile"
	"github.com/carolsimone/continuo/catalog/service/graphbackup"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func backupHeader(tenantID string, nodes, rels map[string]int64) graphbackup.Header {
	return graphbackup.Header{
		FormatVersion: graphbackup.FormatVersion, TenantID: tenantID, TakenAt: time.Now().UTC(),
		CatalogVersion: "b2-test", Nodes: nodes, Relationships: rels,
	}
}

// writeBackup renders a header and records as a backup file.
func writeBackup(t *testing.T, h graphbackup.Header, recs ...any) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := backupfile.NewWriter(&buf)
	require.NoError(t, w.WriteHeader(h))
	for _, rec := range recs {
		switch r := rec.(type) {
		case graphbackup.NodeRecord:
			require.NoError(t, w.WriteNode(r))
		case graphbackup.RelRecord:
			require.NoError(t, w.WriteRel(r))
		}
	}
	return buf.Bytes()
}

func TestLoad_KeepsTheLeaseNodeAndDoesNotLoadASavedOne(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("lease")
	ctx := context.Background()

	// A backup that carries a lease node a crashed taker held.
	future := time.Now().Add(24 * time.Hour).UTC()
	data := writeBackup(t, backupHeader(tenantID, map[string]int64{"Meta": 2, "Node": 1}, nil),
		graphbackup.NodeRecord{Labels: []string{"Meta"}, Props: map[string]any{
			"tenant_id": tenantID, "key": "current_release", "promotion_seq": int64(4),
		}},
		graphbackup.NodeRecord{Labels: []string{"Meta"}, Props: map[string]any{
			"tenant_id": tenantID, "key": "backup", "holder": "crashed-taker", "kind": "backup", "until": future,
		}},
		graphbackup.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{
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
		assert.NotContains(t, n, "crashed-taker")
		assert.NotContains(t, n, "stale")
	}

	// Once the restore releases the lease, the tenant's writes go on.
	require.NoError(t, NewBackupLease(f.client, quietLogger()).Release(ctx, tenantID, "restorer"))
	require.NoError(t, NewBackupLease(f.client, quietLogger()).Acquire(ctx, tenantID, ports.LeaseBackup, "next", time.Minute))
}

// fakeReader is a RecordReader that hands the loader records no file
// check has seen.
type fakeReader struct {
	header  graphbackup.Header
	records []any
}

func (r *fakeReader) Header() graphbackup.Header { return r.header }

func (r *fakeReader) Next() (any, error) {
	if len(r.records) == 0 {
		return nil, io.EOF
	}
	rec := r.records[0]
	r.records = r.records[1:]
	return rec, nil
}

// TestLoad_RefusesALabelOrTypeOutsideTheSchema: the loader checks every
// label and type itself, so a record that reaches it without a file check
// still writes nothing.
func TestLoad_RefusesALabelOrTypeOutsideTheSchema(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("refuse-schema")
	node := func(labels ...string) graphbackup.NodeRecord {
		return graphbackup.NodeRecord{Labels: labels, Props: map[string]any{"tenant_id": tenantID, "unique_id": "a"}}
	}
	nodeA := node("Node")
	rel := func(typ, fromLabel, toLabel string) graphbackup.RelRecord {
		return graphbackup.RelRecord{Type: typ, From: graphbackup.NodeRef{Label: fromLabel, Key: []any{"a"}}, To: graphbackup.NodeRef{Label: toLabel, Key: []any{"a"}}}
	}
	cases := []struct {
		name   string
		header graphbackup.Header
		recs   []any
	}{
		{"a header label", backupHeader(tenantID, map[string]int64{"Table": 1}, nil), []any{node("Table")}},
		{"a header type", backupHeader(tenantID, map[string]int64{"Node": 1}, map[string]int64{"EXECUTES": 1}), []any{nodeA, rel("EXECUTES", "Node", "Node")}},
		{"a node label", backupHeader(tenantID, map[string]int64{"Node": 1}, nil), []any{node("Evil")}},
		{"a second node label", backupHeader(tenantID, map[string]int64{"Node": 1}, nil), []any{node("Node", "Evil")}},
		{"a label that injects Cypher", backupHeader(tenantID, map[string]int64{"Node": 1}, nil),
			[]any{node("Node {tenant_id: 'other'}) DETACH DELETE (n")}},
		{"a relationship type", backupHeader(tenantID, map[string]int64{"Node": 1}, map[string]int64{"DEPENDS_ON": 1}),
			[]any{nodeA, rel("DEPENDS_ON]->(b) DETACH DELETE b //", "Node", "Node")}},
		{"a relationship end", backupHeader(tenantID, map[string]int64{"Node": 1}, map[string]int64{"DEPENDS_ON": 1}),
			[]any{nodeA, rel("DEPENDS_ON", "Node", "Table")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := f.loader().Load(context.Background(), tenantID, &fakeReader{header: c.header, records: c.recs})
			assert.ErrorIs(t, err, graphbackup.ErrOutsideSchema)
			nodes, rels := f.dump(tenantID)
			assert.Empty(t, nodes, "the loader wrote nodes")
			assert.Empty(t, rels, "the loader wrote relationships")
		})
	}
}

func TestLoad_RefusesAnotherTenantsRecords(t *testing.T) {
	f := newGraphFixture(t)
	tenantID, other := f.tenant("refuse-tenant"), f.tenant("refuse-tenant-other")
	ctx := context.Background()

	err := f.loader().Load(ctx, tenantID, &fakeReader{header: backupHeader(other, nil, nil)})
	assert.ErrorIs(t, err, graphbackup.ErrTenantMismatch, "a header of another tenant")

	err = f.loader().Load(ctx, tenantID, &fakeReader{
		header:  backupHeader(tenantID, map[string]int64{"Node": 1}, nil),
		records: []any{graphbackup.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{"tenant_id": other, "unique_id": "a"}}},
	})
	assert.ErrorIs(t, err, graphbackup.ErrTenantMismatch, "a node of another tenant")
	for _, id := range []string{tenantID, other} {
		nodes, _ := f.dump(id)
		assert.Empty(t, nodes, "tenant %s", id)
	}
}

func TestLoad_RefusesATenantThatHoldsNodes(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("not-wiped")
	f.exec(tenantID, `CREATE (:Node {tenant_id: $tenant_id, unique_id: 'kept', code_version_promotion_seq: 1})`, nil)
	err := f.loader().Load(context.Background(), tenantID, &fakeReader{
		header:  backupHeader(tenantID, map[string]int64{"Node": 1}, nil),
		records: []any{graphbackup.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{"tenant_id": tenantID, "unique_id": "a"}}},
	})
	assert.ErrorIs(t, err, errLoadTenantNotEmpty)
	nodes, _ := f.dump(tenantID)
	assert.Len(t, nodes, 1)
}

// TestLoad_RefusesWhatItCannotLoadExactly: an restore whose relationship
// names no node of the backup, or whose records disagree with the header's
// counts, fails instead of reporting a graph other than the backup.
func TestLoad_RefusesWhatItCannotLoadExactly(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("inexact")
	ctx := context.Background()
	nodeA := graphbackup.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{"tenant_id": tenantID, "unique_id": "a"}}

	err := f.loader().Load(ctx, tenantID, &fakeReader{
		header: backupHeader(tenantID, map[string]int64{"Node": 1}, map[string]int64{"DEPENDS_ON": 1}),
		records: []any{nodeA, graphbackup.RelRecord{Type: "DEPENDS_ON",
			From: graphbackup.NodeRef{Label: "Node", Key: []any{"a"}}, To: graphbackup.NodeRef{Label: "Node", Key: []any{"missing"}}}},
	})
	assert.ErrorIs(t, err, errLoadUnresolvedEnd)

	require.NoError(t, f.loader().Wipe(ctx, tenantID))
	err = f.loader().Load(ctx, tenantID, &fakeReader{
		header:  backupHeader(tenantID, map[string]int64{"Node": 2}, nil),
		records: []any{nodeA},
	})
	assert.ErrorIs(t, err, errLoadCountMismatch)

	require.NoError(t, f.loader().Wipe(ctx, tenantID))
	err = f.loader().Load(ctx, tenantID, &fakeReader{
		header:  backupHeader(tenantID, map[string]int64{"Node": 2}, map[string]int64{"DEPENDS_ON": 1}),
		records: []any{nodeA, graphbackup.RelRecord{Type: "DEPENDS_ON", From: graphbackup.NodeRef{Label: "Node", Key: []any{"a"}}, To: graphbackup.NodeRef{Label: "Node", Key: []any{"a"}}}, nodeA},
	})
	assert.ErrorIs(t, err, errLoadOutOfOrder, "a node after a relationship")
}

// TestLoad_BringsAnOlderBackupUpToDate: the restore ends with the startup
// migration, so a backup written before a node carried its version
// watermark, or while a retired node kept its dependencies, comes up to the
// current shape.
func TestLoad_BringsAnOlderBackupUpToDate(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("older")
	data := writeBackup(t, backupHeader(tenantID, map[string]int64{"Meta": 1, "Node": 2}, map[string]int64{"DEPENDS_ON": 1}),
		graphbackup.NodeRecord{Labels: []string{"Meta"}, Props: map[string]any{"tenant_id": tenantID, "key": "current_release", "promotion_seq": int64(9)}},
		graphbackup.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{"tenant_id": tenantID, "unique_id": "live", "active": true}},
		graphbackup.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{"tenant_id": tenantID, "unique_id": "retired", "active": false,
			"code_version_promotion_seq": int64(3)}},
		graphbackup.RelRecord{Type: "DEPENDS_ON", From: graphbackup.NodeRef{Label: "Node", Key: []any{"retired"}}, To: graphbackup.NodeRef{Label: "Node", Key: []any{"live"}}},
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

	require.NoError(t, f.loader().Wipe(context.Background(), tenantID))

	nodes, rels := f.dump(tenantID)
	assert.Empty(t, nodes)
	assert.Empty(t, rels)
	assert.Equal(t, lease, f.leaseNodes(tenantID), "the lease node stays as it was")
	gotNodes, gotRels := f.dump(foreign)
	assert.Equal(t, foreignNodes, gotNodes, "another tenant's nodes")
	assert.Equal(t, foreignRels, gotRels, "another tenant's relationships")

	assert.ErrorIs(t, f.loader().Wipe(context.Background(), ""), errEmptyTenant)
}
