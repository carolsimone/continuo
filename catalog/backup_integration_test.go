//go:build integration

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	s3infra "github.com/carolsimone/continuo/catalog/adapters/s3"
	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/service/backup"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The backup as main wires it, against the test Neo4j and an object store in
// memory: the production lease, graph exporter and importer, unit of work and
// backup store, with the backup service of catalog/service/backup. They live
// in the composition root because the service layer's tests never reach the
// object store.

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// recordedMetrics records the outcome of every export.
type recordedMetrics struct {
	mu       sync.Mutex
	outcomes []string
}

func (m *recordedMetrics) ExportObserved(outcome string, _ time.Duration, _ int64, _, _ int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outcomes = append(m.outcomes, outcome)
}

func (m *recordedMetrics) seen() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.outcomes...)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// graphFixture is the test Neo4j, an object store in memory behind the
// production backup store, and the production lease, exporter, importer and
// unit of work. Each test takes tenants of its own; the cleanup DETACH
// DELETEs every node of those tenants.
type graphFixture struct {
	t        *testing.T
	client   neo4jinfra.Neo4jClient
	mem      *objectstoretest.Memory
	store    ports.BackupStore
	lease    ports.ExportLease
	graph    ports.GraphExporter
	importer ports.GraphImporter
	uow      uow.UnitOfWork
	tenants  []string
}

func newGraphFixture(t *testing.T) *graphFixture {
	t.Helper()
	ctx := context.Background()
	client, err := neo4jinfra.NewNeo4jClient(
		envOr("NEO4J_URI", "bolt://neo4j:7687"),
		envOr("NEO4J_USER", "neo4j"),
		envOr("NEO4J_PASSWORD", "atlas_password"),
		quietLogger(),
	)
	if err != nil {
		testdeps.Unavailable(t, "Neo4j unavailable, skipping integration test: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(ctx) })
	require.NoError(t, neo4jinfra.InitSchema(ctx, client, quietLogger()))
	mem := objectstoretest.NewMemory("continuo")
	f := &graphFixture{
		t: t, client: client, mem: mem, store: s3infra.NewBackupStore(mem),
		lease:    neo4jinfra.NewExportLease(client, quietLogger()),
		graph:    neo4jinfra.NewGraphExporter(client, "b3-test", quietLogger()),
		importer: neo4jinfra.NewGraphImporter(client, quietLogger()),
		uow:      neo4jinfra.NewUnitOfWork(client, quietLogger()),
	}
	t.Cleanup(func() {
		for _, tenantID := range f.tenants {
			f.exec(tenantID, `MATCH (n {tenant_id: $tenant_id}) DETACH DELETE n`, nil)
		}
	})
	return f
}

func (f *graphFixture) tenant(name string) string {
	id := "b3-" + name + "-" + uuid.NewString()
	f.tenants = append(f.tenants, id)
	return id
}

func (f *graphFixture) exporter(store ports.BackupStore, metrics ports.BackupMetrics, clock func() time.Time) *backup.Exporter {
	return backup.NewExporter(f.lease, f.graph, store, metrics, 7, clock, quietLogger())
}

func (f *graphFixture) restorer() *backup.Restorer {
	return backup.NewRestorer(f.lease, f.importer, f.store, quietLogger())
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

func (f *graphFixture) query(tenantID, cypher string) []*neo4j.Record {
	f.t.Helper()
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = s.Close(ctx) }()
	res, err := s.Run(ctx, cypher, map[string]any{"tenant_id": tenantID})
	require.NoError(f.t, err, cypher)
	recs, err := res.Collect(ctx)
	require.NoError(f.t, err, cypher)
	return recs
}

// seed writes a small graph for tenantID: two :Node, a :NodeVersion each
// node points at, a dependency between them, and a case-base rejection, then
// runs the startup migration so the graph has the shape the catalog keeps.
func (f *graphFixture) seed(tenantID string) {
	f.t.Helper()
	f.exec(tenantID, `
CREATE (a:Node {tenant_id: $tenant_id, unique_id: 'svc.a', active: true, schedule_name: 'daily'})
CREATE (b:Node {tenant_id: $tenant_id, unique_id: 'svc.b', active: true, schedule_name: 'daily'})
CREATE (va:NodeVersion {tenant_id: $tenant_id, unique_id: 'svc.a', content_hash: 'h-a'})
CREATE (vb:NodeVersion {tenant_id: $tenant_id, unique_id: 'svc.b', content_hash: 'h-b'})
CREATE (r:Rejection {tenant_id: $tenant_id, release_id: 'r1', node_id: 'svc.b', at: datetime('2026-10-20T09:00:00Z')})
CREATE (b)-[:DEPENDS_ON]->(a)
CREATE (a)-[:CURRENT {promoted_at: datetime('2026-10-20T09:00:00Z')}]->(va)
CREATE (b)-[:CURRENT {promoted_at: datetime('2026-10-20T09:00:00Z')}]->(vb)
CREATE (vb)-[:FAILED]->(r)`, nil)
	require.NoError(f.t, neo4jinfra.InitSchema(context.Background(), f.client, quietLogger()))
}

// dump renders tenantID's graph, its export lease node aside, as sorted
// lines: each node's labels and properties, and each relationship's type,
// properties and ends.
func (f *graphFixture) dump(tenantID string) []string {
	f.t.Helper()
	var out []string
	render := func(v any) string {
		b, err := json.Marshal(v)
		require.NoError(f.t, err)
		return string(b)
	}
	for _, rec := range f.query(tenantID, `
MATCH (n {tenant_id: $tenant_id}) WHERE NOT (n:Meta AND n.key = 'export')
RETURN labels(n) AS labels, properties(n) AS props`) {
		labels, _ := rec.Get("labels")
		props, _ := rec.Get("props")
		out = append(out, "node "+render(labels)+" "+render(props))
	}
	for _, rec := range f.query(tenantID, `
MATCH (a {tenant_id: $tenant_id})-[r]->(b {tenant_id: $tenant_id})
RETURN type(r) AS type, properties(r) AS props, properties(a) AS from, properties(b) AS to`) {
		typ, _ := rec.Get("type")
		props, _ := rec.Get("props")
		from, _ := rec.Get("from")
		to, _ := rec.Get("to")
		out = append(out, "rel "+typ.(string)+" "+render(props)+" "+render(from)+" -> "+render(to))
	}
	sort.Strings(out)
	return out
}

// leaseNode reports whether tenantID has an export lease node, and its
// holder.
func (f *graphFixture) leaseNode(tenantID string) (exists bool, holder any) {
	recs := f.query(tenantID, `MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'}) RETURN m.holder AS holder`)
	if len(recs) == 0 {
		return false, nil
	}
	holder, _ = recs[0].Get("holder")
	return true, holder
}

// write runs one unit-of-work write for tenantID that records message id as
// processed.
func (f *graphFixture) write(tenantID, id string) error {
	return f.uow.Write(context.Background(), uow.Marker{TenantID: tenantID, Group: "b3-test", MessageID: id}, true,
		func(context.Context, uow.Tx) error { return nil })
}

func (f *graphFixture) processed(tenantID, id string) bool {
	ok, err := f.uow.Processed(context.Background(), uow.Marker{TenantID: tenantID, Group: "b3-test", MessageID: id})
	require.NoError(f.t, err)
	return ok
}

// stored returns the keys of the exports the backup store holds for
// tenantID.
func (f *graphFixture) stored(tenantID string) []string {
	f.t.Helper()
	objs, err := f.store.List(context.Background(), tenantID)
	require.NoError(f.t, err)
	var keys []string
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	return keys
}

// records reads the export stored at key.
func (f *graphFixture) records(key string) (export.Header, []any) {
	f.t.Helper()
	src, err := f.store.OpenRead(context.Background(), key)
	require.NoError(f.t, err)
	defer func() { _ = src.Close() }()
	var recs []any
	for {
		rec, err := src.Next()
		if errors.Is(err, io.EOF) {
			return src.Header(), recs
		}
		require.NoError(f.t, err)
		recs = append(recs, rec)
	}
}

// exportKey is the key the backup store gives tenantID's export taken at at:
// tenants/<tenant>/backups/catalog/<UTC second>.jsonl.gz.
func exportKey(tenantID string, at time.Time) string {
	return "tenants/" + tenantID + "/backups/catalog/" + at.UTC().Format("20060102T150405Z") + ".jsonl.gz"
}

// putGzip stores body, gzipped, at key.
func (f *graphFixture) putGzip(key, body string) {
	f.t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write([]byte(body))
	require.NoError(f.t, err)
	require.NoError(f.t, zw.Close())
	require.NoError(f.t, f.mem.Put(context.Background(), objectstore.Ref{Key: key}, buf.Bytes(), "application/gzip"))
}

// gateStore wraps a BackupStore so the first record of the first export it
// opens waits: entered closes when that record arrives, and the write goes
// on once proceed is closed. The lease is held by then, since the export
// writes only after it took the lease.
type gateStore struct {
	ports.BackupStore
	entered chan struct{}
	proceed chan struct{}
	once    sync.Once
}

func newGateStore(store ports.BackupStore) *gateStore {
	return &gateStore{BackupStore: store, entered: make(chan struct{}), proceed: make(chan struct{})}
}

func (g *gateStore) Open(ctx context.Context, tenantID string, at time.Time, progress func()) (ports.ExportSink, string, error) {
	w, key, err := g.BackupStore.Open(ctx, tenantID, at, progress)
	if err != nil {
		return nil, "", err
	}
	return &gateSink{ExportSink: w, gate: g}, key, nil
}

type gateSink struct {
	ports.ExportSink
	gate   *gateStore
	passed bool
}

// WriteHeader is the first record of every export.
func (w *gateSink) WriteHeader(h export.Header) error {
	if !w.passed {
		w.passed = true
		first := false
		w.gate.once.Do(func() { first = true; close(w.gate.entered) })
		if first {
			<-w.gate.proceed
		}
	}
	return w.ExportSink.WriteHeader(h)
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// A write that starts while an export holds the tenant's lease backs off with
// ErrWritesPaused; once the export is stored the same write lands, and the
// export holds the graph as it was before the write.
func TestExporter_WritesPausedDuringExportAndLandAfter(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("paused")
	f.seed(tenantID)
	before := f.dump(tenantID)
	gate := newGateStore(f.store)
	ex := f.exporter(gate, &recordedMetrics{}, time.Now)

	type outcome struct {
		res backup.Result
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := ex.ExportTenant(context.Background(), tenantID)
		done <- outcome{res, err}
	}()
	waitFor(t, gate.entered, "the export to start writing")

	err := f.write(tenantID, "m-during")
	require.ErrorIs(t, err, uow.ErrWritesPaused, "a write during the export backs off")
	assert.False(t, f.processed(tenantID, "m-during"))

	close(gate.proceed)
	got := <-done
	require.NoError(t, got.err)

	require.NoError(t, f.write(tenantID, "m-during"), "the write lands once the export released the lease")
	assert.True(t, f.processed(tenantID, "m-during"))
	assert.Equal(t, []string{got.res.ObjectKey}, f.stored(tenantID), "the export is stored")

	h, recs := f.records(got.res.ObjectKey)
	assert.Equal(t, tenantID, h.TenantID)
	assert.Equal(t, int64(len(recs)), got.res.Nodes+got.res.Relationships)
	for _, rec := range recs {
		if n, ok := rec.(export.NodeRecord); ok {
			assert.NotContains(t, n.Labels, "ProcessedMessage", "the write that backed off is not in the export")
		}
	}
	assert.Equal(t, int64(len(before)), got.res.Nodes+got.res.Relationships, "the export holds the graph as it was")
}

// A lease left by an exporter that crashed (taken and never renewed or
// released) skips exports and pauses writes until it expires; then both go
// on.
func TestExporter_CrashedHolderLeaseExpires(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("crashed")
	f.seed(tenantID)
	require.NoError(t, f.lease.Acquire(context.Background(), tenantID, ports.LeaseExport, "export/crashed-pod/1", 1500*time.Millisecond))
	metrics := &recordedMetrics{}
	ex := f.exporter(f.store, metrics, time.Now)

	_, err := ex.ExportTenant(context.Background(), tenantID)
	require.ErrorIs(t, err, backup.ErrSkipped)
	require.ErrorIs(t, f.write(tenantID, "m-paused"), uow.ErrWritesPaused)
	assert.Empty(t, f.stored(tenantID))

	deadline := time.Now().Add(15 * time.Second)
	for {
		_, err = ex.ExportTenant(context.Background(), tenantID)
		if !errors.Is(err, backup.ErrSkipped) || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.NoError(t, err, "the crashed holder's lease expired")
	assert.Len(t, f.stored(tenantID), 1)
	require.NoError(t, f.write(tenantID, "m-paused"))
	assert.Equal(t, ports.ExportSuccess, metrics.seen()[len(metrics.seen())-1])
	assert.Equal(t, ports.ExportSkipped, metrics.seen()[0])
}

// While one export of a tenant runs, a second export of the same tenant is
// skipped: it stores no object and counts as skipped.
func TestExporter_SecondExportOfTheSameTenantSkips(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("second")
	f.seed(tenantID)
	gate := newGateStore(f.store)
	firstMetrics, secondMetrics := &recordedMetrics{}, &recordedMetrics{}
	first := f.exporter(gate, firstMetrics, time.Now)
	second := f.exporter(f.store, secondMetrics, time.Now)

	done := make(chan error, 1)
	go func() {
		_, err := first.ExportTenant(context.Background(), tenantID)
		done <- err
	}()
	waitFor(t, gate.entered, "the first export to start writing")

	_, err := second.ExportTenant(context.Background(), tenantID)
	require.ErrorIs(t, err, backup.ErrSkipped)
	assert.Equal(t, []string{ports.ExportSkipped}, secondMetrics.seen())
	assert.Empty(t, f.stored(tenantID), "the skipped export stores nothing")

	close(gate.proceed)
	require.NoError(t, <-done)
	assert.Len(t, f.stored(tenantID), 1, "only the first export is stored")
	assert.Equal(t, []string{ports.ExportSuccess}, firstMetrics.seen())
}

// A store that refuses the upload fails the export before the lease: the
// tenant's lease node is never touched, so its writes are never paused.
func TestExporter_StoreRefusesOpenBeforeTheLease(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("refused")
	f.seed(tenantID)
	at := time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC)
	f.mem.FailPut(objectstore.Ref{Key: exportKey(tenantID, at)}, errors.New("AccessDenied"))
	metrics := &recordedMetrics{}
	ex := f.exporter(f.store, metrics, func() time.Time { return at })

	_, err := ex.ExportTenant(context.Background(), tenantID)
	require.ErrorContains(t, err, "AccessDenied")
	exists, _ := f.leaseNode(tenantID)
	assert.False(t, exists, "no lease was taken: the lease node was never created")
	assert.Equal(t, []string{ports.ExportFailure}, metrics.seen())
	require.NoError(t, f.write(tenantID, "m-after"), "a write is not paused")
}

// A cancel while the export uploads (a shutdown) leaves no object and no
// held lease: the next export and the next write go on at once.
func TestExporter_ACancelDuringTheUploadLeavesNoObjectAndNoHeldLease(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("cancelled")
	f.seed(tenantID)
	gate := newGateStore(f.store)
	ex := f.exporter(gate, &recordedMetrics{}, time.Now)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := ex.ExportTenant(ctx, tenantID)
		done <- err
	}()
	waitFor(t, gate.entered, "the export to start writing")
	cancel()
	close(gate.proceed)
	require.ErrorIs(t, <-done, context.Canceled)

	assert.Empty(t, f.stored(tenantID), "a cancelled export stores nothing")
	_, holder := f.leaseNode(tenantID)
	assert.Nil(t, holder, "the lease was released")
	require.NoError(t, f.write(tenantID, "m-after"))
	_, err := f.exporter(f.store, &recordedMetrics{}, time.Now).ExportTenant(context.Background(), tenantID)
	require.NoError(t, err)
}

// A tenant that holds no node but its lease node exports to a header alone,
// which a restore accepts.
func TestExporter_ExportsAnEmptyTenant(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("empty")
	res, err := f.exporter(f.store, &recordedMetrics{}, time.Now).ExportTenant(context.Background(), tenantID)
	require.NoError(t, err)
	assert.Zero(t, res.Nodes)
	assert.Zero(t, res.Relationships)
	h, recs := f.records(res.ObjectKey)
	assert.Empty(t, recs)
	assert.Empty(t, h.Nodes)
	_, err = f.restorer().Restore(context.Background(), res.ObjectKey)
	require.NoError(t, err)
	assert.Empty(t, f.dump(tenantID))
}

// A file the restore cannot load exactly (a label outside the schema, counts
// its records disagree with, another tenant's header) is refused before the
// restore takes the lease or wipes: the tenant's graph and its lease are
// untouched.
func TestRestorer_ValidatesBeforeItWipes(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("validates")
	f.seed(tenantID)
	before := f.dump(tenantID)
	header := func(tenant, nodes string) string {
		return fmt.Sprintf(`{"format_version":1,"tenant_id":%q,"exported_at":"2026-10-30T02:00:00Z","catalog_version":"x","nodes":%s,"relationships":{}}`, tenant, nodes)
	}
	nodeLine := func(tenant, label, uid string) string {
		return fmt.Sprintf(`{"node":[%q],"props":{"tenant_id":%q,"unique_id":%q}}`, label, tenant, uid)
	}
	cases := map[string]string{
		"a label outside the schema": header(tenantID, `{"Evil":1}`) + "\n" + nodeLine(tenantID, "Evil", "x") + "\n",
		"a count mismatch":           header(tenantID, `{"Node":3}`) + "\n" + nodeLine(tenantID, "Node", "a") + "\n" + nodeLine(tenantID, "Node", "b") + "\n",
		"another tenant's header":    header("someone-else", `{"Node":1}`) + "\n" + nodeLine("someone-else", "Node", "a") + "\n",
	}
	i := 0
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			i++
			key := exportKey(tenantID, time.Date(2026, 10, 30, 2, 0, i, 0, time.UTC))
			f.putGzip(key, body)

			_, err := f.restorer().Restore(context.Background(), key)
			require.Error(t, err)
			assert.NotErrorIs(t, err, backup.ErrPartialRestore)
			assert.Equal(t, before, f.dump(tenantID), "the graph is untouched")
			exists, _ := f.leaseNode(tenantID)
			assert.False(t, exists, "the lease was never taken")
		})
	}
}

// A restore returns the tenant's graph to the export: everything written
// after it is gone, everything it held is back, and the lease is free.
func TestRestorer_WipesLoadsAndReleases(t *testing.T) {
	f := newGraphFixture(t)
	tenantID := f.tenant("restores")
	bystander := f.tenant("bystander")
	f.seed(tenantID)
	f.seed(bystander)
	g := f.dump(tenantID)
	others := f.dump(bystander)

	res, err := f.exporter(f.store, &recordedMetrics{}, time.Now).ExportTenant(context.Background(), tenantID)
	require.NoError(t, err)

	f.exec(tenantID, `MATCH (n:Node {tenant_id: $tenant_id, unique_id: 'svc.a'}) SET n.active = false, n.extra = 'changed'`, nil)
	f.exec(tenantID, `MATCH (:Node {tenant_id: $tenant_id, unique_id: 'svc.b'})-[d:DEPENDS_ON]->() DELETE d`, nil)
	f.exec(tenantID, `CREATE (:Node {tenant_id: $tenant_id, unique_id: 'svc.c', active: true})`, nil)
	require.NoError(t, f.write(tenantID, "m-after-export"))
	require.NotEqual(t, g, f.dump(tenantID))

	got, err := f.restorer().Restore(context.Background(), res.ObjectKey)
	require.NoError(t, err)
	assert.Equal(t, backup.RestoreResult{Nodes: res.Nodes, Relationships: res.Relationships}, got)
	assert.Equal(t, g, f.dump(tenantID), "the graph is the exported one again")
	assert.Equal(t, others, f.dump(bystander), "another tenant's graph is untouched")
	_, holder := f.leaseNode(tenantID)
	assert.Nil(t, holder, "the lease is free")
	require.NoError(t, f.write(tenantID, "m-after-restore"), "writes go on after the restore")
}

// countingObjects is an object store whose writers count the bytes they are
// given and keep none, so the production backup store over it measures what
// an export itself holds: the codec, the compressor and the records in
// flight. It lists nothing and reads nothing.
type countingObjects struct{ written atomic.Int64 }

func (s *countingObjects) OpenWriter(context.Context, objectstore.Ref, string) (objectstore.ObjectWriter, error) {
	return &countingWriter{objects: s}, nil
}
func (s *countingObjects) GetStream(context.Context, objectstore.Ref) (io.ReadCloser, error) {
	return nil, errors.New("countingObjects keeps nothing")
}
func (s *countingObjects) List(context.Context, string) ([]objectstore.Object, error) {
	return nil, nil
}
func (s *countingObjects) Delete(context.Context, objectstore.Ref) error { return nil }

type countingWriter struct{ objects *countingObjects }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.objects.written.Add(int64(len(p)))
	return len(p), nil
}
func (w *countingWriter) Close() error { return nil }
func (w *countingWriter) Abort() error { return nil }

// seedSynthetic writes n :Node nodes of tenantID, each with payload bytes of
// random text, and a :DEPENDS_ON chain through them.
func (f *graphFixture) seedSynthetic(tenantID string, n, payload int) {
	f.t.Helper()
	const batch = 250
	raw := make([]byte, payload*3/4)
	for start := 0; start < n; start += batch {
		rows := make([]map[string]any, 0, batch)
		for i := start; i < min(start+batch, n); i++ {
			_, _ = rand.Read(raw)
			rows = append(rows, map[string]any{"uid": fmt.Sprintf("mem.n%06d", i), "payload": base64.StdEncoding.EncodeToString(raw)})
		}
		f.exec(tenantID, `
UNWIND $rows AS row
CREATE (:Node {tenant_id: $tenant_id, unique_id: row.uid, active: true, payload: row.payload})`, map[string]any{"rows": rows})
	}
	f.exec(tenantID, `
MATCH (a:Node {tenant_id: $tenant_id}) WITH a ORDER BY a.unique_id
WITH collect(a) AS ns
UNWIND range(1, size(ns) - 1) AS i
WITH ns[i] AS a, ns[i - 1] AS b
CREATE (a)-[:DEPENDS_ON]->(b)`, nil)
}

// peakLiveHeapDuring runs fn and returns the largest live heap sampled every
// 10 ms while it ran. Live heap is what the last collection found reachable, so
// it leaves out garbage that has not been collected yet and the peak does not
// depend on when the collector runs; collecting at GOGC=10 keeps that
// measurement current, so a burst of allocation is seen before it is dropped.
func peakLiveHeapDuring(fn func()) uint64 {
	defer debug.SetGCPercent(debug.SetGCPercent(10))
	runtime.GC()
	var peak atomic.Uint64
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	read := func() {
		metrics.Read(sample)
		if sample[0].Value.Kind() != metrics.KindUint64 {
			return
		}
		live := sample[0].Value.Uint64()
		for {
			old := peak.Load()
			if live <= old || peak.CompareAndSwap(old, live) {
				return
			}
		}
	}
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			read()
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()
	fn()
	close(stop)
	<-sampled
	read()
	return peak.Load()
}

// An export holds one fetch page of records and the compressor's window,
// whatever the size of the graph: exporting a graph ten times larger leaves
// the peak heap within twice the smaller export's peak plus 16 MiB. The
// object store under the production backup store counts bytes and keeps
// none, so only the export's own memory is measured.
func TestExport_MemoryDoesNotGrowWithTheCatalog(t *testing.T) {
	const (
		n       = 2000
		payload = 6 << 10
	)
	f := newGraphFixture(t)
	small, large := f.tenant("mem-small"), f.tenant("mem-large")
	f.seedSynthetic(small, n, payload)
	f.seedSynthetic(large, 10*n, payload)

	measure := func(tenantID string) (uint64, backup.Result, int64) {
		objects := &countingObjects{}
		ex := f.exporter(s3infra.NewBackupStore(objects), &recordedMetrics{}, time.Now)
		var res backup.Result
		var err error
		peak := peakLiveHeapDuring(func() { res, err = ex.ExportTenant(context.Background(), tenantID) })
		require.NoError(t, err)
		require.Equal(t, objects.written.Load(), res.Bytes, "the export reports the bytes the store took")
		return peak, res, objects.written.Load()
	}
	peakSmall, resSmall, bytesSmall := measure(small)
	peakLarge, resLarge, bytesLarge := measure(large)

	const mib = 1 << 20
	t.Logf("N=%d nodes (%d records, %d MiB written): peak heap %.1f MiB; 10N=%d nodes (%d records, %d MiB written): peak heap %.1f MiB; bound %.1f MiB",
		n, resSmall.Nodes+resSmall.Relationships, bytesSmall/mib, float64(peakSmall)/mib,
		10*n, resLarge.Nodes+resLarge.Relationships, bytesLarge/mib, float64(peakLarge)/mib, float64(2*peakSmall+16*mib)/mib)
	bound := 2*peakSmall + 16*mib
	require.Equal(t, int64(10*n), resLarge.Nodes)
	require.Greater(t, bytesLarge, 9*bytesSmall, "the larger export writes about ten times the bytes")
	require.Greater(t, bytesLarge, int64(64*mib),
		"the larger export writes far more than the bound's slack, so an export that held it whole would break the bound")
	assert.LessOrEqual(t, peakLarge, bound,
		"the peak heap of an export ten times larger stays within twice the smaller one's plus 16 MiB")
}

// An export started between two batches of one message waits for the message's
// last batch, so the stored file holds all of the message's data and its
// processed-message marker.
func TestExport_WaitsForAMessageBetweenBatches(t *testing.T) {
	ctx := context.Background()
	f := newGraphFixture(t)
	tenantID := f.tenant("inflight")
	marker := uow.Marker{TenantID: tenantID, Group: "b3-test", MessageID: "1762077600000-0"}
	batch := func(name string, final bool) {
		f.exec(tenantID, `CREATE (:Node {tenant_id: $tenant_id, unique_id: $name, active: true})`, map[string]any{"name": name})
		require.NoError(t, f.uow.Write(ctx, marker, final, func(context.Context, uow.Tx) error { return nil }))
	}

	batch("batch-1", false)
	ex := f.exporter(f.store, &recordedMetrics{}, time.Now)
	done := make(chan error, 1)
	go func() {
		_, err := ex.ExportTenant(ctx, tenantID)
		done <- err
	}()

	time.Sleep(500 * time.Millisecond)
	_, holder := f.leaseNode(tenantID)
	assert.Nil(t, holder, "the export waits and takes no lease between the message's batches")
	select {
	case err := <-done:
		t.Fatalf("the export ended between the message's batches: %v", err)
	default:
	}

	batch("batch-2", true)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the export did not go ahead after the message's last batch")
	}

	keys := f.stored(tenantID)
	require.Len(t, keys, 1)
	_, recs := f.records(keys[0])
	var probes []string
	markers := 0
	for _, rec := range recs {
		n, ok := rec.(export.NodeRecord)
		if !ok {
			continue
		}
		if slices.Contains(n.Labels, "Node") {
			probes = append(probes, n.Props["unique_id"].(string))
		}
		if slices.Contains(n.Labels, "ProcessedMessage") {
			markers++
		}
	}
	assert.ElementsMatch(t, []string{"batch-1", "batch-2"}, probes, "the export holds both of the message's batches")
	assert.Equal(t, 1, markers, "the export holds the message's marker")
}
