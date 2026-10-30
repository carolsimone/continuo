package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/serialization/exportfile"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeImporter is a GraphImporter that records its calls. Import reads every
// record; wipeErr and importErr, when set, are what those calls return.
type fakeImporter struct {
	mu        sync.Mutex
	ops       []string
	wipeErr   error
	importErr error
	// onImport, when set, runs before Import reads the records.
	onImport func(ctx context.Context) error
	read     []any
}

func (g *fakeImporter) Wipe(_ context.Context, tenantID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ops = append(g.ops, "Wipe "+tenantID)
	return g.wipeErr
}

func (g *fakeImporter) Import(ctx context.Context, tenantID string, in ports.RecordReader) error {
	g.mu.Lock()
	g.ops = append(g.ops, "Import "+tenantID)
	g.mu.Unlock()
	if g.onImport != nil {
		if err := g.onImport(ctx); err != nil {
			return err
		}
	}
	if in.Header().TenantID != tenantID {
		return errors.New("fakeImporter: another tenant's header")
	}
	for {
		rec, err := in.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		g.mu.Lock()
		g.read = append(g.read, rec)
		g.mu.Unlock()
	}
	return g.importErr
}

func (g *fakeImporter) calls() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.ops...)
}

// exportObject is a gzipped export of tenantID holding n :Node nodes and a
// chain of n-1 relationships; exportedAt sets its header's time.
func exportObject(t *testing.T, tenantID string, n int, exportedAt time.Time) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	w := exportfile.NewWriter(zw)
	h := header(tenantID, int64(n), int64(max(n-1, 0)))
	h.ExportedAt = exportedAt
	require.NoError(t, w.WriteHeader(h))
	for i := range n {
		require.NoError(t, w.WriteNode(node(tenantID, i)))
	}
	for i := 1; i < n; i++ {
		require.NoError(t, w.WriteRel(dependsOn(i, i-1)))
	}
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

type restorerFixture struct {
	lease *fakeLease
	graph *fakeImporter
	store *fakeStore
	r     *Restorer
	key   string
}

func newRestorerFixture(t *testing.T) *restorerFixture {
	f := &restorerFixture{lease: newFakeLease(), graph: &fakeImporter{}, store: newFakeStore()}
	f.r = NewRestorer(f.lease, f.graph, f.store, quietLogger())
	key, err := export.ObjectKey(tenant, exportAt)
	require.NoError(t, err)
	f.key = key
	return f
}

// A restore reads the object twice: once to validate it whole, with no lease
// and no write, then, under the restore lease, to load it into the wiped
// graph. It returns the records it loaded and releases the lease.
func TestRestorer_StreamsTheObjectTwiceAndLoadsIt(t *testing.T) {
	f := newRestorerFixture(t)
	f.store.put(f.key, exportObject(t, tenant, 3, exportAt))
	f.store.readErr = func(n int) error {
		if n == 1 {
			assert.Empty(t, f.lease.ops(), "the first read takes no lease")
			assert.Empty(t, f.graph.calls(), "the first read writes nothing")
		}
		return nil
	}

	res, err := f.r.Restore(context.Background(), f.key)
	require.NoError(t, err)
	assert.Equal(t, RestoreResult{Nodes: 3, Relationships: 2}, res)
	assert.Equal(t, []string{f.key, f.key}, f.store.reads)
	assert.Equal(t, []string{"Wipe t1", "Import t1"}, f.graph.calls())
	assert.Len(t, f.graph.read, 5)
	acquire := f.lease.callsOf("Acquire")
	require.Len(t, acquire, 1)
	assert.Equal(t, ports.LeaseRestore, acquire[0].Kind)
	assert.Equal(t, leaseTTL, acquire[0].TTL)
	assert.Equal(t, "Release", f.lease.ops()[len(f.lease.ops())-1])
	assert.False(t, f.lease.isHeld(tenant))
}

// The tenant comes from the key, and a key that is not an export's reads
// nothing and takes no lease.
func TestRestorer_RefusesAKeyThatIsNotAnExport(t *testing.T) {
	for _, key := range []string{"", "tenants/t1/topologies/r1/topology.json.gz", "tenants/t1/backups/catalog/latest.jsonl.gz"} {
		f := newRestorerFixture(t)
		_, err := f.r.Restore(context.Background(), key)
		require.ErrorIs(t, err, export.ErrNotAnExportKey, key)
		assert.Empty(t, f.store.reads, key)
		assert.Empty(t, f.lease.ops(), key)
	}
}

// A file that does not decompress or is not a valid export of the key's
// tenant is refused before the lease and before any wipe.
func TestRestorer_RefusesAnObjectItCannotLoad(t *testing.T) {
	good := exportObject(t, tenant, 2, exportAt)
	cases := map[string][]byte{
		"not gzip":              []byte("{\"format_version\":1}\n"),
		"cut short":             good[:len(good)-6],
		"another tenant's file": exportObject(t, "t2", 2, exportAt),
		"a corrupt trailer": func() []byte {
			b := bytes.Clone(good)
			b[len(b)-8] ^= 0xff // the CRC-32 of the gzip trailer
			return b
		}(),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			f := newRestorerFixture(t)
			f.store.put(f.key, body)
			_, err := f.r.Restore(context.Background(), f.key)
			require.Error(t, err)
			assert.NotErrorIs(t, err, ErrPartialRestore)
			assert.Empty(t, f.lease.ops(), "no lease is taken")
			assert.Empty(t, f.graph.calls(), "nothing is wiped")
		})
	}
}

func TestRestorer_AMissingObjectIsNotFound(t *testing.T) {
	f := newRestorerFixture(t)
	_, err := f.r.Restore(context.Background(), f.key)
	require.ErrorIs(t, err, ports.ErrBackupNotFound)
	assert.Empty(t, f.graph.calls())
}

// A restore while another export or restore holds the lease is skipped and
// wipes nothing.
func TestRestorer_SkipsWhileAnotherHoldsTheLease(t *testing.T) {
	f := newRestorerFixture(t)
	f.store.put(f.key, exportObject(t, tenant, 2, exportAt))
	require.NoError(t, f.lease.Acquire(context.Background(), tenant, ports.LeaseExport, "export/elsewhere", time.Minute))

	_, err := f.r.Restore(context.Background(), f.key)
	require.ErrorIs(t, err, ErrSkipped)
	assert.Empty(t, f.graph.calls())
	assert.True(t, f.lease.isHeld(tenant))
}

// An object that vanishes, or changes, between the validating read and the
// loading read is caught before the wipe: the graph stays as it was.
func TestRestorer_AnObjectThatChangesBeforeTheWipeIsNotLoaded(t *testing.T) {
	t.Run("deleted", func(t *testing.T) {
		f := newRestorerFixture(t)
		f.store.put(f.key, exportObject(t, tenant, 2, exportAt))
		f.store.readErr = func(n int) error {
			if n == 2 {
				return fmt.Errorf("%w: %s", ports.ErrBackupNotFound, f.key)
			}
			return nil
		}
		_, err := f.r.Restore(context.Background(), f.key)
		require.ErrorIs(t, err, ports.ErrBackupNotFound)
		assert.NotErrorIs(t, err, ErrPartialRestore)
		assert.Empty(t, f.graph.calls(), "nothing is wiped")
		assert.False(t, f.lease.isHeld(tenant))
	})
	t.Run("replaced", func(t *testing.T) {
		f := newRestorerFixture(t)
		f.store.put(f.key, exportObject(t, tenant, 2, exportAt))
		f.store.readErr = func(n int) error {
			if n == 2 {
				f.store.objects[f.key] = exportObject(t, tenant, 4, exportAt.Add(time.Second))
			}
			return nil
		}
		_, err := f.r.Restore(context.Background(), f.key)
		require.ErrorContains(t, err, "changed")
		assert.NotErrorIs(t, err, ErrPartialRestore)
		assert.Empty(t, f.graph.calls(), "nothing is wiped")
		assert.False(t, f.lease.isHeld(tenant))
	})
}

// An import that fails after the wipe leaves a partial graph that has not
// been migrated: the error says so and tells the operator to run the restore
// again, and the lease is released.
func TestRestorer_AFailedLoadReportsAPartialGraph(t *testing.T) {
	for name, fail := range map[string]func(g *fakeImporter){
		"import": func(g *fakeImporter) { g.importErr = errors.New("neo4j: connection reset") },
		"wipe":   func(g *fakeImporter) { g.wipeErr = errors.New("neo4j: connection reset") },
	} {
		t.Run(name, func(t *testing.T) {
			f := newRestorerFixture(t)
			f.store.put(f.key, exportObject(t, tenant, 2, exportAt))
			fail(f.graph)

			_, err := f.r.Restore(context.Background(), f.key)
			require.ErrorIs(t, err, ErrPartialRestore)
			assert.ErrorContains(t, err, "connection reset")
			assert.ErrorContains(t, err, "partial")
			assert.ErrorContains(t, err, "schema migration has not run")
			assert.ErrorContains(t, err, "run the restore again")
			assert.False(t, f.lease.isHeld(tenant), "the lease is released")
		})
	}
}

// A lease lost while the export loads stops the restore and reports the
// graph partial.
func TestRestorer_LostLeaseStopsTheRestore(t *testing.T) {
	f := newRestorerFixture(t)
	f.store.put(f.key, exportObject(t, tenant, 2, exportAt))
	f.r.keeper.renewEvery = 10 * time.Millisecond
	f.lease.renewErr = func(int) error { return fmt.Errorf("tenant %q: %w", tenant, ports.ErrLeaseLost) }
	f.graph.onImport = func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return errors.New("the restore was not stopped")
		}
	}

	_, err := f.r.Restore(context.Background(), f.key)
	require.ErrorIs(t, err, ErrPartialRestore)
	require.ErrorIs(t, err, ports.ErrLeaseLost)
	assert.Equal(t, "Release", f.lease.ops()[len(f.lease.ops())-1])
}

// A load that finished after its lease lapsed may hold writes that landed
// while it ran: the restore confirms the lease after the import and fails
// when it is gone.
func TestRestorer_ConfirmsTheLeaseAfterTheLoad(t *testing.T) {
	f := newRestorerFixture(t)
	f.store.put(f.key, exportObject(t, tenant, 2, exportAt))
	f.lease.renewErr = func(int) error { return fmt.Errorf("tenant %q: %w", tenant, ports.ErrLeaseLost) }

	_, err := f.r.Restore(context.Background(), f.key)
	require.ErrorIs(t, err, ports.ErrLeaseLost)
	assert.ErrorContains(t, err, "run the restore again")
}
