package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

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
	// wipeTakes is how long Wipe runs; perRecord how long Import spends on
	// each record it reads.
	wipeTakes, perRecord time.Duration
	read                 []any
}

func (g *fakeImporter) Wipe(_ context.Context, tenantID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ops = append(g.ops, "Wipe "+tenantID)
	time.Sleep(g.wipeTakes)
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
		time.Sleep(g.perRecord)
	}
	return g.importErr
}

func (g *fakeImporter) calls() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.ops...)
}

type restorerFixture struct {
	lease *fakeLease
	graph *fakeImporter
	store *fakeStore
	r     *Restorer
	key   string
}

// newRestorerFixture is a Restorer over fakes; key names the tenant's export
// taken at exportAt, which the test stores (or not).
func newRestorerFixture() *restorerFixture {
	f := &restorerFixture{lease: newFakeLease(), graph: &fakeImporter{}, store: newFakeStore()}
	f.r = NewRestorer(f.lease, f.graph, f.store, quietLogger())
	f.key = fakeKey(tenant, exportAt)
	return f
}

// putExport stores at f.key an export holding n :Node nodes and a chain of
// n-1 relationships.
func (f *restorerFixture) putExport(n int) {
	f.store.put(tenant, exportAt, n)
}

// A restore reads the export twice: once to validate it whole, with no lease
// and no write, then, under the restore lease, to load it into the wiped
// graph. It returns the records it loaded, closes what it opened and
// releases the lease.
func TestRestorer_ReadsTheExportTwiceAndLoadsIt(t *testing.T) {
	f := newRestorerFixture()
	f.putExport(3)
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
	require.Len(t, f.store.sources, 1, "the second read opens the export")
	assert.True(t, f.store.sources[0].closed, "and closes it")
	assert.Equal(t, []string{"Wipe t1", "Import t1"}, f.graph.calls())
	assert.Len(t, f.graph.read, 5)
	acquire := f.lease.callsOf("Acquire")
	require.Len(t, acquire, 1)
	assert.Equal(t, ports.LeaseRestore, acquire[0].Kind)
	assert.Equal(t, leaseTTL, acquire[0].TTL)
	assert.Equal(t, "Release", f.lease.ops()[len(f.lease.ops())-1])
	assert.False(t, f.lease.isHeld(tenant))
}

// The tenant is the one the store locates the key in, and a key the store
// does not locate reads nothing and takes no lease.
func TestRestorer_RefusesAKeyThatIsNotAnExport(t *testing.T) {
	for _, key := range []string{"", "t1", "t1@latest"} {
		f := newRestorerFixture()
		_, err := f.r.Restore(context.Background(), key)
		require.ErrorIs(t, err, ports.ErrNotAnExportKey, key)
		assert.Empty(t, f.store.reads, key)
		assert.Empty(t, f.lease.ops(), key)
	}
}

// An export the store's validation refuses, or one of another tenant than the
// key's, is refused before the lease and before any wipe.
func TestRestorer_RefusesAnExportItCannotLoad(t *testing.T) {
	cases := map[string]func(f *restorerFixture){
		"refused by validation": func(f *restorerFixture) {
			f.putExport(2)
			f.store.validateErr = errors.New("exportfile: malformed export line")
		},
		"another tenant's export": func(f *restorerFixture) {
			h := header("t2", 0, 0)
			f.store.putExport(f.key, storedExport{header: h})
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			f := newRestorerFixture()
			setup(f)
			_, err := f.r.Restore(context.Background(), f.key)
			require.Error(t, err)
			assert.ErrorContains(t, err, "untouched")
			assert.NotErrorIs(t, err, ErrPartialRestore)
			assert.Empty(t, f.lease.ops(), "no lease is taken")
			assert.Empty(t, f.graph.calls(), "nothing is wiped")
		})
	}
}

// A lease that cannot be taken for another reason than another holder fails
// the restore before the wipe, and the restore releases the lease under its
// own holder, in case the Acquire committed while its answer was lost.
func TestRestorer_AcquireFailureWipesNothingAndReleases(t *testing.T) {
	f := newRestorerFixture()
	f.putExport(2)
	f.lease.acquireCommitsThenFails = errors.New("neo4j: connection reset after commit")

	_, err := f.r.Restore(context.Background(), f.key)
	require.ErrorContains(t, err, "connection reset")
	assert.NotErrorIs(t, err, ErrSkipped)
	assert.NotErrorIs(t, err, ErrPartialRestore)
	assert.Empty(t, f.graph.calls(), "nothing is wiped")
	assert.Equal(t, []string{"Acquire", "Release"}, f.lease.ops())
	assert.Equal(t, f.lease.callsOf("Acquire")[0].Holder, f.lease.callsOf("Release")[0].Holder)
	assert.False(t, f.lease.isHeld(tenant), "the lease the failed Acquire took is released")
}

func TestRestorer_AMissingExportIsNotFound(t *testing.T) {
	f := newRestorerFixture()
	_, err := f.r.Restore(context.Background(), f.key)
	require.ErrorIs(t, err, ports.ErrBackupNotFound)
	assert.Empty(t, f.graph.calls())
}

// A restore while another export or restore holds the lease is skipped and
// wipes nothing.
func TestRestorer_SkipsWhileAnotherHoldsTheLease(t *testing.T) {
	f := newRestorerFixture()
	f.putExport(2)
	require.NoError(t, f.lease.Acquire(context.Background(), tenant, ports.LeaseExport, "export/elsewhere", time.Minute))

	_, err := f.r.Restore(context.Background(), f.key)
	require.ErrorIs(t, err, ErrSkipped)
	assert.Empty(t, f.graph.calls())
	assert.True(t, f.lease.isHeld(tenant))
}

// An export that vanishes, or changes, between the validating read and the
// loading read is caught before the wipe: the graph stays as it was.
func TestRestorer_AnObjectThatChangesBeforeTheWipeIsNotLoaded(t *testing.T) {
	t.Run("deleted", func(t *testing.T) {
		f := newRestorerFixture()
		f.putExport(2)
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
		f := newRestorerFixture()
		f.putExport(2)
		f.store.readErr = func(n int) error {
			if n == 2 {
				f.store.exports[f.key] = storedExport{header: header(tenant, 4, 3)}
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
			f := newRestorerFixture()
			f.putExport(2)
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
	f := newRestorerFixture()
	f.putExport(2)
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
	f := newRestorerFixture()
	f.putExport(2)
	f.lease.renewErr = func(int) error { return fmt.Errorf("tenant %q: %w", tenant, ports.ErrLeaseLost) }

	_, err := f.r.Restore(context.Background(), f.key)
	require.ErrorIs(t, err, ports.ErrLeaseLost)
	assert.ErrorContains(t, err, "run the restore again")
}

// A restore that moves no record for the stall limit (an import
// against a database that never answers) is stopped: it fails with
// ErrStalled, reports the graph partial and releases the lease.
func TestRestorer_AStalledRestoreFailsWithinTheBound(t *testing.T) {
	f := newRestorerFixture()
	f.putExport(2)
	f.r.keeper.renewEvery = 5 * time.Millisecond
	f.r.keeper.stallAfter = 50 * time.Millisecond
	f.graph.onImport = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	_, err := f.r.Restore(ctx, f.key)
	require.ErrorIs(t, err, ErrStalled)
	require.ErrorIs(t, err, ErrPartialRestore)
	assert.NotErrorIs(t, err, context.DeadlineExceeded, "the restore ended on its own, not at the test's deadline")
	assert.Less(t, time.Since(start), time.Second)
	assert.False(t, f.lease.isHeld(tenant), "the lease is released")
	assert.NoError(t, f.lease.releaseCtxErrs[len(f.lease.releaseCtxErrs)-1])
}

// Each step of a restore starts the stall limit afresh: the wipe and the
// start of the import move no record, and neither stalls the restore while
// it stays within the limit; records read keep the import going.
func TestRestorer_EachStepStartsTheStallLimitAfresh(t *testing.T) {
	f := newRestorerFixture()
	f.putExport(5)
	f.r.keeper.renewEvery = 5 * time.Millisecond
	f.r.keeper.stallAfter = 100 * time.Millisecond
	f.graph.wipeTakes = 60 * time.Millisecond
	f.graph.onImport = func(context.Context) error {
		time.Sleep(60 * time.Millisecond)
		return nil
	}
	f.graph.perRecord = 40 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	res, err := f.r.Restore(ctx, f.key)
	require.NoError(t, err)
	assert.Equal(t, RestoreResult{Nodes: 5, Relationships: 4}, res)
	assert.Greater(t, time.Since(start), 3*f.r.keeper.stallAfter, "the restore ran longer than the stall limit")
}
