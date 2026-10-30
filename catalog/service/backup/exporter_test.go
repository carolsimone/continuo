package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/serialization/exportfile"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const tenant = "t1"

var exportAt = time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC)

// exporterFixture is an Exporter over fakes, with a clock at exportAt.
type exporterFixture struct {
	lease   *fakeLease
	store   *fakeStore
	metrics *fakeMetrics
	clock   *fakeClock
	ex      *Exporter
}

func newExporterFixture(graph ports.GraphExporter, keep int) *exporterFixture {
	f := &exporterFixture{lease: newFakeLease(), store: newFakeStore(), metrics: &fakeMetrics{}, clock: newFakeClock(exportAt)}
	f.ex = NewExporter(f.lease, graph, f.store, f.metrics, keep, f.clock.Now, quietLogger())
	return f
}

func gunzip(t *testing.T, body []byte) []byte {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(body))
	require.NoError(t, err)
	out, err := io.ReadAll(zr)
	require.NoError(t, err)
	return out
}

// An export opens its object before it takes the lease, writes the graph as
// gzipped JSON lines, stores it, releases the lease and reports its numbers.
func TestExporter_ExportsTheGraphAsGzippedJSONLines(t *testing.T) {
	f := newExporterFixture(smallGraph(3), 7)
	f.lease.onAcquire = func() {
		assert.NotNil(t, f.store.lastWriter(), "the object is opened before the lease is taken")
	}

	res, err := f.ex.ExportTenant(context.Background(), tenant)
	require.NoError(t, err)

	key := "tenants/t1/backups/catalog/20261030T020000Z.jsonl.gz"
	assert.Equal(t, key, res.ObjectKey)
	body, ok := f.store.object(key)
	require.True(t, ok, "the object is stored")
	assert.Equal(t, int64(len(body)), res.Bytes, "Bytes is the stored object's size")
	assert.Equal(t, int64(3), res.Nodes)
	assert.Equal(t, int64(2), res.Relationships)
	h, err := exportfile.Validate(bytes.NewReader(gunzip(t, body)), tenant)
	require.NoError(t, err, "the object is a valid export of the tenant")
	assert.Equal(t, map[string]int64{"Node": 3}, h.Nodes)

	assert.Equal(t, []string{"Acquire", "Renew", "Release"}, f.lease.ops(),
		"the lease is taken, confirmed after the read, and released")
	acquire := f.lease.callsOf("Acquire")[0]
	assert.Equal(t, ports.LeaseExport, acquire.Kind)
	assert.Equal(t, leaseTTL, acquire.TTL)
	assert.Equal(t, acquire.Holder, f.lease.callsOf("Release")[0].Holder)
	assert.False(t, f.lease.isHeld(tenant))

	assert.Equal(t, observation{Outcome: ports.ExportSuccess, Bytes: res.Bytes, Nodes: 3, Rels: 2}, f.metrics.last())
}

func TestExporter_LeaseConstants(t *testing.T) {
	assert.Equal(t, 2*time.Minute, leaseTTL)
	assert.Equal(t, 30*time.Second, leaseRenewEvery)
	assert.Equal(t, leaseTTL, stallLimit, "a stuck holder pauses writes no longer than a crashed one")
	assert.Less(t, renewTimeout, leaseRenewEvery)
	ex := NewExporter(newFakeLease(), smallGraph(1), newFakeStore(), &fakeMetrics{}, 7, time.Now, quietLogger())
	assert.Equal(t, leaseTTL, ex.keeper.ttl)
	assert.Equal(t, leaseRenewEvery, ex.keeper.renewEvery)
	assert.Equal(t, stallLimit, ex.keeper.stallAfter)
	assert.Equal(t, renewTimeout, ex.keeper.renewTimeout)
	r := NewRestorer(newFakeLease(), &fakeImporter{}, newFakeStore(), quietLogger())
	assert.Equal(t, ex.keeper.stallAfter, r.keeper.stallAfter)
}

// Each export takes the lease under a holder of its own, so a renewal of one
// export can never extend another's.
func TestExporter_EachExportHoldsTheLeaseUnderItsOwnHolder(t *testing.T) {
	f := newExporterFixture(smallGraph(1), 7)
	for range 2 {
		_, err := f.ex.ExportTenant(context.Background(), tenant)
		require.NoError(t, err)
		f.clock.Advance(time.Hour)
	}
	acquires := f.lease.callsOf("Acquire")
	require.Len(t, acquires, 2)
	assert.NotEqual(t, acquires[0].Holder, acquires[1].Holder)
	assert.True(t, strings.HasPrefix(acquires[0].Holder, "export/"), acquires[0].Holder)
}

// A graph error in the middle of the stream aborts the upload and releases
// the lease: nothing is stored, and retention does not run.
func TestExporter_FailureAbortsAndReleases(t *testing.T) {
	boom := errors.New("neo4j went away")
	graph := fakeGraph{export: func(_ context.Context, tenantID string, out ports.RecordWriter) error {
		if err := out.WriteHeader(header(tenantID, 5, 0)); err != nil {
			return err
		}
		for i := range 2 {
			if err := out.WriteNode(node(tenantID, i)); err != nil {
				return err
			}
		}
		return boom
	}}
	f := newExporterFixture(graph, 1)
	older, _ := export.ObjectKey(tenant, exportAt.Add(-48*time.Hour))
	oldest, _ := export.ObjectKey(tenant, exportAt.Add(-72*time.Hour))
	f.store.put(older, []byte("x"))
	f.store.put(oldest, []byte("x"))

	_, err := f.ex.ExportTenant(context.Background(), tenant)
	require.ErrorIs(t, err, boom)

	w := f.store.lastWriter()
	assert.True(t, w.aborted, "the upload is aborted")
	assert.False(t, w.closed)
	assert.Equal(t, []string{"Acquire", "Release"}, f.lease.ops())
	assert.False(t, f.lease.isHeld(tenant), "the lease is released")
	assert.Equal(t, []string{oldest, older}, f.store.keys(), "nothing is stored and nothing is pruned")
	assert.Zero(t, f.store.lists, "retention runs only after a success")
	assert.Equal(t, []string{ports.ExportFailure}, f.metrics.outcomes())
}

// A Close that fails stores nothing: the upload is aborted and the lease
// released.
func TestExporter_CloseFailureAbortsAndReleases(t *testing.T) {
	boom := errors.New("complete refused")
	f := newExporterFixture(smallGraph(2), 7)
	f.store.writer = func(w *fakeWriter) { w.closeErr = boom }

	_, err := f.ex.ExportTenant(context.Background(), tenant)
	require.ErrorIs(t, err, boom)
	assert.True(t, f.store.lastWriter().aborted)
	assert.False(t, f.lease.isHeld(tenant))
	assert.Empty(t, f.store.keys())
	assert.Equal(t, []string{ports.ExportFailure}, f.metrics.outcomes())
}

// A failing Abort does not keep the lease: the lease is released and both
// errors are returned.
func TestExporter_ReleasesTheLeaseEvenWhenAbortFails(t *testing.T) {
	boom := errors.New("read failed")
	abortErr := errors.New("abort refused")
	f := newExporterFixture(fakeGraph{export: func(context.Context, string, ports.RecordWriter) error { return boom }}, 7)
	f.store.writer = func(w *fakeWriter) { w.abortErr = abortErr }

	_, err := f.ex.ExportTenant(context.Background(), tenant)
	require.ErrorIs(t, err, boom)
	require.ErrorIs(t, err, abortErr)
	assert.Equal(t, []string{"Acquire", "Release"}, f.lease.ops())
	assert.False(t, f.lease.isHeld(tenant))
	assert.Equal(t, []string{ports.ExportFailure}, f.metrics.outcomes())
}

// A renewal that reports the lease lost cancels the export: its upload is
// aborted, and it fails with ErrLeaseLost.
func TestExporter_LostLeaseAbortsTheExport(t *testing.T) {
	graph := fakeGraph{export: func(ctx context.Context, tenantID string, out ports.RecordWriter) error {
		if err := out.WriteHeader(header(tenantID, 1, 0)); err != nil {
			return err
		}
		<-ctx.Done()
		return fmt.Errorf("stream nodes: %w", ctx.Err())
	}}
	f := newExporterFixture(graph, 7)
	f.ex.keeper.renewEvery = 10 * time.Millisecond
	f.lease.renewErr = func(int) error { return fmt.Errorf("tenant %q: %w", tenant, ports.ErrLeaseLost) }

	done := make(chan error, 1)
	go func() {
		_, err := f.ex.ExportTenant(context.Background(), tenant)
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ports.ErrLeaseLost)
	case <-time.After(5 * time.Second):
		t.Fatal("the export did not stop when its lease was lost")
	}
	assert.True(t, f.store.lastWriter().aborted)
	assert.Empty(t, f.store.keys())
	assert.Equal(t, []string{ports.ExportFailure}, f.metrics.outcomes())
	assert.Equal(t, "Release", f.lease.ops()[len(f.lease.ops())-1], "the lease is released all the same")
}

// A renewal that fails for another reason is retried at the next tick: the
// lease has not lapsed, so the export goes on.
func TestExporter_RenewFailingTransientlyKeepsTheExport(t *testing.T) {
	var mu sync.Mutex
	renewed := 0
	graph := fakeGraph{export: func(ctx context.Context, tenantID string, out ports.RecordWriter) error {
		deadline := time.Now().Add(5 * time.Second)
		for {
			mu.Lock()
			n := renewed
			mu.Unlock()
			if n >= 3 {
				break
			}
			if time.Now().After(deadline) || ctx.Err() != nil {
				return errors.New("the renewals did not come")
			}
			time.Sleep(5 * time.Millisecond)
		}
		return smallGraph(2).export(ctx, tenantID, out)
	}}
	f := newExporterFixture(graph, 7)
	f.ex.keeper.renewEvery = 10 * time.Millisecond
	f.lease.renewErr = func(n int) error {
		mu.Lock()
		renewed = n
		mu.Unlock()
		if n <= 2 {
			return errors.New("neo4j: connection reset")
		}
		return nil
	}

	res, err := f.ex.ExportTenant(context.Background(), tenant)
	require.NoError(t, err)
	_, ok := f.store.object(res.ObjectKey)
	assert.True(t, ok)
	assert.Equal(t, []string{ports.ExportSuccess}, f.metrics.outcomes())
}

// The read ends before any renewal notices a lapsed lease: the export still
// confirms it holds the lease before it stores the object, so an export
// that writes may have changed is never stored.
func TestExporter_ConfirmsTheLeaseBeforeItStoresTheObject(t *testing.T) {
	f := newExporterFixture(smallGraph(2), 7)
	f.lease.renewErr = func(int) error { return fmt.Errorf("tenant %q: %w", tenant, ports.ErrLeaseLost) }

	_, err := f.ex.ExportTenant(context.Background(), tenant)
	require.ErrorIs(t, err, ports.ErrLeaseLost)
	assert.True(t, f.store.lastWriter().aborted)
	assert.Empty(t, f.store.keys())
	assert.Equal(t, []string{"Acquire", "Renew", "Release"}, f.lease.ops())
}

// A lease another export or a restore holds skips the export: its upload is
// aborted, nothing is stored, nothing is pruned, and the outcome is skipped.
func TestExporter_SkipsWhileAnotherHoldsTheLease(t *testing.T) {
	f := newExporterFixture(smallGraph(1), 1)
	require.NoError(t, f.lease.Acquire(context.Background(), tenant, ports.LeaseRestore, "restore/elsewhere", time.Minute))
	older, _ := export.ObjectKey(tenant, exportAt.Add(-48*time.Hour))
	oldest, _ := export.ObjectKey(tenant, exportAt.Add(-72*time.Hour))
	f.store.put(older, []byte("x"))
	f.store.put(oldest, []byte("x"))

	_, err := f.ex.ExportTenant(context.Background(), tenant)
	require.ErrorIs(t, err, ErrSkipped)
	assert.True(t, f.store.lastWriter().aborted)
	assert.Equal(t, []string{oldest, older}, f.store.keys())
	assert.Zero(t, f.store.lists)
	assert.Equal(t, []string{ports.ExportSkipped}, f.metrics.outcomes())
	assert.True(t, f.lease.isHeld(tenant), "the other holder's lease stays")
	assert.Equal(t, []string{"Acquire", "Acquire"}, f.lease.ops(), "a skipped export renews and releases nothing")
}

// A lease that cannot be read is a failure, not a skip.
func TestExporter_AcquireFailureIsAFailure(t *testing.T) {
	f := newExporterFixture(smallGraph(1), 7)
	f.lease.acquireErr = errors.New("neo4j: connection refused")

	_, err := f.ex.ExportTenant(context.Background(), tenant)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrSkipped)
	assert.True(t, f.store.lastWriter().aborted)
	assert.Equal(t, []string{ports.ExportFailure}, f.metrics.outcomes())
}

// A store that refuses the upload fails the export before any lease is
// taken, so the tenant's writes are never paused for it.
func TestExporter_OpenFailureTakesNoLease(t *testing.T) {
	f := newExporterFixture(smallGraph(1), 7)
	f.store.openErr = errors.New("access denied")

	_, err := f.ex.ExportTenant(context.Background(), tenant)
	require.ErrorContains(t, err, "access denied")
	assert.Empty(t, f.lease.ops(), "no lease is taken")
	assert.Equal(t, []string{ports.ExportFailure}, f.metrics.outcomes())
}

// A cancel while the export streams (shutdown) aborts the upload and still
// releases the lease, on a context that is not cancelled.
func TestExporter_ACancelAbortsTheUploadAndReleasesTheLease(t *testing.T) {
	started := make(chan struct{})
	graph := fakeGraph{export: func(ctx context.Context, tenantID string, out ports.RecordWriter) error {
		if err := out.WriteHeader(header(tenantID, 1, 0)); err != nil {
			return err
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	f := newExporterFixture(graph, 7)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-started; cancel() }()

	_, err := f.ex.ExportTenant(ctx, tenant)
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, f.store.lastWriter().aborted)
	assert.Empty(t, f.store.keys())
	assert.False(t, f.lease.isHeld(tenant))
	require.Len(t, f.lease.releaseCtxErrs, 1)
	assert.NoError(t, f.lease.releaseCtxErrs[0], "the lease is released on a context that is not cancelled")
}

// A release that fails does not undo a stored export: the object is complete,
// and the lease expires by itself.
func TestExporter_AFailedReleaseKeepsTheStoredExport(t *testing.T) {
	f := newExporterFixture(smallGraph(1), 7)
	f.lease.releaseErr = errors.New("neo4j: connection reset")

	res, err := f.ex.ExportTenant(context.Background(), tenant)
	require.NoError(t, err)
	_, ok := f.store.object(res.ObjectKey)
	assert.True(t, ok)
	assert.Equal(t, []string{ports.ExportSuccess}, f.metrics.outcomes())
}

// The export of a tenant with no nodes is a header alone.
func TestExporter_ExportsATenantWithNoNodes(t *testing.T) {
	f := newExporterFixture(smallGraph(0), 7)
	res, err := f.ex.ExportTenant(context.Background(), tenant)
	require.NoError(t, err)
	assert.Zero(t, res.Nodes)
	assert.Zero(t, res.Relationships)
	body, _ := f.store.object(res.ObjectKey)
	_, err = exportfile.Validate(bytes.NewReader(gunzip(t, body)), tenant)
	require.NoError(t, err)
}

// After a success, every export of the tenant but the newest keep is deleted.
func TestExporter_RetentionDeletesAllButTheNewestKeep(t *testing.T) {
	f := newExporterFixture(smallGraph(1), 3)
	var older []string
	for d := 1; d <= 4; d++ {
		key, _ := export.ObjectKey(tenant, exportAt.Add(-time.Duration(d)*24*time.Hour))
		f.store.put(key, []byte("x"))
		older = append(older, key)
	}
	other, _ := export.ObjectKey("t2", exportAt.Add(-30*24*time.Hour))
	f.store.put(other, []byte("x"))

	res, err := f.ex.ExportTenant(context.Background(), tenant)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{res.ObjectKey, older[0], older[1], other}, f.store.keys(),
		"the new export and the two newest before it stay; another tenant's exports are not touched")
}

// Retention never deletes the export it just stored, even when the store
// holds exports dated after it (a clock that was ahead): the stored export is
// always one of the kept, and the newest of the rest fill the other places.
func TestExporter_RetentionNeverDeletesTheExportItStored(t *testing.T) {
	future, _ := export.ObjectKey(tenant, exportAt.Add(365*24*time.Hour))
	past, _ := export.ObjectKey(tenant, exportAt.Add(-24*time.Hour))
	stored, _ := export.ObjectKey(tenant, exportAt)
	for keep, want := range map[int][]string{1: {stored}, 2: {stored, future}, 3: {stored, future, past}} {
		t.Run(fmt.Sprintf("keep=%d", keep), func(t *testing.T) {
			f := newExporterFixture(smallGraph(1), keep)
			f.store.put(future, []byte("x"))
			f.store.put(past, []byte("x"))

			res, err := f.ex.ExportTenant(context.Background(), tenant)
			require.NoError(t, err)
			require.Equal(t, stored, res.ObjectKey)
			assert.ElementsMatch(t, want, f.store.keys())
		})
	}
}

// A delete that fails does not stop the others, and does not fail the export.
func TestExporter_RetentionGoesOnPastAFailedDelete(t *testing.T) {
	f := newExporterFixture(smallGraph(1), 1)
	var older []string
	for d := 1; d <= 3; d++ {
		key, _ := export.ObjectKey(tenant, exportAt.Add(-time.Duration(d)*24*time.Hour))
		f.store.put(key, []byte("x"))
		older = append(older, key)
	}
	f.store.deleteErr[older[0]] = errors.New("access denied")

	res, err := f.ex.ExportTenant(context.Background(), tenant)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{res.ObjectKey, older[0]}, f.store.keys())
	assert.Equal(t, []string{ports.ExportSuccess}, f.metrics.outcomes())
}

// A list that fails leaves every export in place and the export a success.
func TestExporter_RetentionListFailureKeepsTheExport(t *testing.T) {
	f := newExporterFixture(smallGraph(1), 1)
	past, _ := export.ObjectKey(tenant, exportAt.Add(-24*time.Hour))
	f.store.put(past, []byte("x"))
	f.store.listErr = errors.New("throttled")

	res, err := f.ex.ExportTenant(context.Background(), tenant)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{res.ObjectKey, past}, f.store.keys())
	assert.Equal(t, []string{ports.ExportSuccess}, f.metrics.outcomes())
}

// stallFixture is an exporterFixture whose lease is renewed every 5 ms and
// whose export stalls after stallAfter without progress, so a test of the
// stall bound runs in milliseconds.
func stallFixture(graph ports.GraphExporter, stallAfter time.Duration) *exporterFixture {
	f := newExporterFixture(graph, 7)
	f.ex.keeper.renewEvery = 5 * time.Millisecond
	f.ex.keeper.stallAfter = stallAfter
	return f
}

// exportWithin runs ExportTenant under a deadline far past the stall bound,
// and returns its error and how long it ran.
func exportWithin(t *testing.T, ex *Exporter, deadline time.Duration) (error, time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	start := time.Now()
	_, err := ex.ExportTenant(ctx, tenant)
	return err, time.Since(start)
}

// assertStalledAndCleanedUp checks that a stalled export failed with
// ErrStalled well before the test's deadline, stored nothing, aborted its
// upload and released its lease on a context that is not cancelled.
func assertStalledAndCleanedUp(t *testing.T, f *exporterFixture, err error, took time.Duration) {
	t.Helper()
	require.ErrorIs(t, err, ErrStalled, "the stall is the error's cause")
	assert.NotErrorIs(t, err, context.DeadlineExceeded, "the export ended on its own, not at the test's deadline")
	assert.Less(t, took, time.Second, "the export ends soon after the stall limit")
	assert.True(t, f.store.lastWriter().aborted, "the upload is aborted")
	assert.Empty(t, f.store.keys(), "nothing is stored")
	assert.False(t, f.lease.isHeld(tenant), "the lease is released")
	require.NotEmpty(t, f.lease.releaseCtxErrs)
	assert.NoError(t, f.lease.releaseCtxErrs[len(f.lease.releaseCtxErrs)-1], "the lease is released on a context that is not cancelled")
	assert.Equal(t, []string{ports.ExportFailure}, f.metrics.outcomes())
}

// A graph read that never answers (a database that holds the connection
// open) would keep renewing the lease, and so pause the tenant's writes, for
// as long as it hangs. An export that moves no record and no byte for the
// stall limit is stopped: it fails with ErrStalled, aborts and releases.
func TestExporter_AStalledExportFailsWithinTheBound(t *testing.T) {
	for name, graph := range map[string]fakeGraph{
		// Before the header: the counting queries hang.
		"before the header": {export: func(ctx context.Context, _ string, _ ports.RecordWriter) error {
			<-ctx.Done()
			return ctx.Err()
		}},
		// After some records: the stream hangs.
		"mid-stream": {export: func(ctx context.Context, tenantID string, out ports.RecordWriter) error {
			if err := out.WriteHeader(header(tenantID, 3, 0)); err != nil {
				return err
			}
			if err := out.WriteNode(node(tenantID, 0)); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		}},
	} {
		t.Run(name, func(t *testing.T) {
			f := stallFixture(graph, 50*time.Millisecond)
			err, took := exportWithin(t, f.ex, 3*time.Second)
			assertStalledAndCleanedUp(t, f, err, took)
		})
	}
}

// An upload the store never answers is stopped too: the stall cancels the
// context the object was opened with, which ends the hung write.
func TestExporter_AStalledUploadFailsWithinTheBound(t *testing.T) {
	f := stallFixture(smallGraph(3), 50*time.Millisecond)
	f.store.writer = func(w *fakeWriter) { w.blockWrites = true }
	err, took := exportWithin(t, f.ex, 3*time.Second)
	assertStalledAndCleanedUp(t, f, err, took)
}

// A renewal that never answers does not keep the stall from being noticed:
// each renewal is bounded.
func TestExporter_AHungRenewalDoesNotHideAStall(t *testing.T) {
	f := stallFixture(fakeGraph{export: func(ctx context.Context, _ string, _ ports.RecordWriter) error {
		<-ctx.Done()
		return ctx.Err()
	}}, 50*time.Millisecond)
	f.ex.keeper.renewTimeout = 10 * time.Millisecond
	f.lease.renewBlocks = true
	err, took := exportWithin(t, f.ex, 3*time.Second)
	assertStalledAndCleanedUp(t, f, err, took)
}

// An export that keeps moving records is never stopped, however long it
// runs: the stall limit counts from the last record or byte.
func TestExporter_AnExportThatMovesIsNotStalled(t *testing.T) {
	graph := fakeGraph{export: func(_ context.Context, tenantID string, out ports.RecordWriter) error {
		time.Sleep(60 * time.Millisecond) // the counting step, within the limit
		if err := out.WriteHeader(header(tenantID, 8, 0)); err != nil {
			return err
		}
		for i := range 8 {
			time.Sleep(40 * time.Millisecond)
			if err := out.WriteNode(node(tenantID, i)); err != nil {
				return err
			}
		}
		return nil
	}}
	f := stallFixture(graph, 120*time.Millisecond)
	err, took := exportWithin(t, f.ex, 3*time.Second)
	require.NoError(t, err)
	assert.Greater(t, took, 2*f.ex.keeper.stallAfter, "the export ran longer than the stall limit")
	assert.Len(t, f.store.keys(), 1)
	assert.Equal(t, []string{ports.ExportSuccess}, f.metrics.outcomes())
}
