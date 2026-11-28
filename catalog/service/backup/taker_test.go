package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const tenant = "t1"

var backupAt = time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC)

// takerFixture is an Taker over fakes, with a clock at backupAt.
type takerFixture struct {
	lease   *fakeLease
	store   *fakeStore
	metrics *fakeMetrics
	clock   *fakeClock
	ex      *Taker
}

func newTakerFixture(graph ports.GraphSaver, keep int) *takerFixture {
	f := &takerFixture{lease: newFakeLease(), store: newFakeStore(), metrics: &fakeMetrics{}, clock: newFakeClock(backupAt)}
	f.ex = NewTaker(f.lease, graph, f.store, f.metrics, keep, f.clock.Now, quietLogger())
	return f
}

// A backup opens its sink before it takes the lease, streams the graph's
// records into it in order, completes it, releases the lease and reports its
// numbers: the key and the size are the store's.
func TestTaker_StreamsTheGraphIntoTheStore(t *testing.T) {
	f := newTakerFixture(smallGraph(3), 7)
	f.lease.onAcquire = func() {
		assert.NotNil(t, f.store.lastSink(), "the backup is opened before the lease is taken")
	}

	res, err := f.ex.TakeTenant(context.Background(), tenant)
	require.NoError(t, err)

	key := fakeKey(tenant, backupAt)
	assert.Equal(t, key, res.ObjectKey)
	stored, ok := f.store.stored(key)
	require.True(t, ok, "the backup is stored")
	assert.Equal(t, stored.size, res.Bytes, "Bytes is the size the store reported")
	assert.Equal(t, int64(3), res.Nodes)
	assert.Equal(t, int64(2), res.Relationships)
	assert.Equal(t, header(tenant, 3, 2), stored.header)
	assert.Equal(t, []any{node(tenant, 0), node(tenant, 1), node(tenant, 2), dependsOn(1, 0), dependsOn(2, 1)}, stored.records,
		"every record, in the order the graph wrote it")

	assert.Equal(t, []string{"Acquire", "Renew", "Release"}, f.lease.ops(),
		"the lease is taken, confirmed after the read, and released")
	acquire := f.lease.callsOf("Acquire")[0]
	assert.Equal(t, ports.LeaseBackup, acquire.Kind)
	assert.Equal(t, leaseTTL, acquire.TTL)
	assert.Equal(t, acquire.Holder, f.lease.callsOf("Release")[0].Holder)
	assert.False(t, f.lease.isHeld(tenant))

	assert.Equal(t, observation{Outcome: ports.BackupSuccess, Bytes: res.Bytes, Nodes: 3, Rels: 2}, f.metrics.last())
}

func TestTaker_LeaseConstants(t *testing.T) {
	assert.Equal(t, 2*time.Minute, leaseTTL)
	assert.Equal(t, 30*time.Second, leaseRenewEvery)
	assert.Equal(t, leaseTTL, stallLimit, "a stuck holder pauses writes no longer than a crashed one")
	assert.Less(t, renewTimeout, leaseRenewEvery)
	ex := NewTaker(newFakeLease(), smallGraph(1), newFakeStore(), &fakeMetrics{}, 7, time.Now, quietLogger())
	assert.Equal(t, leaseTTL, ex.keeper.ttl)
	assert.Equal(t, leaseRenewEvery, ex.keeper.renewEvery)
	assert.Equal(t, stallLimit, ex.keeper.stallAfter)
	assert.Equal(t, renewTimeout, ex.keeper.renewTimeout)
	assert.Equal(t, inFlightRetryEvery, ex.keeper.waitEvery)
	r := NewRestorer(newFakeLease(), &fakeLoader{}, newFakeStore(), quietLogger())
	assert.Equal(t, ex.keeper.stallAfter, r.keeper.stallAfter)
}

// Each backup takes the lease under a holder of its own, so a renewal of one
// backup can never extend another's.
func TestTaker_EachBackupHoldsTheLeaseUnderItsOwnHolder(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 7)
	for range 2 {
		_, err := f.ex.TakeTenant(context.Background(), tenant)
		require.NoError(t, err)
		f.clock.Advance(time.Hour)
	}
	acquires := f.lease.callsOf("Acquire")
	require.Len(t, acquires, 2)
	assert.NotEqual(t, acquires[0].Holder, acquires[1].Holder)
	assert.True(t, strings.HasPrefix(acquires[0].Holder, "backup/"), acquires[0].Holder)
}

// A graph error in the middle of the stream aborts the backup and releases
// the lease: nothing is stored, and retention does not run.
func TestTaker_FailureAbortsAndReleases(t *testing.T) {
	boom := errors.New("neo4j went away")
	graph := fakeGraph{backup: func(_ context.Context, tenantID string, out ports.RecordWriter) error {
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
	f := newTakerFixture(graph, 1)
	older := f.store.put(tenant, backupAt.Add(-48*time.Hour), 1)
	oldest := f.store.put(tenant, backupAt.Add(-72*time.Hour), 1)

	_, err := f.ex.TakeTenant(context.Background(), tenant)
	require.ErrorIs(t, err, boom)

	w := f.store.lastSink()
	assert.True(t, w.aborted, "the backup is aborted")
	assert.False(t, w.closed)
	assert.Equal(t, []string{"Acquire", "Release"}, f.lease.ops())
	assert.False(t, f.lease.isHeld(tenant), "the lease is released")
	assert.Equal(t, []string{oldest, older}, f.store.keys(), "nothing is stored and nothing is pruned")
	assert.Zero(t, f.store.lists, "retention runs only after a success")
	assert.Equal(t, []string{ports.BackupFailure}, f.metrics.outcomes())
}

// A Close that fails stores nothing: the backup is aborted and the lease
// released.
func TestTaker_CloseFailureAbortsAndReleases(t *testing.T) {
	boom := errors.New("complete refused")
	f := newTakerFixture(smallGraph(2), 7)
	f.store.sink = func(w *fakeSink) { w.closeErr = boom }

	_, err := f.ex.TakeTenant(context.Background(), tenant)
	require.ErrorIs(t, err, boom)
	assert.True(t, f.store.lastSink().aborted)
	assert.False(t, f.lease.isHeld(tenant))
	assert.Empty(t, f.store.keys())
	assert.Equal(t, []string{ports.BackupFailure}, f.metrics.outcomes())
}

// A failing Abort does not keep the lease: the lease is released and both
// errors are returned.
func TestTaker_ReleasesTheLeaseEvenWhenAbortFails(t *testing.T) {
	boom := errors.New("read failed")
	abortErr := errors.New("abort refused")
	f := newTakerFixture(fakeGraph{backup: func(context.Context, string, ports.RecordWriter) error { return boom }}, 7)
	f.store.sink = func(w *fakeSink) { w.abortErr = abortErr }

	_, err := f.ex.TakeTenant(context.Background(), tenant)
	require.ErrorIs(t, err, boom)
	require.ErrorIs(t, err, abortErr)
	assert.Equal(t, []string{"Acquire", "Release"}, f.lease.ops())
	assert.False(t, f.lease.isHeld(tenant))
	assert.Equal(t, []string{ports.BackupFailure}, f.metrics.outcomes())
}

// A renewal that reports the lease lost cancels the backup: it is
// aborted, and it fails with ErrLeaseLost.
func TestTaker_LostLeaseAbortsTheBackup(t *testing.T) {
	graph := fakeGraph{backup: func(ctx context.Context, tenantID string, out ports.RecordWriter) error {
		if err := out.WriteHeader(header(tenantID, 1, 0)); err != nil {
			return err
		}
		<-ctx.Done()
		return fmt.Errorf("stream nodes: %w", ctx.Err())
	}}
	f := newTakerFixture(graph, 7)
	f.ex.keeper.renewEvery = 10 * time.Millisecond
	f.lease.renewErr = func(int) error { return fmt.Errorf("tenant %q: %w", tenant, ports.ErrLeaseLost) }

	done := make(chan error, 1)
	go func() {
		_, err := f.ex.TakeTenant(context.Background(), tenant)
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ports.ErrLeaseLost)
	case <-time.After(5 * time.Second):
		t.Fatal("the backup did not stop when its lease was lost")
	}
	assert.True(t, f.store.lastSink().aborted)
	assert.Empty(t, f.store.keys())
	assert.Equal(t, []string{ports.BackupFailure}, f.metrics.outcomes())
	assert.Equal(t, "Release", f.lease.ops()[len(f.lease.ops())-1], "the lease is released all the same")
}

// A renewal that fails for another reason is retried at the next tick: the
// lease has not lapsed, so the backup goes on.
func TestTaker_RenewFailingTransientlyKeepsTheBackup(t *testing.T) {
	var mu sync.Mutex
	renewed := 0
	graph := fakeGraph{backup: func(ctx context.Context, tenantID string, out ports.RecordWriter) error {
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
		return smallGraph(2).backup(ctx, tenantID, out)
	}}
	f := newTakerFixture(graph, 7)
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

	res, err := f.ex.TakeTenant(context.Background(), tenant)
	require.NoError(t, err)
	_, ok := f.store.stored(res.ObjectKey)
	assert.True(t, ok)
	assert.Equal(t, []string{ports.BackupSuccess}, f.metrics.outcomes())
}

// The read ends before any renewal notices a lapsed lease: the backup still
// confirms it holds the lease before it stores the backup, so a backup
// that writes may have changed is never stored.
func TestTaker_ConfirmsTheLeaseBeforeItStoresTheObject(t *testing.T) {
	f := newTakerFixture(smallGraph(2), 7)
	f.lease.renewErr = func(int) error { return fmt.Errorf("tenant %q: %w", tenant, ports.ErrLeaseLost) }

	_, err := f.ex.TakeTenant(context.Background(), tenant)
	require.ErrorIs(t, err, ports.ErrLeaseLost)
	assert.True(t, f.store.lastSink().aborted)
	assert.Empty(t, f.store.keys())
	assert.Equal(t, []string{"Acquire", "Renew", "Release"}, f.lease.ops())
}

// A lease another backup or a restore holds skips the backup: it is
// aborted, nothing is stored, nothing is pruned, and the outcome is skipped.
func TestTaker_SkipsWhileAnotherHoldsTheLease(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 1)
	require.NoError(t, f.lease.Acquire(context.Background(), tenant, ports.LeaseRestore, "restore/elsewhere", time.Minute))
	older := f.store.put(tenant, backupAt.Add(-48*time.Hour), 1)
	oldest := f.store.put(tenant, backupAt.Add(-72*time.Hour), 1)

	_, err := f.ex.TakeTenant(context.Background(), tenant)
	require.ErrorIs(t, err, ErrSkipped)
	assert.True(t, f.store.lastSink().aborted)
	assert.Equal(t, []string{oldest, older}, f.store.keys())
	assert.Zero(t, f.store.lists)
	assert.Equal(t, []string{ports.BackupSkipped}, f.metrics.outcomes())
	assert.True(t, f.lease.isHeld(tenant), "the other holder's lease stays")
	assert.Equal(t, []string{"Acquire", "Acquire"}, f.lease.ops(), "a skipped backup renews and releases nothing")
}

// A lease that cannot be taken for another reason than another holder is a
// failure, not a skip. The Acquire may have committed while its answer was
// lost, so the backup releases the lease under its own holder, which does
// nothing to a lease it does not have.
func TestTaker_AcquireFailureIsAFailureAndReleases(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 7)
	f.lease.acquireErr = errors.New("neo4j: connection refused")

	_, err := f.ex.TakeTenant(context.Background(), tenant)
	require.ErrorContains(t, err, "connection refused")
	assert.NotErrorIs(t, err, ErrSkipped)
	assert.True(t, f.store.lastSink().aborted)
	assert.Equal(t, []string{ports.BackupFailure}, f.metrics.outcomes())
	assert.Equal(t, []string{"Acquire", "Release"}, f.lease.ops())
	assert.Equal(t, f.lease.callsOf("Acquire")[0].Holder, f.lease.callsOf("Release")[0].Holder,
		"the release names the backup's own holder")
	require.Len(t, f.lease.releaseCtxErrs, 1)
	assert.NoError(t, f.lease.releaseCtxErrs[0])
}

// An Acquire that committed but reported a failure leaves no held lease: the
// release that follows ends it, so the tenant's writes are not paused for the
// lease's ttl.
func TestTaker_AnAcquireThatCommittedButFailedIsReleased(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 7)
	f.lease.acquireCommitsThenFails = errors.New("neo4j: connection reset after commit")

	_, err := f.ex.TakeTenant(context.Background(), tenant)
	require.ErrorContains(t, err, "connection reset")
	assert.False(t, f.lease.isHeld(tenant), "the lease the failed Acquire took is released")
}

// A store that refuses the backup fails the backup before any lease is
// taken, so the tenant's writes are never paused for it.
func TestTaker_OpenFailureTakesNoLease(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 7)
	f.store.openErr = errors.New("access denied")

	_, err := f.ex.TakeTenant(context.Background(), tenant)
	require.ErrorContains(t, err, "access denied")
	assert.Empty(t, f.lease.ops(), "no lease is taken")
	assert.Equal(t, []string{ports.BackupFailure}, f.metrics.outcomes())
}

// A cancel while the backup streams (shutdown) aborts the backup and still
// releases the lease, on a context that is not cancelled.
func TestTaker_ACancelAbortsTheUploadAndReleasesTheLease(t *testing.T) {
	started := make(chan struct{})
	graph := fakeGraph{backup: func(ctx context.Context, tenantID string, out ports.RecordWriter) error {
		if err := out.WriteHeader(header(tenantID, 1, 0)); err != nil {
			return err
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	f := newTakerFixture(graph, 7)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-started; cancel() }()

	_, err := f.ex.TakeTenant(ctx, tenant)
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, f.store.lastSink().aborted)
	assert.Empty(t, f.store.keys())
	assert.False(t, f.lease.isHeld(tenant))
	require.Len(t, f.lease.releaseCtxErrs, 1)
	assert.NoError(t, f.lease.releaseCtxErrs[0], "the lease is released on a context that is not cancelled")
}

// A release that fails does not undo a stored backup: the backup is complete,
// and the lease expires by itself.
func TestTaker_AFailedReleaseKeepsTheStoredBackup(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 7)
	f.lease.releaseErr = errors.New("neo4j: connection reset")

	res, err := f.ex.TakeTenant(context.Background(), tenant)
	require.NoError(t, err)
	_, ok := f.store.stored(res.ObjectKey)
	assert.True(t, ok)
	assert.Equal(t, []string{ports.BackupSuccess}, f.metrics.outcomes())
}

// The backup of a tenant with no nodes is a header alone.
func TestTaker_BackupsATenantWithNoNodes(t *testing.T) {
	f := newTakerFixture(smallGraph(0), 7)
	res, err := f.ex.TakeTenant(context.Background(), tenant)
	require.NoError(t, err)
	assert.Zero(t, res.Nodes)
	assert.Zero(t, res.Relationships)
	stored, ok := f.store.stored(res.ObjectKey)
	require.True(t, ok)
	assert.Equal(t, header(tenant, 0, 0), stored.header)
	assert.Empty(t, stored.records)
}

// After a success, every backup of the tenant but the newest keep is deleted.
func TestTaker_RetentionKeepsTheNewestN(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 3)
	var older []string
	for d := 1; d <= 4; d++ {
		older = append(older, f.store.put(tenant, backupAt.Add(-time.Duration(d)*24*time.Hour), 1))
	}
	other := f.store.put("t2", backupAt.Add(-30*24*time.Hour), 1)

	res, err := f.ex.TakeTenant(context.Background(), tenant)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{res.ObjectKey, older[0], older[1], other}, f.store.keys(),
		"the new backup and the two newest before it stay; another tenant's backups are not touched")
}

// Retention never deletes the backup it just stored, even when the store
// holds backups dated after it (a clock that was ahead): the stored backup is
// always one of the kept, and the newest of the rest fill the other places.
func TestTaker_RetentionNeverDeletesTheBackupItStored(t *testing.T) {
	future := fakeKey(tenant, backupAt.Add(365*24*time.Hour))
	past := fakeKey(tenant, backupAt.Add(-24*time.Hour))
	stored := fakeKey(tenant, backupAt)
	for keep, want := range map[int][]string{1: {stored}, 2: {stored, future}, 3: {stored, future, past}} {
		t.Run(fmt.Sprintf("keep=%d", keep), func(t *testing.T) {
			f := newTakerFixture(smallGraph(1), keep)
			f.store.put(tenant, backupAt.Add(365*24*time.Hour), 1)
			f.store.put(tenant, backupAt.Add(-24*time.Hour), 1)

			res, err := f.ex.TakeTenant(context.Background(), tenant)
			require.NoError(t, err)
			require.Equal(t, stored, res.ObjectKey)
			assert.ElementsMatch(t, want, f.store.keys())
		})
	}
}

// A delete that fails does not stop the others, and does not fail the backup.
func TestTaker_RetentionGoesOnPastAFailedDelete(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 1)
	var older []string
	for d := 1; d <= 3; d++ {
		older = append(older, f.store.put(tenant, backupAt.Add(-time.Duration(d)*24*time.Hour), 1))
	}
	f.store.deleteErr[older[0]] = errors.New("access denied")

	res, err := f.ex.TakeTenant(context.Background(), tenant)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{res.ObjectKey, older[0]}, f.store.keys())
	assert.Equal(t, []string{ports.BackupSuccess}, f.metrics.outcomes())
}

// A list that fails leaves every backup in place and the backup a success.
func TestTaker_RetentionListFailureKeepsTheBackup(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 1)
	past := f.store.put(tenant, backupAt.Add(-24*time.Hour), 1)
	f.store.listErr = errors.New("throttled")

	res, err := f.ex.TakeTenant(context.Background(), tenant)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{res.ObjectKey, past}, f.store.keys())
	assert.Equal(t, []string{ports.BackupSuccess}, f.metrics.outcomes())
}

// stallFixture is a takerFixture whose lease is renewed every 5 ms and
// whose backup stalls after stallAfter without progress, so a test of the
// stall bound runs in milliseconds.
func stallFixture(graph ports.GraphSaver, stallAfter time.Duration) *takerFixture {
	f := newTakerFixture(graph, 7)
	f.ex.keeper.renewEvery = 5 * time.Millisecond
	f.ex.keeper.stallAfter = stallAfter
	return f
}

// takeWithin runs TakeTenant under a deadline far past the stall bound,
// and returns how long it ran and its error.
func takeWithin(t *testing.T, ex *Taker, deadline time.Duration) (time.Duration, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	start := time.Now()
	_, err := ex.TakeTenant(ctx, tenant)
	return time.Since(start), err
}

// assertStalledAndCleanedUp checks that a stalled backup failed with
// ErrStalled well before the test's deadline, stored nothing, aborted its
// backup and released its lease on a context that is not cancelled.
func assertStalledAndCleanedUp(t *testing.T, f *takerFixture, err error, took time.Duration) {
	t.Helper()
	require.ErrorIs(t, err, ErrStalled, "the stall is the error's cause")
	assert.NotErrorIs(t, err, context.DeadlineExceeded, "the backup ended on its own, not at the test's deadline")
	assert.Less(t, took, time.Second, "the backup ends soon after the stall limit")
	assert.True(t, f.store.lastSink().aborted, "the backup is aborted")
	assert.Empty(t, f.store.keys(), "nothing is stored")
	assert.False(t, f.lease.isHeld(tenant), "the lease is released")
	require.NotEmpty(t, f.lease.releaseCtxErrs)
	assert.NoError(t, f.lease.releaseCtxErrs[len(f.lease.releaseCtxErrs)-1], "the lease is released on a context that is not cancelled")
	assert.Equal(t, []string{ports.BackupFailure}, f.metrics.outcomes())
}

// A graph read that never answers (a database that holds the connection
// open) would keep renewing the lease, and so pause the tenant's writes, for
// as long as it hangs. A backup that moves no record for the stall limit
// is stopped: it fails with ErrStalled, aborts and releases.
func TestTaker_AStalledBackupFailsWithinTheBound(t *testing.T) {
	for name, graph := range map[string]fakeGraph{
		// Before the header: the counting queries hang.
		"before the header": {backup: func(ctx context.Context, _ string, _ ports.RecordWriter) error {
			<-ctx.Done()
			return ctx.Err()
		}},
		// After some records: the stream hangs.
		"mid-stream": {backup: func(ctx context.Context, tenantID string, out ports.RecordWriter) error {
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
			took, err := takeWithin(t, f.ex, 3*time.Second)
			assertStalledAndCleanedUp(t, f, err, took)
		})
	}
}

// A write the store never answers is stopped too: the stall cancels the
// context the backup was opened with, which ends the hung write.
func TestTaker_AStalledUploadFailsWithinTheBound(t *testing.T) {
	f := stallFixture(smallGraph(3), 50*time.Millisecond)
	f.store.sink = func(w *fakeSink) { w.blockWrites = true }
	took, err := takeWithin(t, f.ex, 3*time.Second)
	assertStalledAndCleanedUp(t, f, err, took)
}

// A renewal that never answers does not keep the stall from being noticed:
// each renewal is bounded.
func TestTaker_AHungRenewalDoesNotHideAStall(t *testing.T) {
	f := stallFixture(fakeGraph{backup: func(ctx context.Context, _ string, _ ports.RecordWriter) error {
		<-ctx.Done()
		return ctx.Err()
	}}, 50*time.Millisecond)
	f.ex.keeper.renewTimeout = 10 * time.Millisecond
	f.lease.renewBlocks = true
	took, err := takeWithin(t, f.ex, 3*time.Second)
	assertStalledAndCleanedUp(t, f, err, took)
}

// A backup that keeps moving records is never stopped, however long it
// runs: the stall limit counts from the last record.
func TestTaker_AnBackupThatMovesIsNotStalled(t *testing.T) {
	graph := fakeGraph{backup: func(_ context.Context, tenantID string, out ports.RecordWriter) error {
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
	took, err := takeWithin(t, f.ex, 3*time.Second)
	require.NoError(t, err)
	assert.Greater(t, took, 2*f.ex.keeper.stallAfter, "the backup ran longer than the stall limit")
	assert.Len(t, f.store.keys(), 1)
	assert.Equal(t, []string{ports.BackupSuccess}, f.metrics.outcomes())
}

// A record whose write takes longer than the stall limit stays alive while the
// store reports progress for each write to the object store that makes up the
// record.
func TestTaker_StoreProgressKeepsALongRecordAlive(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 7)
	f.ex.keeper.renewEvery = 5 * time.Millisecond
	f.ex.keeper.stallAfter = 30 * time.Millisecond
	f.store.sink = func(w *fakeSink) {
		w.onWrite = func() {
			for range 6 {
				time.Sleep(10 * time.Millisecond)
				if w.progress != nil {
					w.progress()
				}
			}
		}
	}

	_, err := f.ex.TakeTenant(context.Background(), tenant)
	require.NoError(t, err)
}

func TestTaker_WaitsForWritesInFlightThenBackups(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 7)
	f.lease.acquireErrs = []error{ports.ErrWritesInFlight, ports.ErrWritesInFlight}
	f.ex.keeper.waitEvery = 5 * time.Millisecond

	_, err := f.ex.TakeTenant(context.Background(), tenant)
	require.NoError(t, err)
	assert.Len(t, f.lease.callsOf("Acquire"), 3, "two refusals, then the lease")
	assert.Equal(t, []string{ports.BackupSuccess}, f.metrics.outcomes())
	assert.NotContains(t, f.lease.ops()[:2], "Release", "a refusal took nothing, so it releases nothing")
}

func TestTaker_StopsWaitingWhenItsContextEnds(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 7)
	f.lease.acquireErr = ports.ErrWritesInFlight
	f.ex.keeper.waitEvery = 5 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := f.ex.TakeTenant(ctx, tenant)
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("the backup kept waiting after its context ended")
	}
	assert.True(t, f.store.lastSink().aborted, "the opened backup is aborted")
	assert.Equal(t, []string{ports.BackupFailure}, f.metrics.outcomes())
}

// A restore can take the lease while a backup waits; the backup then ends
// skipped instead of queueing behind the restore.
func TestTaker_WaitingBackupSkipsWhenALeaseTurnsUp(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 7)
	f.lease.acquireErrs = []error{ports.ErrWritesInFlight, ports.ErrLeaseHeld}
	f.ex.keeper.waitEvery = 5 * time.Millisecond

	_, err := f.ex.TakeTenant(context.Background(), tenant)
	require.ErrorIs(t, err, ErrSkipped)
	assert.Equal(t, []string{ports.BackupSkipped}, f.metrics.outcomes())
}

// TakeEach backs up the tenants one after the other, in the order given, and
// reports each one's outcome as it ends.
func TestTaker_TakeEachBackupsInOrder(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 7)
	var order []string
	var results []Result

	notReached := f.ex.TakeEach(context.Background(), []string{"a", "b", "c"}, func(tenantID string, res Result, err error) {
		require.NoError(t, err)
		order = append(order, tenantID)
		results = append(results, res)
	})

	assert.Equal(t, []string{"a", "b", "c"}, order)
	assert.Empty(t, notReached)
	for i, tenantID := range order {
		assert.Equal(t, fakeKey(tenantID, backupAt), results[i].ObjectKey, "each tenant's backup is its own")
	}
}

// A tenant whose backup fails or is skipped does not stop the ones after it:
// each hears its own error.
func TestTaker_TakeEachReportsEachTenantsError(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 7)
	f.lease.held["b"] = "someone-else"
	var errs []error

	f.ex.TakeEach(context.Background(), []string{"a", "b", "c"}, func(_ string, _ Result, err error) {
		errs = append(errs, err)
	})

	require.Len(t, errs, 3)
	assert.NoError(t, errs[0])
	assert.ErrorIs(t, errs[1], ErrSkipped)
	assert.NoError(t, errs[2])
}

// When the context ends, TakeEach starts no further tenant and returns the
// ones it did not start.
func TestTaker_TakeEachReturnsTheTenantsItDidNotStart(t *testing.T) {
	f := newTakerFixture(smallGraph(1), 7)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var order []string

	notReached := f.ex.TakeEach(ctx, []string{"a", "b", "c"}, func(tenantID string, _ Result, _ error) {
		order = append(order, tenantID)
		cancel()
	})

	assert.Equal(t, []string{"a"}, order)
	assert.Equal(t, []string{"b", "c"}, notReached)
}
