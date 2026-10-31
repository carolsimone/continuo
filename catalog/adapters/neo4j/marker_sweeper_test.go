//go:build integration

package neo4jinfra

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgretention "github.com/carolsimone/continuo/pkg/retention"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Prune is the retention sweeper's prune function.
var _ pkgretention.PruneFunc = (*MarkerSweeper)(nil).Prune

// seedMarkers creates n processed-message markers of the fixture's tenant,
// named prefix-1 … prefix-n, each recorded age ago by the database clock, so
// the test has no clock skew between the seed and the sweep.
func (f *uowFixture) seedMarkers(prefix string, n int, age time.Duration) {
	f.t.Helper()
	f.run(`UNWIND range(1, $n) AS i
CREATE (:ProcessedMessage {tenant_id: $tenant_id, group: $group, message_id: $prefix + '-' + toString(i),
    processed_at: datetime() - duration({milliseconds: $age_ms})})`,
		map[string]any{"n": n, "group": streams.CatalogReleaseTopology, "prefix": prefix, "age_ms": age.Milliseconds()})
}

// seedOldBystander creates a node of another label that carries an old
// processed_at as well, so a sweep that matched nodes of any label would take
// it.
func (f *uowFixture) seedOldBystander() {
	f.t.Helper()
	f.run(`CREATE (:UnitOfWorkProbe {tenant_id: $tenant_id, test_marker: $test_marker, name: 'bystander',
    processed_at: datetime() - duration({hours: 72})})`,
		map[string]any{"test_marker": f.t.Name()})
}

// run executes a write statement bound to the fixture's tenant.
func (f *uowFixture) run(cypher string, params map[string]any) {
	f.t.Helper()
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = s.Close(ctx) }()
	bound := map[string]any{"tenant_id": f.tenant}
	for k, v := range params {
		bound[k] = v
	}
	res, err := s.Run(ctx, cypher, bound)
	require.NoError(f.t, err)
	_, err = res.Consume(ctx)
	require.NoError(f.t, err)
}

// markerIDs lists the message ids of the fixture's markers, in order.
func (f *uowFixture) markerIDs() ([]string, error) {
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = s.Close(ctx) }()
	res, err := s.Run(ctx, `MATCH (p:ProcessedMessage {tenant_id: $tenant_id}) RETURN p.message_id AS id ORDER BY id`,
		map[string]any{"tenant_id": f.tenant})
	if err != nil {
		return nil, err
	}
	recs, err := res.Collect(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(recs))
	for _, rec := range recs {
		id, _ := rec.Get("id")
		ids = append(ids, id.(string))
	}
	return ids, nil
}

func (f *uowFixture) requireMarkerIDs(want ...string) {
	f.t.Helper()
	got, err := f.markerIDs()
	require.NoError(f.t, err)
	require.Equal(f.t, append([]string{}, want...), got)
}

func (f *uowFixture) requireBystanderKept() {
	f.t.Helper()
	assert.Equal(f.t, int64(1), f.probes(), "the sweep touches no node of another label")
}

// TestMarkerSweeper_DeletesOlderThanRetentionInBatches seeds more expired
// markers than one batch: each Prune deletes at most limit of them and returns
// how many, and the sweep ends with a Prune that returns 0. Markers inside the
// retention stay, as does a node of another label with an old processed_at.
func TestMarkerSweeper_DeletesOlderThanRetentionInBatches(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	f.seedMarkers("old", 25, 2*time.Hour)
	f.seedMarkers("recent", 5, 30*time.Minute)
	f.seedOldBystander()
	sweeper := NewMarkerSweeper(f.client, quietLogger())

	for i, want := range []int64{10, 10, 5, 0} {
		got, err := sweeper.Prune(ctx, time.Hour, 10)
		require.NoError(t, err, "prune %d", i+1)
		require.Equal(t, want, got, "prune %d", i+1)
	}

	f.requireMarkerIDs("recent-1", "recent-2", "recent-3", "recent-4", "recent-5")
	f.requireBystanderKept()
	f.assertGateUnlocked()

	// The next write of the tenant is not held up by the sweep.
	require.NoError(t, NewUnitOfWork(f.client, quietLogger()).Write(ctx, f.marker("1762077600000-0", nil), true,
		func(context.Context, uow.Tx) error { return nil }))
}

// TestMarkerSweeper_KeepsMarkersWithinTheReplayHorizon runs the sweeper the way
// main does, with a 7-day retention and the replay horizon as the target's
// minimum: a 20-day-old marker is still inside the horizon and stays, while a
// 40-day-old one goes.
func TestMarkerSweeper_KeepsMarkersWithinTheReplayHorizon(t *testing.T) {
	f := newUoWFixture(t)
	f.seedMarkers("twenty-days", 1, 20*24*time.Hour)
	f.seedMarkers("forty-days", 1, 40*24*time.Hour)
	sweeper := NewMarkerSweeper(f.client, quietLogger())

	retention := pkgretention.NewSweeper(
		[]pkgretention.Target{{
			Name:         "processed_messages",
			Prune:        sweeper.Prune,
			MinRetention: model.ReplayHorizon,
		}},
		pkgretention.Config{Retention: 7 * 24 * time.Hour, Interval: 50 * time.Millisecond},
		quietLogger(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		retention.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	require.Eventually(t, func() bool {
		ids, err := f.markerIDs()
		return err == nil && len(ids) == 1
	}, 15*time.Second, 50*time.Millisecond, "the 40-day-old marker is deleted")
	// Both markers fall to one statement, so the 20-day-old one would be gone
	// by now had the 7-day retention applied.
	f.requireMarkerIDs("twenty-days-1")
}

// TestMarkerSweeper_SkipsATenantWhoseLeaseIsHeld holds tenant A's export lease:
// a sweep deletes tenant B's expired markers and none of A's. Once A's lease
// ends, by its release or by its expiry, A's markers go too.
func TestMarkerSweeper_SkipsATenantWhoseLeaseIsHeld(t *testing.T) {
	ctx := context.Background()
	a, b := newUoWFixture(t), newUoWFixture(t)
	a.seedMarkers("a-old", 3, 2*time.Hour)
	b.seedMarkers("b-old", 3, 2*time.Hour)
	sweeper := NewMarkerSweeper(a.client, quietLogger())
	lease := NewExportLease(a.client, quietLogger())

	require.NoError(t, lease.Acquire(ctx, a.tenant, ports.LeaseExport, "exporter-1", time.Minute))
	got, err := sweeper.Prune(ctx, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(3), got, "only the tenant whose lease is free is swept")
	a.requireMarkerIDs("a-old-1", "a-old-2", "a-old-3")
	b.requireMarkerIDs()

	require.NoError(t, lease.Release(ctx, a.tenant, "exporter-1"))
	got, err = sweeper.Prune(ctx, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(3), got, "a released lease no longer protects the tenant's markers")
	a.requireMarkerIDs()

	// A holder that crashed leaves a lease that expires on its own.
	a.seedMarkers("a-later", 2, 2*time.Hour)
	require.NoError(t, lease.Acquire(ctx, a.tenant, ports.LeaseRestore, "restorer-1", 5*time.Second))
	got, err = sweeper.Prune(ctx, time.Hour, 100)
	require.NoError(t, err)
	assert.Zero(t, got, "a live lease protects the tenant")
	a.requireMarkerIDs("a-later-1", "a-later-2")
	require.Eventually(t, func() bool {
		n, err := sweeper.Prune(ctx, time.Hour, 100)
		return err == nil && n == 2
	}, 15*time.Second, 250*time.Millisecond, "an expired lease no longer protects the tenant")
	a.requireMarkerIDs()
}

// TestMarkerSweeper_LimitBoundsTheWholePass spreads expired markers over two
// tenants: the limit counts every tenant's deletions together, so no pass
// deletes more than limit markers. A tenant with only recent markers is not
// touched at all, not even to create its lease node.
func TestMarkerSweeper_LimitBoundsTheWholePass(t *testing.T) {
	ctx := context.Background()
	a, b, quiet := newUoWFixture(t), newUoWFixture(t), newUoWFixture(t)
	a.seedMarkers("a-old", 6, 2*time.Hour)
	b.seedMarkers("b-old", 6, 2*time.Hour)
	quiet.seedMarkers("quiet-recent", 4, 10*time.Minute)
	sweeper := NewMarkerSweeper(a.client, quietLogger())

	total := func() int {
		ia, err := a.markerIDs()
		require.NoError(t, err)
		ib, err := b.markerIDs()
		require.NoError(t, err)
		return len(ia) + len(ib)
	}
	for i, want := range []int64{8, 4, 0} {
		got, err := sweeper.Prune(ctx, time.Hour, 8)
		require.NoError(t, err, "prune %d", i+1)
		require.Equal(t, want, got, "prune %d", i+1)
	}
	assert.Zero(t, total())
	quiet.requireMarkerIDs("quiet-recent-1", "quiet-recent-2", "quiet-recent-3", "quiet-recent-4")
	assert.Nil(t, quiet.leaseNode(), "a tenant with nothing to sweep is left alone")
}

// TestMarkerSweeper_WaitsForInFlightWrites takes the tenant's export gate the
// way an export's Acquire does: a Prune starts while a write of the tenant is
// in flight and waits for it, so a sweep never deletes between an export
// taking the lease and the writes that preceded it.
func TestMarkerSweeper_WaitsForInFlightWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newUoWFixture(t)
	f.seedMarkers("old", 3, 2*time.Hour)
	sweeper := NewMarkerSweeper(f.client, quietLogger())
	u := NewUnitOfWork(f.client, quietLogger())

	inFn := make(chan struct{})
	proceed := make(chan struct{})
	var release sync.Once
	letWriteFinish := func() { release.Do(func() { close(proceed) }) }
	t.Cleanup(letWriteFinish)
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- u.Write(ctx, f.marker("1762077600000-0", nil), true, func(context.Context, uow.Tx) error {
			close(inFn)
			<-proceed
			return nil
		})
	}()
	select {
	case <-inFn:
	case err := <-writeDone:
		t.Fatalf("the write ended before its fn ran: %v", err)
	}

	type result struct {
		n   int64
		err error
	}
	pruned := make(chan result, 1)
	go func() {
		n, err := sweeper.Prune(ctx, time.Hour, 100)
		pruned <- result{n, err}
	}()
	select {
	case r := <-pruned:
		t.Fatalf("the sweep ran while a write of the tenant was in flight: deleted %d, err %v", r.n, r.err)
	case <-time.After(750 * time.Millisecond):
	}
	f.requireMarkerIDs("old-1", "old-2", "old-3")

	letWriteFinish()
	require.NoError(t, <-writeDone)
	select {
	case r := <-pruned:
		require.NoError(t, r.err)
		assert.Equal(t, int64(3), r.n)
	case <-time.After(15 * time.Second):
		t.Fatal("the sweep did not run after the write committed")
	}
	f.requireMarkerIDs("1762077600000-0")
	f.assertGateUnlocked()
}

// TestMarkerSweeper_ATenantThatFailsDoesNotStopTheOthers breaks tenant A's
// lease node (its until is text, so the gate cannot tell whether a lease is
// held): A's sweep fails and is logged, B's markers go, and the pass returns
// what it deleted with no error, so the sweeper keeps its count.
func TestMarkerSweeper_ATenantThatFailsDoesNotStopTheOthers(t *testing.T) {
	ctx := context.Background()
	a, b := newUoWFixture(t), newUoWFixture(t)
	a.seedMarkers("a-old", 2, 2*time.Hour)
	b.seedMarkers("b-old", 3, 2*time.Hour)
	a.run(`CREATE (:Meta {tenant_id: $tenant_id, key: 'export', holder: 'broken', until: 'not a time'})`, nil)
	var logs syncBuffer
	sweeper := NewMarkerSweeper(a.client, slog.New(slog.NewTextHandler(&logs, nil)))

	got, err := sweeper.Prune(ctx, time.Hour, 1000)
	require.NoError(t, err, "a tenant's failure is logged, not returned")
	assert.GreaterOrEqual(t, got, int64(3), "the count holds what the other tenants' sweeps deleted")
	b.requireMarkerIDs()
	a.requireMarkerIDs("a-old-1", "a-old-2")
	assert.Contains(t, logs.String(), "level=ERROR")
	assert.Contains(t, logs.String(), a.tenant, "the log names the tenant whose sweep failed")
}

// TestMarkerSweeper_ACancelledPassReturnsTheContextsErrorWithoutAnErrorLog
// cancels a pass while it waits on a tenant's export gate, which a write of
// the tenant holds: the pass ends with the context's error and logs no
// error, as a shutdown does.
func TestMarkerSweeper_ACancelledPassReturnsTheContextsErrorWithoutAnErrorLog(t *testing.T) {
	f := newUoWFixture(t)
	f.seedMarkers("old", 2, 2*time.Hour)
	var logs syncBuffer
	sweeper := NewMarkerSweeper(f.client, slog.New(slog.NewTextHandler(&logs, nil)))
	u := NewUnitOfWork(f.client, quietLogger())

	inFn := make(chan struct{})
	proceed := make(chan struct{})
	var release sync.Once
	letWriteFinish := func() { release.Do(func() { close(proceed) }) }
	t.Cleanup(letWriteFinish)
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- u.Write(context.Background(), f.marker("1762077600000-0", nil), true, func(context.Context, uow.Tx) error {
			close(inFn)
			<-proceed
			return nil
		})
	}()
	<-inFn

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := sweeper.Prune(ctx, time.Hour, 1000)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotContains(t, logs.String(), "level=ERROR", "a cancelled pass logs no error")

	letWriteFinish()
	require.NoError(t, <-writeDone)
	f.requireMarkerIDs("1762077600000-0", "old-1", "old-2")
}

// syncBuffer is a bytes.Buffer safe for the concurrent writes of a logger.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestMarkerSweeper_RefusesAnInvalidSweep(t *testing.T) {
	sweeper := NewMarkerSweeper(nil, quietLogger())
	for _, tc := range []struct {
		name      string
		retention time.Duration
		limit     int
	}{
		{"zero retention", 0, 10},
		{"negative retention", -time.Hour, 10},
		{"sub-millisecond retention", time.Microsecond, 10},
		{"zero limit", time.Hour, 0},
		{"negative limit", time.Hour, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := sweeper.Prune(context.Background(), tc.retention, tc.limit)
			assert.Error(t, err)
			assert.Zero(t, n)
		})
	}
}
