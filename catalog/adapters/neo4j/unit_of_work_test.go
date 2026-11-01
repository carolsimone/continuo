//go:build integration

package neo4jinfra

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// integrationClient connects to the test Neo4j and applies the service's
// schema, whose meta_key_unique constraint makes the export gate's MERGE of a
// tenant's lease node race-free.
func integrationClient(t *testing.T) Neo4jClient {
	t.Helper()
	client, err := NewNeo4jClient(
		envOr("NEO4J_URI", "bolt://neo4j:7687"),
		envOr("NEO4J_USER", "neo4j"),
		envOr("NEO4J_PASSWORD", "atlas_password"),
		quietLogger(),
	)
	if err != nil {
		testdeps.Unavailable(t, "Neo4j unavailable, skipping integration test: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	require.NoError(t, InitSchema(context.Background(), client, quietLogger()))
	return client
}

// uowFixture gives one test its own tenant. Every node the code under test
// writes carries that tenant, and the probe nodes a test writes carry its
// test_marker; the cleanup DETACH DELETEs exactly those nodes.
type uowFixture struct {
	t      *testing.T
	client Neo4jClient
	tenant string
}

func newUoWFixture(t *testing.T) *uowFixture {
	t.Helper()
	f := &uowFixture{t: t, client: integrationClient(t), tenant: "uow-test-" + uuid.NewString()}
	t.Cleanup(func() {
		ctx := context.Background()
		s := f.client.NewSession(ctx, neo4j.AccessModeWrite)
		defer func() { _ = s.Close(ctx) }()
		for _, stmt := range []string{
			`MATCH (n:Meta {tenant_id: $tenant_id}) DETACH DELETE n`,
			`MATCH (n:ProcessedMessage {tenant_id: $tenant_id}) DETACH DELETE n`,
			`MATCH (n:UnitOfWorkProbe {test_marker: $test_marker}) DETACH DELETE n`,
		} {
			res, err := s.Run(ctx, stmt, map[string]any{"tenant_id": f.tenant, "test_marker": t.Name()})
			if err == nil {
				_, err = res.Consume(ctx)
			}
			if err != nil {
				t.Errorf("cleanup %q: %v", stmt, err)
			}
		}
	})
	return f
}

func (f *uowFixture) marker(messageID string, outboxEntryID *uuid.UUID) uow.Marker {
	return uow.Marker{TenantID: f.tenant, Group: streams.CatalogReleaseTopology, MessageID: messageID, OutboxEntryID: outboxEntryID}
}

// writeProbe creates one probe node on the open transaction of a Write. It is
// the effect the tests check lands with the marker, or not at all.
func (f *uowFixture) writeProbe(ctx context.Context, tx uow.Tx, name string) error {
	return execCypher(ctx, tx.(*transaction).run,
		`CREATE (:UnitOfWorkProbe {tenant_id: $tenant_id, test_marker: $test_marker, name: $name})`,
		map[string]any{"test_marker": f.t.Name(), "name": name})
}

func (f *uowFixture) count(cypher string) int64 {
	f.t.Helper()
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = s.Close(ctx) }()
	res, err := s.Run(ctx, cypher, map[string]any{"tenant_id": f.tenant})
	require.NoError(f.t, err)
	rec, err := res.Single(ctx)
	require.NoError(f.t, err)
	n, _ := rec.Get("n")
	return n.(int64)
}

func (f *uowFixture) probes() int64 {
	return f.count(`MATCH (n:UnitOfWorkProbe {tenant_id: $tenant_id}) RETURN count(n) AS n`)
}

func (f *uowFixture) markers() int64 {
	return f.count(`MATCH (n:ProcessedMessage {tenant_id: $tenant_id}) RETURN count(n) AS n`)
}

// leaseNode returns the tenant's export lease node's properties, or nil when
// the tenant has none.
func (f *uowFixture) leaseNode() map[string]any {
	f.t.Helper()
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = s.Close(ctx) }()
	res, err := s.Run(ctx, `MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'}) RETURN properties(m) AS p`,
		map[string]any{"tenant_id": f.tenant})
	require.NoError(f.t, err)
	recs, err := res.Collect(ctx)
	require.NoError(f.t, err)
	require.LessOrEqual(f.t, len(recs), 1, "one export lease node per tenant")
	if len(recs) == 0 {
		return nil
	}
	p, _ := recs[0].Get("p")
	return p.(map[string]any)
}

// assertGateUnlocked checks the tenant's lease node exists and carries no
// _lock: the gate's lock property never outlives its transaction.
func (f *uowFixture) assertGateUnlocked() {
	f.t.Helper()
	node := f.leaseNode()
	require.NotNil(f.t, node, "every write leaves the tenant's export lease node behind")
	assert.NotContains(f.t, node, "_lock")
}

func (f *uowFixture) processed(u uow.UnitOfWork, m uow.Marker) bool {
	f.t.Helper()
	ok, err := u.Processed(context.Background(), m)
	require.NoError(f.t, err)
	return ok
}

func TestUnitOfWork_MarkerIsAtomicWithWrites(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	m := f.marker("1762077600000-0", nil)

	boom := errors.New("fn failed after writing")
	err := u.Write(ctx, m, true, func(ctx context.Context, tx uow.Tx) error {
		require.NoError(t, f.writeProbe(ctx, tx, "first"))
		return boom
	})
	require.ErrorIs(t, err, boom, "fn's error is returned as it is")
	assert.Zero(t, f.probes(), "fn's writes roll back with its error")
	assert.Zero(t, f.markers(), "no marker without the data")
	assert.False(t, f.processed(u, m))

	err = u.Write(ctx, m, true, func(ctx context.Context, tx uow.Tx) error {
		return f.writeProbe(ctx, tx, "second")
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), f.probes())
	assert.Equal(t, int64(1), f.markers())
	assert.True(t, f.processed(u, m))
	f.assertGateUnlocked()
}

func TestUnitOfWork_NonFinalWriteRecordsNoMarker(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	m := f.marker("1762077600000-0", nil)

	require.NoError(t, u.Write(ctx, m, false, func(ctx context.Context, tx uow.Tx) error {
		return f.writeProbe(ctx, tx, "batch-1")
	}))
	assert.Equal(t, int64(1), f.probes())
	assert.Zero(t, f.markers())
	assert.False(t, f.processed(u, m), "a non-final batch leaves the message unprocessed")

	require.NoError(t, u.Write(ctx, m, true, func(ctx context.Context, tx uow.Tx) error {
		return f.writeProbe(ctx, tx, "batch-2")
	}))
	assert.Equal(t, int64(2), f.probes())
	assert.Equal(t, int64(1), f.markers())
	assert.True(t, f.processed(u, m))
}

func TestUnitOfWork_FoundMarkerSkipsFn(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	m := f.marker("1762077600000-0", nil)

	require.NoError(t, u.Write(ctx, m, true, func(ctx context.Context, tx uow.Tx) error {
		return f.writeProbe(ctx, tx, "first")
	}))

	for _, final := range []bool{true, false} {
		called := false
		err := u.Write(ctx, m, final, func(ctx context.Context, tx uow.Tx) error {
			called = true
			return f.writeProbe(ctx, tx, "replay")
		})
		require.NoError(t, err, "a processed message is skipped, not failed")
		assert.False(t, called, "fn never runs for a message whose marker exists (final=%v)", final)
	}
	assert.Equal(t, int64(1), f.probes())
	assert.Equal(t, int64(1), f.markers())
}

func TestUnitOfWork_OutboxEntryIDMatchesAcrossMessageIDs(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	entry := uuid.New()

	require.NoError(t, u.Write(ctx, f.marker("1762077600000-0", &entry), true, func(ctx context.Context, tx uow.Tx) error {
		return f.writeProbe(ctx, tx, "first")
	}))

	// The same outbox row re-sent under a fresh stream message id.
	resent := f.marker("1762077700000-0", &entry)
	assert.True(t, f.processed(u, resent))
	called := false
	require.NoError(t, u.Write(ctx, resent, true, func(ctx context.Context, tx uow.Tx) error {
		called = true
		return nil
	}))
	assert.False(t, called, "the re-sent outbox row is not processed twice")

	assert.False(t, f.processed(u, f.marker("1762077700000-0", nil)),
		"a fresh message id without the outbox entry id is a different message")
	other := uuid.New()
	assert.False(t, f.processed(u, f.marker("1762077700000-0", &other)))
	assert.False(t, f.processed(u, uow.Marker{
		TenantID: f.tenant, Group: streams.CatalogReleaseVersions, MessageID: "1762077600000-0", OutboxEntryID: &entry,
	}), "a marker belongs to its group")
	assert.False(t, f.processed(u, uow.Marker{
		TenantID: f.tenant + "-other", Group: streams.CatalogReleaseTopology, MessageID: "1762077600000-0", OutboxEntryID: &entry,
	}), "a marker belongs to its tenant")
	assert.Equal(t, int64(1), f.markers())
}

func TestUnitOfWork_RefusesAnIncompleteMarker(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	noop := func(context.Context, uow.Tx) error { return nil }
	for name, m := range map[string]uow.Marker{
		"no tenant":     {Group: streams.CatalogReleaseTopology, MessageID: "1-0"},
		"no group":      {TenantID: f.tenant, MessageID: "1-0"},
		"no message id": {TenantID: f.tenant, Group: streams.CatalogReleaseTopology},
	} {
		_, err := u.Processed(ctx, m)
		assert.Error(t, err, "Processed: %s", name)
		assert.Error(t, u.Write(ctx, m, true, noop), "Write: %s", name)
	}
	assert.Nil(t, f.leaseNode(), "a refused write opens no transaction")
}

// TestUnitOfWork_TwoPodsRaceOneMarker runs two unit-of-work instances, each on
// its own driver as two catalog pods would, against one message at the same
// moment. The tenant's export gate serialises them: the first to lock it runs
// fn and commits the marker; the second waits on the lock, then finds the
// marker and skips fn. Two rounds cover a tenant whose lease node the race
// creates and one whose node already exists.
func TestUnitOfWork_TwoPodsRaceOneMarker(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	pods := []uow.UnitOfWork{
		NewUnitOfWork(integrationClient(t), quietLogger()),
		NewUnitOfWork(integrationClient(t), quietLogger()),
	}

	for round, messageID := range []string{"1762077600000-0", "1762077700000-0"} {
		require.Equal(t, round == 0, f.leaseNode() == nil, "round %d starts with the lease node absent only the first time", round)
		m := f.marker(messageID, nil)

		type outcome struct {
			err          error
			began, ended time.Time
		}
		var fnCalls atomic.Int32
		results := make([]outcome, len(pods))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, pod := range pods {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				began := time.Now()
				err := pod.Write(ctx, m, true, func(ctx context.Context, tx uow.Tx) error {
					fnCalls.Add(1)
					if err := f.writeProbe(ctx, tx, messageID); err != nil {
						return err
					}
					// Hold the transaction open so the other pod's Write
					// reaches the gate while this one is still in flight.
					time.Sleep(500 * time.Millisecond)
					return nil
				})
				results[i] = outcome{err: err, began: began, ended: time.Now()}
			}()
		}
		close(start)
		wg.Wait()

		require.True(t, results[0].began.Before(results[1].ended) && results[1].began.Before(results[0].ended),
			"round %d: the two writes overlapped in time", round)
		for i, r := range results {
			require.NoError(t, r.err, "round %d: pod %d: the loser finds the winner's marker and skips fn", round, i)
		}
		assert.Equal(t, int32(1), fnCalls.Load(), "round %d: fn ran once", round)
		assert.Equal(t, int64(round+1), f.probes(), "round %d: fn's effect landed once", round)
		assert.Equal(t, int64(round+1), f.markers(), "round %d: one marker", round)
		assert.True(t, f.processed(pods[0], m))
		assert.True(t, f.processed(pods[1], m))
	}
	f.assertGateUnlocked()
}

func TestUnitOfWork_WritesBackOffWhileLeaseHeld(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	lease := NewExportLease(f.client, quietLogger())
	m := f.marker("1762077600000-0", nil)

	require.NoError(t, lease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-1", time.Minute))

	called := false
	err := u.Write(ctx, m, true, func(ctx context.Context, tx uow.Tx) error {
		called = true
		return f.writeProbe(ctx, tx, "paused")
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, uow.ErrWritesPaused), "got %v", err)
	assert.True(t, IsUnavailable(err), "a paused write backs off like an outage, with no dead letter")
	assert.False(t, called, "fn never runs while the lease is held")
	assert.Zero(t, f.probes())
	assert.Zero(t, f.markers())
	assert.False(t, f.processed(u, m))
	f.assertGateUnlocked()
}

func TestUnitOfWork_WriteLandsAfterLeaseRelease(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	lease := NewExportLease(f.client, quietLogger())
	m := f.marker("1762077600000-0", nil)
	probe := func(ctx context.Context, tx uow.Tx) error { return f.writeProbe(ctx, tx, "after-release") }

	require.NoError(t, lease.Acquire(ctx, f.tenant, ports.LeaseRestore, "restorer-1", time.Minute))
	require.ErrorIs(t, u.Write(ctx, m, true, probe), uow.ErrWritesPaused)

	require.NoError(t, lease.Release(ctx, f.tenant, "restorer-1"))
	node := f.leaseNode()
	for _, prop := range []string{"holder", "kind", "until"} {
		assert.NotContains(t, node, prop, "a released lease keeps no %s", prop)
	}

	require.NoError(t, u.Write(ctx, m, true, probe))
	assert.Equal(t, int64(1), f.probes())
	assert.Equal(t, int64(1), f.markers())
	assert.True(t, f.processed(u, m))
	f.assertGateUnlocked()
}

// inflightIDs returns the message ids the tenant's lease node lists as in
// flight, or nil when it lists none.
func (f *uowFixture) inflightIDs() []any {
	f.t.Helper()
	node := f.leaseNode()
	if node == nil {
		return nil
	}
	ids, _ := node["inflight_message_ids"].([]any)
	return ids
}

func (f *uowFixture) writeProbeBatch(u uow.UnitOfWork, m uow.Marker, final bool, name string) {
	f.t.Helper()
	require.NoError(f.t, u.Write(context.Background(), m, final, func(ctx context.Context, tx uow.Tx) error {
		return f.writeProbe(ctx, tx, name)
	}))
}

func TestUnitOfWork_NonFinalWriteMarksItsMessageInFlight(t *testing.T) {
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	a := f.marker("1762077600000-0", nil)

	f.writeProbeBatch(u, a, false, "batch-1")
	node := f.leaseNode()
	assert.Equal(t, []any{"1762077600000-0"}, node["inflight_message_ids"])
	until, ok := node["inflight_until"].(time.Time)
	require.True(t, ok, "inflight_until is a datetime")
	assert.Greater(t, time.Until(until), time.Minute, "the mark lives for the in-flight TTL")
	f.assertGateUnlocked()

	f.writeProbeBatch(u, a, true, "batch-2")
	node = f.leaseNode()
	assert.NotContains(t, node, "inflight_message_ids")
	assert.NotContains(t, node, "inflight_until")
}

func TestUnitOfWork_FinalWriteKeepsOtherMessagesInFlight(t *testing.T) {
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	a := f.marker("1762077600000-0", nil)
	b := f.marker("1762077700000-0", nil)

	f.writeProbeBatch(u, a, false, "a-1")
	f.writeProbeBatch(u, b, false, "b-1")
	f.writeProbeBatch(u, a, true, "a-2")
	assert.Equal(t, []any{"1762077700000-0"}, f.inflightIDs(), "A's last batch leaves B's id")
	assert.Contains(t, f.leaseNode(), "inflight_until")

	f.writeProbeBatch(u, b, true, "b-2")
	node := f.leaseNode()
	assert.NotContains(t, node, "inflight_message_ids")
	assert.NotContains(t, node, "inflight_until")
}

func TestUnitOfWork_RepeatedNonFinalWriteListsItsMessageOnce(t *testing.T) {
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	a := f.marker("1762077600000-0", nil)

	f.writeProbeBatch(u, a, false, "batch-1")
	f.writeProbeBatch(u, a, false, "batch-2")
	assert.Equal(t, []any{"1762077600000-0"}, f.inflightIDs())
}

func TestUnitOfWork_FinalOnlyWriteLeavesNoInFlightMark(t *testing.T) {
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())

	f.writeProbeBatch(u, f.marker("1762077600000-0", nil), true, "only")
	node := f.leaseNode()
	assert.NotContains(t, node, "inflight_message_ids")
	assert.NotContains(t, node, "inflight_until")
}

func TestUnitOfWork_FailedNonFinalWriteLeavesNoInFlightMark(t *testing.T) {
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	boom := errors.New("fn failed")

	err := u.Write(context.Background(), f.marker("1762077600000-0", nil), false, func(ctx context.Context, tx uow.Tx) error {
		return boom
	})
	require.ErrorIs(t, err, boom)
	node := f.leaseNode()
	assert.NotContains(t, node, "inflight_message_ids", "a rolled-back batch marks nothing")
	assert.NotContains(t, node, "inflight_until")
}

// A redelivery under a fresh stream message id finishes the message under that
// id, so the id of the delivery that died mid-way is the one a write must
// find and clear when it finds the marker.
func TestUnitOfWork_FoundMarkerEndsItsInFlightMark(t *testing.T) {
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	entry := uuid.New()
	a := f.marker("1762077600000-0", &entry)
	b := f.marker("1762077700000-0", &entry)

	f.writeProbeBatch(u, a, false, "a-1")
	f.writeProbeBatch(u, b, true, "b-final")
	assert.Equal(t, []any{"1762077600000-0"}, f.inflightIDs(), "B's final write does not clear A's id")

	called := false
	require.NoError(t, u.Write(context.Background(), a, true, func(ctx context.Context, tx uow.Tx) error {
		called = true
		return nil
	}))
	assert.False(t, called, "A finds B's marker by the outbox entry id and skips fn")
	node := f.leaseNode()
	assert.NotContains(t, node, "inflight_message_ids")
	assert.NotContains(t, node, "inflight_until")
	f.assertGateUnlocked()
}

func TestUnitOfWork_WritesPausedFollowsTheLease(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	lease := NewExportLease(f.client, quietLogger())
	paused := func() bool {
		ok, err := u.WritesPaused(ctx, f.tenant)
		require.NoError(t, err)
		return ok
	}

	assert.False(t, paused(), "a tenant with no lease node is not paused")
	require.NoError(t, lease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-1", time.Minute))
	assert.True(t, paused())
	require.NoError(t, lease.Release(ctx, f.tenant, "exporter-1"))
	assert.False(t, paused())
}
