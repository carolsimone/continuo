//go:build integration

package neo4jinfra

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLease_SecondAcquireIsHeld(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	lease := NewExportLease(f.client, quietLogger())

	require.NoError(t, lease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-1", time.Minute))
	assert.ErrorIs(t, lease.Acquire(ctx, f.tenant, ports.LeaseRestore, "restorer-1", time.Minute), ports.ErrLeaseHeld)
	assert.ErrorIs(t, lease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-1", time.Minute), ports.ErrLeaseHeld,
		"the holder renews its lease; it cannot acquire it twice")

	node := f.leaseNode()
	assert.Equal(t, "exporter-1", node["holder"])
	assert.Equal(t, string(ports.LeaseExport), node["kind"])
	assert.NotContains(t, node, "_lock")
}

// TestLease_ConcurrentAcquiresGrantOne starts several Acquires of one tenant's
// lease at once: Acquire reads the lease only after it holds the node's lock,
// so exactly one is granted and every other one is ErrLeaseHeld.
func TestLease_ConcurrentAcquiresGrantOne(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	// The contenders race on an existing lease node, as they do once the
	// tenant has written anything.
	require.NoError(t, NewUnitOfWork(f.client, quietLogger()).Write(ctx, f.marker("1762077500000-0", nil), true,
		func(context.Context, uow.Tx) error { return nil }))

	const contenders = 4
	results := make([]error, contenders)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range contenders {
		lease := NewExportLease(integrationClient(t), quietLogger())
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = lease.Acquire(ctx, f.tenant, ports.LeaseExport, fmt.Sprintf("exporter-%d", i), time.Minute)
		}()
	}
	close(start)
	wg.Wait()

	granted := ""
	for i, err := range results {
		if err == nil {
			require.Empty(t, granted, "a second contender was granted the lease")
			granted = fmt.Sprintf("exporter-%d", i)
			continue
		}
		require.ErrorIs(t, err, ports.ErrLeaseHeld, "contender %d", i)
	}
	require.NotEmpty(t, granted, "one contender is granted the lease")
	assert.Equal(t, granted, f.leaseNode()["holder"])
}

// TestLease_TenantsHoldSeparateGatesAndLeases runs two tenants side by side.
// Each tenant's gate and lease live on its own (:Meta {tenant_id, key:
// 'export'}) node, so the tenants' first writes create their lease nodes at
// the same moment, one tenant writes while the other's write holds its gate,
// both take their leases at once, and a lease one tenant holds pauses only
// that tenant's writes.
func TestLease_TenantsHoldSeparateGatesAndLeases(t *testing.T) {
	ctx := context.Background()
	a, b := newUoWFixture(t), newUoWFixture(t)
	u := NewUnitOfWork(a.client, quietLogger())
	probe := func(f *uowFixture, name string) func(context.Context, uow.Tx) error {
		return func(ctx context.Context, tx uow.Tx) error { return f.writeProbe(ctx, tx, name) }
	}
	require.Nil(t, a.leaseNode())
	require.Nil(t, b.leaseNode())

	// Tenant a's first write creates its lease node and holds the gate open.
	inA := make(chan struct{})
	proceed := make(chan struct{})
	letAFinish := sync.OnceFunc(func() { close(proceed) })
	t.Cleanup(letAFinish)
	aDone := make(chan error, 1)
	go func() {
		aDone <- u.Write(ctx, a.marker("1762077600000-0", nil), true, func(ctx context.Context, tx uow.Tx) error {
			if err := a.writeProbe(ctx, tx, "a-in-flight"); err != nil {
				return err
			}
			close(inA)
			<-proceed
			return nil
		})
	}()
	select {
	case <-inA:
	case err := <-aDone:
		t.Fatalf("tenant a's write ended before its fn ran: %v", err)
	}

	// Tenant b's first write creates b's lease node and commits meanwhile.
	bDone := make(chan error, 1)
	go func() { bDone <- u.Write(ctx, b.marker("1762077600000-0", nil), true, probe(b, "b-first")) }()
	select {
	case err := <-bDone:
		require.NoError(t, err, "tenant b writes while tenant a's write holds a's gate")
	case <-time.After(10 * time.Second):
		t.Fatal("tenant b's write waited for tenant a's gate")
	}
	letAFinish()
	require.NoError(t, <-aDone)

	// Both tenants take their leases at the same moment.
	holders := map[*uowFixture]string{a: "exporter-a", b: "exporter-b"}
	errs := make(chan error, len(holders))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for f, holder := range holders {
		lease := NewExportLease(integrationClient(t), quietLogger())
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := lease.Acquire(ctx, f.tenant, ports.LeaseExport, holder, time.Minute); err != nil {
				errs <- fmt.Errorf("tenant %s: %w", f.tenant, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err, "a tenant's lease is granted whatever another tenant holds")
	}
	assert.Equal(t, "exporter-a", a.leaseNode()["holder"])
	assert.Equal(t, "exporter-b", b.leaseNode()["holder"])

	// Tenant b's export ends; tenant a's lease pauses tenant a's writes only.
	lease := NewExportLease(a.client, quietLogger())
	require.NoError(t, lease.Release(ctx, b.tenant, "exporter-b"))
	require.NoError(t, u.Write(ctx, b.marker("1762077700000-0", nil), true, probe(b, "b-second")),
		"tenant a's lease does not pause tenant b's writes")
	require.ErrorIs(t, u.Write(ctx, a.marker("1762077700000-0", nil), true, probe(a, "a-paused")), uow.ErrWritesPaused)

	assert.Equal(t, int64(1), a.probes())
	assert.Equal(t, int64(1), a.markers())
	assert.Equal(t, int64(2), b.probes())
	assert.Equal(t, int64(2), b.markers())
	assert.Equal(t, "exporter-a", a.leaseNode()["holder"], "tenant b's release leaves tenant a's lease alone")
	a.assertGateUnlocked()
	b.assertGateUnlocked()
}

func TestLease_RefusesAnInvalidRequest(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	lease := NewExportLease(f.client, quietLogger())

	assert.Error(t, lease.Acquire(ctx, "", ports.LeaseExport, "exporter-1", time.Minute), "no tenant")
	assert.Error(t, lease.Acquire(ctx, f.tenant, ports.LeaseKind("backup"), "exporter-1", time.Minute), "unknown kind")
	assert.Error(t, lease.Acquire(ctx, f.tenant, ports.LeaseExport, "", time.Minute), "no holder")
	assert.Error(t, lease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-1", 0), "no ttl")
	assert.Error(t, lease.Renew(ctx, f.tenant, "exporter-1", 0), "no ttl")
	assert.Nil(t, f.leaseNode(), "a refused request writes nothing")
}

func TestLease_RenewByOtherHolderIsLost(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	lease := NewExportLease(f.client, quietLogger())

	assert.ErrorIs(t, lease.Renew(ctx, f.tenant, "exporter-1", time.Minute), ports.ErrLeaseLost,
		"a tenant with no lease has nothing to renew")

	require.NoError(t, lease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-1", 10*time.Second))
	before := f.leaseNode()["until"].(time.Time)

	assert.ErrorIs(t, lease.Renew(ctx, f.tenant, "exporter-2", time.Hour), ports.ErrLeaseLost)
	assert.Equal(t, before, f.leaseNode()["until"].(time.Time), "a refused renewal leaves the lease as it was")
	assert.Equal(t, "exporter-1", f.leaseNode()["holder"])

	require.NoError(t, lease.Renew(ctx, f.tenant, "exporter-1", time.Hour))
	after := f.leaseNode()["until"].(time.Time)
	assert.True(t, after.After(before.Add(30*time.Minute)), "the holder's renewal moves until to the new ttl: %v -> %v", before, after)
	assert.NotContains(t, f.leaseNode(), "_lock")
}

// TestLease_ExpiresWhenHolderCrashes takes a two-second lease and never
// renews it, as an exporter that crashed would leave it. While it is live,
// writes back off and a second holder's Acquire is refused, and the test
// checks the refusal came before the lease's ttl ran out, so a stall of the
// test cannot pass it. The second holder then polls until the lease's until
// passes on the database clock and takes it over; the crashed holder can
// neither renew nor release it. Once the second holder releases it, writes
// land.
func TestLease_ExpiresWhenHolderCrashes(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	lease := NewExportLease(f.client, quietLogger())
	probe := func(ctx context.Context, tx uow.Tx) error { return f.writeProbe(ctx, tx, "after-expiry") }
	const ttl = 2 * time.Second

	start := time.Now()
	require.NoError(t, lease.Acquire(ctx, f.tenant, ports.LeaseExport, "crashed-exporter", ttl))
	require.ErrorIs(t, u.Write(ctx, f.marker("1762077600000-0", nil), true, probe), uow.ErrWritesPaused)
	err := lease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-2", time.Minute)
	refusedAfter := time.Since(start)
	require.ErrorIs(t, err, ports.ErrLeaseHeld, "a live lease, though its holder crashed, is not taken over")
	require.Less(t, refusedAfter, ttl, "the refusal came while the lease was live, so it proves the lease held")

	require.Eventually(t, func() bool {
		return lease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-2", time.Minute) == nil
	}, 15*time.Second, 100*time.Millisecond, "an expired lease is taken over")
	assert.GreaterOrEqual(t, time.Since(start), ttl, "the lease was not taken over before it expired")

	assert.ErrorIs(t, lease.Renew(ctx, f.tenant, "crashed-exporter", time.Minute), ports.ErrLeaseLost,
		"an expired lease cannot be renewed: writes may have landed since it lapsed")
	require.NoError(t, lease.Release(ctx, f.tenant, "crashed-exporter"))
	assert.Equal(t, "exporter-2", f.leaseNode()["holder"], "a release by the old holder leaves the new lease alone")
	require.ErrorIs(t, u.Write(ctx, f.marker("1762077600000-0", nil), true, probe), uow.ErrWritesPaused)

	require.NoError(t, lease.Release(ctx, f.tenant, "exporter-2"))
	require.NoError(t, u.Write(ctx, f.marker("1762077600000-0", nil), true, probe), "a released lease no longer pauses writes")
	assert.Equal(t, int64(1), f.probes())
	assert.Equal(t, int64(1), f.markers())
}

// TestLease_AcquireWaitsForInFlightWrite holds a write's transaction open and
// checks that Acquire blocks on the tenant's lease node until that write
// commits: an export never starts while a write is half done.
func TestLease_AcquireWaitsForInFlightWrite(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	u := NewUnitOfWork(f.client, quietLogger())
	lease := NewExportLease(f.client, quietLogger())

	// Create the tenant's lease node first, so the in-flight write below
	// locks an existing node.
	require.NoError(t, u.Write(ctx, f.marker("1762077500000-0", nil), true, func(context.Context, uow.Tx) error { return nil }))

	inFn := make(chan struct{})
	proceed := make(chan struct{})
	letWriteFinish := sync.OnceFunc(func() { close(proceed) })
	t.Cleanup(letWriteFinish)

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- u.Write(ctx, f.marker("1762077600000-0", nil), true, func(ctx context.Context, tx uow.Tx) error {
			if err := f.writeProbe(ctx, tx, "in-flight"); err != nil {
				return err
			}
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

	acquired := make(chan error, 1)
	go func() { acquired <- lease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-1", time.Minute) }()
	select {
	case err := <-acquired:
		t.Fatalf("Acquire returned (%v) while a write held the tenant's lease node", err)
	case <-time.After(750 * time.Millisecond):
	}

	letWriteFinish()
	require.NoError(t, <-writeDone, "the in-flight write commits")
	require.NoError(t, <-acquired, "Acquire takes the lease once the write has committed")

	assert.Equal(t, int64(1), f.probes(), "the write that was in flight landed before the export began")
	assert.Equal(t, int64(2), f.markers())
	assert.Equal(t, "exporter-1", f.leaseNode()["holder"])
	require.ErrorIs(t, u.Write(ctx, f.marker("1762077700000-0", nil), true, func(context.Context, uow.Tx) error { return nil }),
		uow.ErrWritesPaused, "a write that starts after the lease is taken backs off")
}
