//go:build integration

package neo4jinfra

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPromoteRelease_NewerSeqRecordsSeqOnMeta(t *testing.T) {
	f := newSwapFixture(t)
	now := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)

	assert.Equal(t, topology.PromotionApplied, f.promoteAt("rel-1", 3, releaseOf(modelNode("a")), now))

	rid, seq := f.liveMeta()
	assert.Equal(t, "rel-1", rid)
	assert.Equal(t, int64(3), seq)
	updated, ok := f.rows(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'current_release'}) RETURN m.updated_at AS v`, nil)[0].Get("v")
	require.True(t, ok)
	assert.True(t, now.Equal(updated.(time.Time)), "updated_at is the promotion's time")
}

// The tenant's :Meta alone holds the live seq: the swap writes no
// :TopologyRoot.
func TestPromoteRelease_WritesNoTopologyRoot(t *testing.T) {
	f := newSwapFixture(t)
	topologyRoots := func() int64 { return f.count(`MATCH (r:TopologyRoot) RETURN count(r) AS n`, nil) }
	require.Zero(t, topologyRoots(), "the startup migration deleted every :TopologyRoot")

	f.promote("rel-1", releaseOf(modelNode("a")), time.Now().UTC())
	f.promote("rel-2", releaseOf(modelNode("b")), time.Now().UTC())

	assert.Zero(t, topologyRoots())
}

// A promotion that is not newer than the live one writes only its marker: the
// tenant's graph is the same before and after, and the message reads as
// processed.
func TestPromoteRelease_StaleAndRedeliveredWriteOnlyTheMarker(t *testing.T) {
	cases := []struct {
		name      string
		releaseID string
		seq       int64
		nodes     []topology.ReleasePromotedTopologyNode
		want      topology.PromotionOutcome
	}{
		{"older seq", "rel-old", 7, releaseOf(modelNode("old")), topology.PromotionStale},
		// The redelivery carries no nodes: applied, it would retire the live
		// topology.
		{"same seq and release", "rel-live", 8, nil, topology.PromotionRedelivered},
		{"same seq of another release", "rel-other", 8, releaseOf(modelNode("other")), topology.PromotionStale},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSwapFixture(t)
			now := time.Now().UTC()
			f.promoteAt("rel-prev", 6, releaseOf(modelNode("dropped")), now)
			require.Equal(t, topology.PromotionApplied, f.promoteAt("rel-live", 8, releaseOf(modelNode("a"), modelNode("b", "a")), now))
			before := f.snapshot()
			markersBefore := f.count(`MATCH (p:ProcessedMessage {tenant_id: $tenant_id}) RETURN count(p) AS n`, nil)

			m := f.marker()
			outcome, err := f.tryPromoteAt(m, tc.releaseID, tc.seq, tc.nodes, now.Add(time.Hour))
			require.NoError(t, err)
			assert.Equal(t, tc.want, outcome)

			assert.Equal(t, before, f.snapshot(), "the graph is unchanged")
			assert.Equal(t, markersBefore+1, f.count(`MATCH (p:ProcessedMessage {tenant_id: $tenant_id}) RETURN count(p) AS n`, nil),
				"the promotion's marker is its only write")
			assert.True(t, f.processed(m))
			rid, seq := f.liveMeta()
			assert.Equal(t, "rel-live", rid)
			assert.Equal(t, int64(8), seq)
		})
	}
}

// A current_release node with a release but no seq takes the first promotion
// that carries one.
func TestPromoteRelease_MetaWithoutSeqAcceptsTheFirstPromotion(t *testing.T) {
	f := newSwapFixture(t)
	f.exec(`CREATE (:Meta {tenant_id: $tenant_id, key: 'current_release', release_id: 'rel-live'})`, nil)

	assert.Equal(t, topology.PromotionApplied, f.promoteAt("rel-live", 1, releaseOf(modelNode("a")), time.Now().UTC()))
	_, seq := f.liveMeta()
	assert.Equal(t, int64(1), seq)
}

// Two promotions of one tenant racing through the unit of work: the newer one
// ends live whichever starts first, and the older one never reverts it.
func TestPromoteRelease_ConcurrentPromotionsKeepTheNewestLive(t *testing.T) {
	first := newSwapFixture(t)
	for i := 0; i < 10; i++ {
		f := first.another()
		var wg sync.WaitGroup
		start := make(chan struct{})
		outcomes := make([]topology.PromotionOutcome, 2)
		errs := make([]error, 2)
		for j, p := range []struct {
			releaseID string
			seq       int64
		}{{"rel-old", 1}, {"rel-new", 2}} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				outcomes[j], errs[j] = f.tryPromoteAt(f.marker(), p.releaseID, p.seq, releaseOf(modelNode(p.releaseID)), time.Now().UTC())
			}()
		}
		close(start)
		wg.Wait()
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])

		assert.Equal(t, topology.PromotionApplied, outcomes[1], "iteration %d: the newer promotion always applies", i)
		rid, seq := f.liveMeta()
		assert.Equal(t, "rel-new", rid, "iteration %d", i)
		assert.Equal(t, int64(2), seq, "iteration %d", i)
		assert.Equal(t, []string{"rel-new"}, f.activeIDs(), "iteration %d", i)
	}
}

// swapResult is what one promotion run outside the unit of work returned.
type swapResult struct {
	outcome topology.PromotionOutcome
	err     error
}

// swapInTx runs one promotion of fx's tenant on a bare write transaction,
// without the unit of work's export gate. swapped, when set, is closed once
// the swap's statements ran; hold, when set, keeps the transaction open after
// that until hold is closed.
func swapInTx(fx *swapFixture, releaseID string, seq int64, swapped chan<- struct{}, hold <-chan struct{}) <-chan swapResult {
	ctx := context.Background()
	done := make(chan swapResult, 1)
	go func() {
		var outcome topology.PromotionOutcome
		err := inWriteTx(ctx, fx.client, fx.tenant, func(run TenantRunner) (bool, error) {
			tx := &transaction{run: run, logger: quietLogger()}
			var err error
			outcome, err = tx.Topology().PromoteRelease(ctx, fx.tenant, releaseID, seq, releaseOf(modelNode(releaseID)), time.Now().UTC())
			if err != nil {
				return false, err
			}
			if swapped != nil {
				close(swapped)
			}
			if hold != nil {
				<-hold
			}
			return true, nil
		})
		done <- swapResult{outcome: outcome, err: err}
	}()
	return done
}

// metaRace starts the holder's promotion, waits until it swapped and holds
// its transaction open, then starts the waiter's. It checks the waiter does
// not finish while the holder's transaction is open, lets the holder commit,
// and returns both results. meanwhile, when set, runs while both are in
// flight.
func metaRace(t *testing.T, f *swapFixture, holderRelease string, holderSeq int64, waiterRelease string, waiterSeq int64, meanwhile func()) (holder, waiter swapResult) {
	t.Helper()
	swapped := make(chan struct{})
	hold := make(chan struct{})
	var once sync.Once
	commitHolder := func() { once.Do(func() { close(hold) }) }
	// Registered after the fixture's cleanup, so it runs first: a failed
	// check never leaves a transaction open over the nodes the cleanup deletes.
	t.Cleanup(commitHolder)

	first := swapInTx(f, holderRelease, holderSeq, swapped, hold)
	select {
	case <-swapped:
	case r := <-first:
		t.Fatalf("the holder's promotion ended before it held its lock: %+v", r)
	case <-time.After(10 * time.Second):
		t.Fatal("the holder's promotion did not swap")
	}

	second := swapInTx(f, waiterRelease, waiterSeq, nil, nil)
	if meanwhile != nil {
		meanwhile()
	}
	select {
	case r := <-second:
		t.Fatalf("the waiter's promotion finished while the holder's transaction held current_release: %+v", r)
	case <-time.After(750 * time.Millisecond):
	}

	commitHolder()
	holder = <-first
	require.NoError(t, holder.err)
	select {
	case waiter = <-second:
	case <-time.After(10 * time.Second):
		t.Fatal("the waiter's promotion did not resume after the holder committed")
	}
	require.NoError(t, waiter.err)
	return holder, waiter
}

// TestPromoteRelease_SecondPromotionBlocksOnMetaLock runs the swap on bare
// write transactions, without the unit of work's export gate, so the tenant's
// current_release node is the only lock two promotions share.
//
// On a graph with no current_release node yet, the second promotion waits for
// the first to commit and then applies over it: both apply, once each, in seq
// order, and the tenant ends with one current_release node. A promotion of
// another tenant meanwhile waits for nothing and gets a node of its own. Once
// the node exists, an older promotion that arrives while a newer one holds it
// waits too, then reads the newer seq and leaves the topology alone.
func TestPromoteRelease_SecondPromotionBlocksOnMetaLock(t *testing.T) {
	f := newSwapFixture(t)
	other := f.another()
	metaNodes := func(fx *swapFixture) int64 {
		return fx.count(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'current_release'}) RETURN count(m) AS n`, nil)
	}
	require.Zero(t, metaNodes(f), "the tenant starts with no current_release node")
	require.Zero(t, metaNodes(other))

	first, second := metaRace(t, f, "r1", 1, "r2", 2, func() {
		select {
		case r := <-swapInTx(other, "o1", 1, nil, nil):
			require.NoError(t, r.err)
			assert.Equal(t, topology.PromotionApplied, r.outcome, "another tenant's promotion does not wait")
		case <-time.After(10 * time.Second):
			t.Fatal("another tenant's promotion waited for the first tenant's lock")
		}
	})
	assert.Equal(t, topology.PromotionApplied, first.outcome)
	assert.Equal(t, topology.PromotionApplied, second.outcome, "the second promotion read the seq the first wrote and applied over it")
	assert.Equal(t, int64(1), metaNodes(f), "one current_release node per tenant")
	rid, seq := f.liveMeta()
	assert.Equal(t, "r2", rid)
	assert.Equal(t, int64(2), seq)
	assert.Equal(t, []string{"r2"}, f.activeIDs())
	assert.Equal(t, []string{"r1", "r2"}, f.nodeIDs(), "the first release's node is retired, not deleted")

	assert.Equal(t, int64(1), metaNodes(other))
	rid, seq = other.liveMeta()
	assert.Equal(t, "o1", rid)
	assert.Equal(t, int64(1), seq)

	newer, older := metaRace(t, f, "r4", 4, "r3", 3, nil)
	assert.Equal(t, topology.PromotionApplied, newer.outcome)
	assert.Equal(t, topology.PromotionStale, older.outcome, "the older promotion read the newer seq once the lock was free")
	rid, seq = f.liveMeta()
	assert.Equal(t, "r4", rid)
	assert.Equal(t, int64(4), seq)
	assert.Equal(t, []string{"r4"}, f.activeIDs())
}
