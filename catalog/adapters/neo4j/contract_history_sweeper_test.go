//go:build integration

package neo4jinfra

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/service/ports"
	pkgretention "github.com/carolsimone/continuo/pkg/retention"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Prune is the retention sweeper's prune function.
var _ pkgretention.PruneFunc = (*ContractHistorySweeper)(nil).Prune

// The test graph is shared with every other integration suite, and a sweep
// reaches every tenant that holds history. A retention of ten years, with
// seeded edges older than that, keeps a test's sweep off every fixture but its
// own.
const (
	historyRetention = 3650 * 24 * time.Hour
	ancient          = 4000 * 24 * time.Hour
	recent           = 24 * time.Hour

	hourlyNode = "analytics.py_hourly_kpis"
	freshNode  = "analytics.py_fresh_kpis"
)

// historyHash is the contract hash of the i-th distinct contract of a node.
func historyHash(i int) string { return fmt.Sprintf("sha256:%04x", i) }

// changeContracts promotes one release, under the next seq, that gives each of
// uniqueIDs the contract named by hash.
func (f *swapFixture) changeContracts(hash string, uniqueIDs ...string) {
	f.t.Helper()
	f.seq++
	in := make([]contractversion.Incoming, 0, len(uniqueIDs))
	for _, id := range uniqueIDs {
		in = append(in, contractOf(id, hash, "owner-"+hash))
	}
	f.writeContracts(contractPromotion(fmt.Sprintf("rel-%d", f.seq), f.seq), in...)
}

// writeHistory gives uniqueID one change for each hash, in order, so the
// i-th hash is promoted as version i.
func (f *swapFixture) writeHistory(uniqueID string, hashes ...string) {
	f.t.Helper()
	for _, h := range hashes {
		f.changeContracts(h, uniqueID)
	}
}

// ageEdges sets the promoted_at of uniqueID's edge of each version in ages to
// that long before the database's now.
func (f *swapFixture) ageEdges(uniqueID string, ages map[int64]time.Duration) {
	f.t.Helper()
	for version, age := range ages {
		f.exec(`
			MATCH (:Node {tenant_id: $tenant_id, unique_id: $uid})-[e:CONTRACT_PROMOTED {version: $version}]->(:ContractVersion {tenant_id: $tenant_id})
			SET e.promoted_at = datetime() - duration({milliseconds: $age_ms})`,
			map[string]any{"uid": uniqueID, "version": version, "age_ms": age.Milliseconds()})
	}
}

// ageAllEdges makes every edge of uniqueID's history, versions 1 to n, age old.
func (f *swapFixture) ageAllEdges(uniqueID string, n int, age time.Duration) {
	f.t.Helper()
	ages := make(map[int64]time.Duration, n)
	for v := 1; v <= n; v++ {
		ages[int64(v)] = age
	}
	f.ageEdges(uniqueID, ages)
}

// edgeVersions lists the versions of uniqueID's :CONTRACT_PROMOTED edges.
func (f *swapFixture) edgeVersions(uniqueID string) []int64 {
	f.t.Helper()
	out := []int64{}
	for _, e := range f.promotions(uniqueID) {
		out = append(out, e.props["version"].(int64))
	}
	return out
}

// storedHashes lists the contract hashes uniqueID's :ContractVersion nodes
// hold, sorted.
func (f *swapFixture) storedHashes(uniqueID string) []string {
	f.t.Helper()
	return f.column(`
		MATCH (v:ContractVersion {tenant_id: $tenant_id, unique_id: $uid}) RETURN v.contract_hash AS v ORDER BY v`,
		map[string]any{"uid": uniqueID})
}

func (f *swapFixture) requireGateUnlocked() {
	f.t.Helper()
	recs := f.rows(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'}) RETURN properties(m) AS p`, nil)
	require.Len(f.t, recs, 1, "a sweep that took the gate leaves the tenant's lease node behind")
	p, _ := recs[0].Get("p")
	assert.NotContains(f.t, p.(map[string]any), "_lock", "the gate's lock never outlives the sweep's transaction")
}

// TestContractHistorySweeper_KeepsEachNodesTwoNewestEdges ages a node's six
// edges beyond the retention: its two newest stay, the current contract and
// the one before it, with their versions, and the rest go with the versions
// nothing reaches. A node with two old edges and a recent one keeps its two
// newest, and a node whose edges are all recent loses nothing.
func TestContractHistorySweeper_KeepsEachNodesTwoNewestEdges(t *testing.T) {
	ctx := context.Background()
	f := newSwapFixture(t)
	for _, id := range []string{kpisNode, hourlyNode, freshNode} {
		f.seedLiveNode(id)
	}
	for i := 1; i <= 6; i++ {
		f.changeContracts(historyHash(i), kpisNode)
	}
	for i := 1; i <= 3; i++ {
		f.changeContracts(historyHash(i), hourlyNode, freshNode)
	}
	f.ageAllEdges(kpisNode, 6, ancient)
	f.ageEdges(hourlyNode, map[int64]time.Duration{1: ancient, 2: ancient, 3: recent})
	f.ageAllEdges(freshNode, 3, recent)
	sweeper := NewContractHistorySweeper(f.client, quietLogger())

	deleted, err := sweeper.Prune(ctx, historyRetention, 1000)
	require.NoError(t, err)
	assert.Equal(t, int64(4+4+1+1), deleted, "four edges and four versions of one node, one of each of the other")

	assert.Equal(t, []int64{5, 6}, f.edgeVersions(kpisNode))
	assert.Equal(t, []string{historyHash(5), historyHash(6)}, f.storedHashes(kpisNode))
	hash, pointer := f.contractPointer(kpisNode)
	assert.Equal(t, historyHash(6), hash, "the current contract is untouched")
	assert.Equal(t, int64(6), pointer["version"])

	assert.Equal(t, []int64{2, 3}, f.edgeVersions(hourlyNode), "an old edge that is among the two newest stays")
	assert.Equal(t, []string{historyHash(2), historyHash(3)}, f.storedHashes(hourlyNode))
	assert.Equal(t, []int64{1, 2, 3}, f.edgeVersions(freshNode))
	assert.Equal(t, []string{historyHash(1), historyHash(2), historyHash(3)}, f.storedHashes(freshNode))

	again, err := sweeper.Prune(ctx, historyRetention, 1000)
	require.NoError(t, err)
	assert.Zero(t, again, "a second sweep has nothing left to delete")
	f.requireGateUnlocked()
}

// TestContractHistorySweeper_NeverDeletesTheEdgeThePointerNames points a
// node's current contract at its oldest edge, as no promotion does: that edge
// stays whatever its age, beside the node's two newest.
func TestContractHistorySweeper_NeverDeletesTheEdgeThePointerNames(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.writeHistory(kpisNode, historyHash(1), historyHash(2), historyHash(3), historyHash(4))
	f.exec(`
		MATCH (n:Node {tenant_id: $tenant_id, unique_id: $uid})-[cur:CURRENT_CONTRACT]->(:ContractVersion)
		MATCH (n)-[e:CONTRACT_PROMOTED {version: 1}]->(v:ContractVersion)
		DELETE cur
		CREATE (n)-[:CURRENT_CONTRACT {version: 1, promotion_seq: e.promotion_seq, release_id: e.release_id, promoted_at: e.promoted_at}]->(v)`,
		map[string]any{"uid": kpisNode})
	f.ageAllEdges(kpisNode, 4, ancient)
	sweeper := NewContractHistorySweeper(f.client, quietLogger())

	deleted, err := sweeper.Prune(context.Background(), historyRetention, 1000)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted, "the second edge and its version")

	assert.Equal(t, []int64{1, 3, 4}, f.edgeVersions(kpisNode))
	assert.Equal(t, []string{historyHash(1), historyHash(3), historyHash(4)}, f.storedHashes(kpisNode))
	hash, pointer := f.contractPointer(kpisNode)
	assert.Equal(t, historyHash(1), hash)
	assert.Equal(t, int64(1), pointer["version"])
}

// TestContractHistorySweeper_DeletesUnreachedVersionsOnly sweeps a node that
// went back to an earlier contract: the edge of its first version is deleted,
// but the contract it named is still the current one and stays. A version no
// edge reaches goes, a version only a pointer reaches stays, and so does every
// version a recent edge reaches, in the tenant's own graph or another's.
func TestContractHistorySweeper_DeletesUnreachedVersionsOnly(t *testing.T) {
	ctx := context.Background()
	f := newSwapFixture(t)
	other := f.another()
	f.seedLiveNode(kpisNode)
	f.writeHistory(kpisNode, historyHash(1), historyHash(2), historyHash(1))
	f.ageAllEdges(kpisNode, 3, ancient)
	other.seedLiveNode(kpisNode)
	other.writeHistory(kpisNode, historyHash(1), historyHash(2), historyHash(3))
	other.ageAllEdges(kpisNode, 3, recent)

	f.exec(`CREATE (:ContractVersion {tenant_id: $tenant_id, unique_id: 'analytics.orphan', contract_hash: 'sha256:dead', document: '{}'})`, nil)
	f.exec(`
		CREATE (n:Node {tenant_id: $tenant_id, unique_id: 'analytics.pointer_only', active: true})
		CREATE (n)-[:CURRENT_CONTRACT {version: 1}]->(:ContractVersion {tenant_id: $tenant_id, unique_id: 'analytics.pointer_only', contract_hash: 'sha256:beef', document: '{}'})`, nil)
	sweeper := NewContractHistorySweeper(f.client, quietLogger())

	deleted, err := sweeper.Prune(ctx, historyRetention, 1000)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted, "the first version's edge, and the version nothing reaches")

	assert.Equal(t, []int64{2, 3}, f.edgeVersions(kpisNode))
	assert.Equal(t, []string{historyHash(1), historyHash(2)}, f.storedHashes(kpisNode),
		"the first contract is still reached by the revert's edge and by the pointer")
	assert.Zero(t, f.count(`MATCH (v:ContractVersion {tenant_id: $tenant_id, unique_id: 'analytics.orphan'}) RETURN count(v) AS n`, nil))
	assert.Equal(t, int64(1), f.count(`MATCH (v:ContractVersion {tenant_id: $tenant_id, unique_id: 'analytics.pointer_only'}) RETURN count(v) AS n`, nil))

	assert.Equal(t, []int64{1, 2, 3}, other.edgeVersions(kpisNode))
	assert.Equal(t, []string{historyHash(1), historyHash(2), historyHash(3)}, other.storedHashes(kpisNode))
}

// TestContractHistorySweeper_TheCounterKeepsRising prunes a node's history
// and promotes more changes: each takes the number above the newest edge,
// which a sweep never deletes. A revert to a contract the sweep kept names its
// earliest remaining version, and a revert to one it deleted is recorded as
// a change that repeats none.
func TestContractHistorySweeper_TheCounterKeepsRising(t *testing.T) {
	ctx := context.Background()
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.writeHistory(kpisNode, historyHash(1), historyHash(2), historyHash(3), historyHash(4))
	f.ageAllEdges(kpisNode, 4, ancient)
	sweeper := NewContractHistorySweeper(f.client, quietLogger())

	_, err := sweeper.Prune(ctx, historyRetention, 1000)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, f.edgeVersions(kpisNode))
	assert.Equal(t, map[string]contractversion.NodeState{kpisNode: {
		UniqueID:           kpisNode,
		Watermark:          f.seq,
		CurrentHash:        historyHash(4),
		CurrentVersion:     4,
		MaxVersion:         4,
		FirstVersionByHash: map[string]int64{historyHash(3): 3, historyHash(4): 4},
	}}, f.contractStates(kpisNode), "what a promotion reads of the node is what it read before the sweep, less the deleted versions")

	f.changeContracts(historyHash(5), kpisNode)
	f.changeContracts(historyHash(3), kpisNode)
	f.changeContracts(historyHash(1), kpisNode)

	type change struct {
		version int64
		hash    string
		sameAs  any
	}
	var got []change
	for _, e := range f.promotions(kpisNode) {
		got = append(got, change{e.props["version"].(int64), e.hash, e.props["same_as_version"]})
	}
	assert.Equal(t, []change{
		{3, historyHash(3), nil},
		{4, historyHash(4), nil},
		{5, historyHash(5), nil},
		{6, historyHash(3), int64(3)},
		{7, historyHash(1), nil},
	}, got, "numbers 5, 6 and 7 follow the newest edge the sweep kept")
	assert.Equal(t, int64(7), f.contractStates(kpisNode)[kpisNode].MaxVersion)
	hash, pointer := f.contractPointer(kpisNode)
	assert.Equal(t, historyHash(1), hash)
	assert.Equal(t, int64(7), pointer["version"])
	assert.Contains(t, f.storedHashes(kpisNode), historyHash(1), "a deleted contract that comes back is stored again")
}

// TestContractHistorySweeper_LeavesATenantWhoseExportLeaseIsHeld holds a
// tenant's export lease: the sweep deletes nothing of that tenant while the
// lease is live, and everything past the retention once it is released.
func TestContractHistorySweeper_LeavesATenantWhoseExportLeaseIsHeld(t *testing.T) {
	ctx := context.Background()
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.writeHistory(kpisNode, historyHash(1), historyHash(2), historyHash(3), historyHash(4))
	f.ageAllEdges(kpisNode, 4, ancient)
	sweeper := NewContractHistorySweeper(f.client, quietLogger())
	lease := NewExportLease(f.client, quietLogger())

	require.NoError(t, lease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-1", time.Minute))
	deleted, err := sweeper.Prune(ctx, historyRetention, 1000)
	require.NoError(t, err)
	assert.Zero(t, deleted, "a tenant whose lease is held is skipped")
	assert.Equal(t, []int64{1, 2, 3, 4}, f.edgeVersions(kpisNode))
	assert.Len(t, f.storedHashes(kpisNode), 4)

	require.NoError(t, lease.Release(ctx, f.tenant, "exporter-1"))
	deleted, err = sweeper.Prune(ctx, historyRetention, 1000)
	require.NoError(t, err)
	assert.Equal(t, int64(4), deleted, "a released lease no longer protects the tenant's history")
	assert.Equal(t, []int64{3, 4}, f.edgeVersions(kpisNode))
	f.requireGateUnlocked()
}

// TestContractHistorySweeper_LimitBoundsEachPass sweeps eight edges and the
// eight versions they leave unreached with a limit of five: no pass deletes
// more than five rows of the two kinds together, and the sweep ends with a
// pass that deletes none.
func TestContractHistorySweeper_LimitBoundsEachPass(t *testing.T) {
	ctx := context.Background()
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	hashes := make([]string, 0, 10)
	for i := 1; i <= 10; i++ {
		hashes = append(hashes, historyHash(i))
	}
	f.writeHistory(kpisNode, hashes...)
	f.ageAllEdges(kpisNode, 10, ancient)
	sweeper := NewContractHistorySweeper(f.client, quietLogger())

	for i, want := range []int64{5, 5, 5, 1, 0} {
		got, err := sweeper.Prune(ctx, historyRetention, 5)
		require.NoError(t, err, "prune %d", i+1)
		require.Equal(t, want, got, "prune %d", i+1)
	}
	assert.Equal(t, []int64{9, 10}, f.edgeVersions(kpisNode))
	assert.Equal(t, []string{historyHash(9), historyHash(10)}, f.storedHashes(kpisNode))
}
