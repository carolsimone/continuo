//go:build integration

package neo4jinfra

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests of this file write contract history through the contracts
// group's repository and read it back through ContractQueryRepository.

// designHistory writes the design's history of py_daily_kpis: contract a at
// seq 41, b at 47, and a again at 52.
func (f *swapFixture) designHistory() {
	f.t.Helper()
	f.writeContracts(contractPromotion("rel-4e5f6a7-212", 41), contractOf(kpisNode, contractA, "service-py"))
	f.writeContracts(contractPromotion("rel-5d6e7f8-219", 47), contractOf(kpisNode, contractB, "analytics-eng"))
	f.writeContracts(contractPromotion("rel-77aa0bc-230", 52), contractOf(kpisNode, contractA, "service-py"))
}

func (f *swapFixture) contractReader() *ContractQueryRepository {
	return NewContractQueryRepository(f.client)
}

// versionNumbers lists the versions of views in their order.
func versionNumbers(views []contractversion.VersionView) []int64 {
	out := []int64{}
	for _, v := range views {
		out = append(out, v.Version)
	}
	return out
}

func TestContractQueryRepository_History(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.designHistory()
	ctx := context.Background()

	all, err := f.contractReader().History(ctx, f.tenant, kpisNode, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 2, 1}, versionNumbers(all), "newest first; a limit of 0 lists every version")

	want := []contractversion.VersionView{
		{UniqueID: kpisNode, Version: 3, ContractHash: contractA, ReleaseID: "rel-77aa0bc-230", PromotionSeq: 52,
			ContentHash: kpisContent, ContinuoContracts: "0.2.0", SameAsVersion: 1,
			SourcePath: "contracts/py_daily_kpis.odcs.yaml", IsCurrent: true},
		{UniqueID: kpisNode, Version: 2, ContractHash: contractB, ReleaseID: "rel-5d6e7f8-219", PromotionSeq: 47,
			ContentHash: kpisContent, ContinuoContracts: "0.2.0", SourcePath: "contracts/py_daily_kpis.odcs.yaml"},
		{UniqueID: kpisNode, Version: 1, ContractHash: contractA, ReleaseID: "rel-4e5f6a7-212", PromotionSeq: 41,
			ContentHash: kpisContent, ContinuoContracts: "0.2.0", SourcePath: "contracts/py_daily_kpis.odcs.yaml"},
	}
	for i := range all {
		assertUTC(t, promotedAtOf(want[i].PromotionSeq), all[i].PromotedAt)
		all[i].PromotedAt = time.Time{}
	}
	assert.Equal(t, want, all, "a history row carries no document")

	two, err := f.contractReader().History(ctx, f.tenant, kpisNode, 2)
	require.NoError(t, err)
	assert.Equal(t, []int64{3, 2}, versionNumbers(two))

	none, err := f.contractReader().History(ctx, f.tenant, "analytics.absent", 0)
	require.NoError(t, err)
	assert.Empty(t, none, "a node with no contract has no history")
}

func TestContractQueryRepository_CurrentAndByVersion(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.seedLiveNode("analytics.table_a")
	f.designHistory()
	ctx := context.Background()
	r := f.contractReader()

	cur, err := r.Current(ctx, f.tenant, kpisNode)
	require.NoError(t, err)
	assert.Equal(t, int64(3), cur.Version, "the current contract is the revert, not the newest content")
	assert.Equal(t, contractA, cur.ContractHash)
	assert.Equal(t, int64(1), cur.SameAsVersion)
	assert.Equal(t, "rel-77aa0bc-230", cur.ReleaseID)
	assert.True(t, cur.IsCurrent)
	assertUTC(t, promotedAtOf(52), cur.PromotedAt)
	assert.Equal(t, contractDocument(kpisNode, "service-py"), cur.Document, "the stored form decodes back whole")

	first, err := r.ByVersion(ctx, f.tenant, kpisNode, 1)
	require.NoError(t, err)
	assert.Equal(t, contractA, first.ContractHash)
	assert.False(t, first.IsCurrent, "the first edge to the current content is not the current version")
	assert.Equal(t, int64(41), first.PromotionSeq)
	assert.Equal(t, contractDocument(kpisNode, "service-py"), first.Document)

	second, err := r.ByVersion(ctx, f.tenant, kpisNode, 2)
	require.NoError(t, err)
	assert.Equal(t, contractDocument(kpisNode, "analytics-eng"), second.Document)

	_, err = r.ByVersion(ctx, f.tenant, kpisNode, 4)
	assert.ErrorIs(t, err, contractversion.ErrNotFound)
	_, err = r.Current(ctx, f.tenant, "analytics.table_a")
	assert.ErrorIs(t, err, contractversion.ErrNotFound, "a node with no contract has no current contract")
	_, err = r.Current(ctx, f.tenant, "analytics.absent")
	assert.ErrorIs(t, err, contractversion.ErrNotFound)
}

func TestContractQueryRepository_LiveSkipsARetiredNode(t *testing.T) {
	f := newSwapFixture(t)
	ctx := context.Background()
	f.promoteAt("rel-40", 40, []topology.ReleasePromotedTopologyNode{
		modelNode("analytics.z_last"), modelNode(kpisNode), modelNode("analytics.retired"), modelNode("analytics.no_contract"),
	}, time.Now())
	f.writeContracts(contractPromotion("rel-41", 41),
		contractOf("analytics.z_last", contractB, "z"), contractOf(kpisNode, contractA, "service-py"),
		contractOf("analytics.retired", contractA, "gone"))
	// A release without analytics.retired retires it.
	f.promoteAt("rel-42", 42, []topology.ReleasePromotedTopologyNode{
		modelNode("analytics.z_last"), modelNode(kpisNode), modelNode("analytics.no_contract"),
	}, time.Now())
	require.Equal(t, false, f.node("analytics.retired")["active"])

	live, err := f.contractReader().Live(ctx, f.tenant)
	require.NoError(t, err)
	require.Len(t, live, 2)
	assert.Equal(t, "analytics.py_daily_kpis", live[0].UniqueID, "sorted by unique id")
	assert.Equal(t, "analytics.z_last", live[1].UniqueID)

	got := live[0]
	assertUTC(t, promotedAtOf(41), got.Provenance.PromotedAt)
	got.Provenance.PromotedAt = time.Time{}
	assert.Equal(t, contractversion.LiveContract{
		UniqueID:     kpisNode,
		Version:      1,
		ContractHash: contractA,
		Document:     contractDocument(kpisNode, "service-py"),
		Provenance: contractversion.Provenance{
			ContractHash:      contractA,
			ContentHash:       kpisContent,
			ReleaseID:         "rel-41",
			ContinuoContracts: "0.2.0",
			SourcePath:        "contracts/py_daily_kpis.odcs.yaml",
			PromotionSeq:      41,
		},
	}, got)

	// The retired node keeps its history and its pointer, and has no current
	// contract.
	hash, _ := f.contractPointer("analytics.retired")
	assert.Equal(t, contractA, hash, "a retired node keeps its pointer")
	history, err := f.contractReader().History(ctx, f.tenant, "analytics.retired", 0)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.False(t, history[0].IsCurrent, "no version of a retired node is current")
	_, err = f.contractReader().ByVersion(ctx, f.tenant, "analytics.retired", 1)
	assert.NoError(t, err)
	_, err = f.contractReader().Current(ctx, f.tenant, "analytics.retired")
	assert.ErrorIs(t, err, contractversion.ErrNotFound)
}

func TestContractQueryRepository_ReadsOneTenant(t *testing.T) {
	f := newSwapFixture(t)
	other := f.another()
	other.seedLiveNode(kpisNode)
	other.writeContracts(contractPromotion("rel-41", 41), contractOf(kpisNode, contractA, "theirs"))
	ctx := context.Background()

	live, err := f.contractReader().Live(ctx, f.tenant)
	require.NoError(t, err)
	assert.Empty(t, live)
	history, err := f.contractReader().History(ctx, f.tenant, kpisNode, 0)
	require.NoError(t, err)
	assert.Empty(t, history)
	_, err = f.contractReader().Current(ctx, f.tenant, kpisNode)
	assert.ErrorIs(t, err, contractversion.ErrNotFound)
}
