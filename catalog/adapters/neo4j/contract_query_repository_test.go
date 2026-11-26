//go:build integration

package neo4jinfra

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/catalog/service/uow"
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

// The live set of a promotion is the nodes its bundle named, by their
// contract watermark: a node the promotion added, which the contracts group
// created inactive before any topology swap, is in it; a node the promotion
// dropped is not, even while the swap has not retired it yet.
func TestContractQueryRepository_LiveIsThePromotionsNodes(t *testing.T) {
	f := newSwapFixture(t)
	ctx := context.Background()
	f.promoteAt("rel-40", 40, []topology.ReleasePromotedTopologyNode{
		modelNode("analytics.z_last"), modelNode(kpisNode), modelNode("analytics.dropped"), modelNode("analytics.no_contract"),
	}, time.Now())
	f.writeContracts(contractPromotion("rel-41", 41),
		contractOf("analytics.z_last", contractB, "z"), contractOf(kpisNode, contractA, "service-py"),
		contractOf("analytics.dropped", contractA, "gone"))
	// rel-42 drops analytics.dropped and adds analytics.added, before its
	// topology swap.
	f.writeContracts(contractPromotion("rel-42", 42),
		contractOf("analytics.z_last", contractB, "z"), contractOf(kpisNode, contractA, "service-py"),
		contractOf("analytics.added", contractB, "new"))
	require.Equal(t, true, f.node("analytics.dropped")["active"], "the swap has not retired it")
	require.Equal(t, false, f.node("analytics.added")["active"], "the swap has not made it live")

	live, err := f.contractReader().Live(ctx, f.tenant, 42)
	require.NoError(t, err)
	ids := []string{}
	for _, c := range live {
		ids = append(ids, c.UniqueID)
	}
	assert.Equal(t, []string{"analytics.added", kpisNode, "analytics.z_last"}, ids, "sorted by unique id")

	got := live[1]
	assertUTC(t, promotedAtOf(41), got.Provenance.PromotedAt)
	got.Provenance.PromotedAt = time.Time{}
	assert.Equal(t, contractversion.LiveContract{
		UniqueID:       kpisNode,
		Version:        1,
		ContractHash:   contractA,
		Document:       contractDocument(kpisNode, "service-py"),
		PhysicalSchema: "analytics",
		Provenance: contractversion.Provenance{
			ContractHash:      contractA,
			ContentHash:       kpisContent,
			ReleaseID:         "rel-41",
			ContinuoContracts: "0.2.0",
			SourcePath:        "contracts/py_daily_kpis.odcs.yaml",
			PromotionSeq:      41,
		},
	}, got, "an unchanged contract keeps the provenance of the promotion that made it current")

	older, err := f.contractReader().Live(ctx, f.tenant, 41)
	require.NoError(t, err)
	require.Len(t, older, 1)
	assert.Equal(t, "analytics.dropped", older[0].UniqueID, "only the node no newer promotion reached keeps seq 41")

	latest, err := f.contractReader().LatestPromotionSeq(ctx, f.tenant)
	require.NoError(t, err)
	assert.Equal(t, int64(42), latest)
	none, err := f.contractReader().LatestPromotionSeq(ctx, f.another().tenant)
	require.NoError(t, err)
	assert.Zero(t, none, "a tenant with no contract has no contract promotion")
}

// The live set carries each node's declared schema, the spelling the topology
// swap stored on the :Node, which can differ in case from the lower-case
// contract id; a node the contracts group created before any swap has none.
func TestContractQueryRepository_LiveCarriesTheDeclaredSchema(t *testing.T) {
	f := newSwapFixture(t)
	quoted := modelNode("analytics.quoted")
	quoted.SchemaName = "Analytics"
	f.promoteAt("rel-40", 40, []topology.ReleasePromotedTopologyNode{quoted}, time.Now())
	f.writeContracts(contractPromotion("rel-41", 41),
		contractOf("analytics.quoted", contractA, "svc"), contractOf("analytics.unswapped", contractB, "svc"))

	live, err := f.contractReader().Live(context.Background(), f.tenant, 41)

	require.NoError(t, err)
	require.Len(t, live, 2)
	assert.Equal(t, "analytics.quoted", live[0].UniqueID)
	assert.Equal(t, "Analytics", live[0].PhysicalSchema, "the swap's spelling, not the lower-case id's")
	assert.Equal(t, "analytics.unswapped", live[1].UniqueID)
	assert.Empty(t, live[1].PhysicalSchema, "a node no swap has written has no declared schema")
}

// A retired node keeps its history and its pointer, and has no current
// contract.
func TestContractQueryRepository_ARetiredNodeHasNoCurrentContract(t *testing.T) {
	f := newSwapFixture(t)
	ctx := context.Background()
	f.promoteAt("rel-40", 40, []topology.ReleasePromotedTopologyNode{modelNode(kpisNode), modelNode("analytics.retired")}, time.Now())
	f.writeContracts(contractPromotion("rel-41", 41),
		contractOf(kpisNode, contractA, "service-py"), contractOf("analytics.retired", contractA, "gone"))
	f.promoteAt("rel-42", 42, []topology.ReleasePromotedTopologyNode{modelNode(kpisNode)}, time.Now())
	require.Equal(t, false, f.node("analytics.retired")["active"])

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

	live, err := f.contractReader().Live(ctx, f.tenant, 41)
	require.NoError(t, err)
	assert.Empty(t, live)
	latest, err := f.contractReader().LatestPromotionSeq(ctx, f.tenant)
	require.NoError(t, err)
	assert.Zero(t, latest)
	history, err := f.contractReader().History(ctx, f.tenant, kpisNode, 0)
	require.NoError(t, err)
	assert.Empty(t, history)
	_, err = f.contractReader().Current(ctx, f.tenant, kpisNode)
	assert.ErrorIs(t, err, contractversion.ErrNotFound)
}

// recordLiveFolder runs transaction B's live-folder write for the promotion.
func (f *swapFixture) recordLiveFolder(seq int64, release string) {
	f.t.Helper()
	require.NoError(f.t, f.uow.Write(context.Background(), f.contractsMarker(), true, func(ctx context.Context, tx uow.Tx) error {
		return tx.Contracts().RecordLiveFolder(ctx, f.tenant, seq, release, "settings")
	}))
}

// A live node whose contract the newest recorded promotion did not carry (the
// promotion's bundle left it out) has no current contract once
// that promotion's live folder is recorded, as its file is gone from the
// folder. Until then, and for a node the promotion carried, the pointer is
// current.
func TestContractQueryRepository_ANodeTheNewestPromotionLeftOutHasNoCurrentContract(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.seedLiveNode("analytics.reverted")
	ctx := context.Background()
	r := f.contractReader()
	f.writeContracts(contractPromotion("rel-41", 41),
		contractOf(kpisNode, contractA, "service-py"), contractOf("analytics.reverted", contractA, "service-old"))
	f.recordLiveFolder(41, "rel-41")
	f.writeContracts(contractPromotion("rel-42", 42), contractOf(kpisNode, contractB, "service-py"))

	_, err := r.Current(ctx, f.tenant, "analytics.reverted")
	require.NoError(t, err, "before promotion 42's folder is recorded the node keeps its contract")

	f.recordLiveFolder(42, "rel-42")

	_, err = r.Current(ctx, f.tenant, "analytics.reverted")
	assert.ErrorIs(t, err, contractversion.ErrNotFound)
	history, err := r.History(ctx, f.tenant, "analytics.reverted", 0)
	require.NoError(t, err)
	require.Len(t, history, 1, "the node keeps its history")
	assert.False(t, history[0].IsCurrent)
	cur, err := r.Current(ctx, f.tenant, kpisNode)
	require.NoError(t, err)
	assert.Equal(t, contractB, cur.ContractHash, "a node the promotion carried keeps its current contract")
}

// A promotion that names the ontology its contracts were resolved against
// records it on the edge, and every read of that edge carries it: the history,
// the current version and the provenance of the live set. An edge of a
// promotion that names none carries none.
func TestContractQueryRepository_ReadsTheOntologyVersion(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	ctx := context.Background()
	p := contractPromotion("rel-41", 41)
	p.OntologyVersion = "rel-o-3@sha256:abc123"
	f.writeContracts(p, contractOf(kpisNode, contractA, "service-py"))
	f.writeContracts(contractPromotion("rel-47", 47), contractOf(kpisNode, contractB, "analytics-eng"))

	history, err := f.contractReader().History(ctx, f.tenant, kpisNode, 0)
	require.NoError(t, err)
	require.Len(t, history, 2)
	assert.Empty(t, history[0].OntologyVersion, "version 2 names no ontology")
	assert.Equal(t, "rel-o-3@sha256:abc123", history[1].OntologyVersion)

	first, err := f.contractReader().ByVersion(ctx, f.tenant, kpisNode, 1)
	require.NoError(t, err)
	assert.Equal(t, "rel-o-3@sha256:abc123", first.OntologyVersion)

	live41, err := f.contractReader().Live(ctx, f.tenant, 41)
	require.NoError(t, err)
	assert.Empty(t, live41, "promotion 47 reached the node after 41")
	live47, err := f.contractReader().Live(ctx, f.tenant, 47)
	require.NoError(t, err)
	require.Len(t, live47, 1)
	assert.Empty(t, live47[0].Provenance.OntologyVersion, "the edge that made version 2 current names no ontology")
}

// The live set's provenance carries the ontology version of the edge that made
// each node's contract current.
func TestContractQueryRepository_LiveProvenanceCarriesTheOntologyVersion(t *testing.T) {
	f := newSwapFixture(t)
	p := contractPromotion("rel-41", 41)
	p.OntologyVersion = "rel-o-3@sha256:abc123"
	f.writeContracts(p, contractOf(kpisNode, contractA, "service-py"))

	live, err := f.contractReader().Live(context.Background(), f.tenant, 41)

	require.NoError(t, err)
	require.Len(t, live, 1)
	assert.Equal(t, "rel-o-3@sha256:abc123", live[0].Provenance.OntologyVersion)
}
