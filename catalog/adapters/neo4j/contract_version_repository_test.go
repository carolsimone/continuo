//go:build integration

package neo4jinfra

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests of this file write contract history for the swap fixture's tenant
// through the unit of work, as the contracts group does: one Write reads the
// nodes' states, plans the promotion and writes the plan.

const (
	kpisNode    = "analytics.py_daily_kpis"
	contractA   = "sha256:3f1c"
	contractB   = "sha256:b07e"
	kpisContent = "sha256:91d4"
)

// contractOf is the bundle contract of uniqueID whose stored form is told
// apart by its owner, so a test can tell which document a version holds.
func contractOf(uniqueID, contractHash, owner string) contractversion.Incoming {
	return contractversion.Incoming{
		UniqueID:     uniqueID,
		ContractHash: contractHash,
		ContentHash:  kpisContent,
		Family:       "python",
		Service:      "service-py",
		SourcePath:   "contracts/py_daily_kpis.odcs.yaml",
		Document:     contractDocument(uniqueID, owner),
	}
}

// contractDocument is a stored form small enough to compare whole. Its keys
// are out of order and it holds a character JSON escapes for HTML, so the
// canonical text the graph stores is pinned by the tests.
func contractDocument(uniqueID, owner string) map[string]any {
	return map[string]any{
		"version": "0",
		"id":      uniqueID,
		"team":    map[string]any{"name": owner},
		"schema": []any{map[string]any{
			"name":       "py_daily_kpis",
			"properties": []any{map[string]any{"name": "total_amount", "physicalType": "NUMERIC(12,2)", "required": true}},
		}},
		"description": map[string]any{"purpose": "count & sum <EUR>"},
	}
}

// contractPromotion is the promotion of releaseID at seq. Its time is given
// outside UTC, so the tests prove the repository stores it in UTC.
func contractPromotion(releaseID string, seq int64) contractversion.Promotion {
	return contractversion.Promotion{
		TenantID:          "unused-by-the-repository",
		ReleaseID:         releaseID,
		ContinuoContracts: "0.2.0",
		PromotionSeq:      seq,
		PromotedAt:        promotedAtOf(seq).In(time.FixedZone("CET", 3600)),
	}
}

// writeContracts reads the states of in's nodes, plans p and writes the plan
// in one final Write under a fresh message of the contracts group.
func (f *swapFixture) writeContracts(p contractversion.Promotion, in ...contractversion.Incoming) (contractversion.WritePlan, contractversion.WriteResult) {
	f.t.Helper()
	var (
		plan contractversion.WritePlan
		res  contractversion.WriteResult
	)
	ids := make([]string, 0, len(in))
	for _, c := range in {
		ids = append(ids, c.UniqueID)
	}
	err := f.uow.Write(context.Background(), f.contractsMarker(), true, func(ctx context.Context, tx uow.Tx) error {
		states, err := tx.Contracts().ReadStates(ctx, f.tenant, ids)
		if err != nil {
			return err
		}
		plan = contractversion.Plan(p, in, states)
		res, err = tx.Contracts().Write(ctx, f.tenant, p, plan)
		return err
	})
	require.NoError(f.t, err)
	return plan, res
}

// writePlan writes a plan the test built, skipping the planner.
func (f *swapFixture) writePlan(p contractversion.Promotion, plan contractversion.WritePlan) contractversion.WriteResult {
	f.t.Helper()
	var res contractversion.WriteResult
	err := f.uow.Write(context.Background(), f.contractsMarker(), true, func(ctx context.Context, tx uow.Tx) error {
		var err error
		res, err = tx.Contracts().Write(ctx, f.tenant, p, plan)
		return err
	})
	require.NoError(f.t, err)
	return res
}

// contractStates reads the contract state of uniqueIDs.
func (f *swapFixture) contractStates(uniqueIDs ...string) map[string]contractversion.NodeState {
	f.t.Helper()
	var states map[string]contractversion.NodeState
	err := f.uow.Write(context.Background(), f.contractsMarker(), true, func(ctx context.Context, tx uow.Tx) error {
		var err error
		states, err = tx.Contracts().ReadStates(ctx, f.tenant, uniqueIDs)
		return err
	})
	require.NoError(f.t, err)
	return states
}

func (f *swapFixture) contractsMarker() uow.Marker {
	return uow.Marker{TenantID: f.tenant, Group: streams.CatalogReleaseContracts, MessageID: uuid.NewString()}
}

// promotedEdge is one :CONTRACT_PROMOTED edge with the hash it points at.
type promotedEdge struct {
	hash  string
	props map[string]any
}

// promotions lists uniqueID's :CONTRACT_PROMOTED edges by version.
func (f *swapFixture) promotions(uniqueID string) []promotedEdge {
	f.t.Helper()
	var out []promotedEdge
	for _, rec := range f.rows(`
		MATCH (:Node {tenant_id: $tenant_id, unique_id: $uid})-[e:CONTRACT_PROMOTED]->(v:ContractVersion {tenant_id: $tenant_id})
		RETURN v.contract_hash AS h, properties(e) AS p ORDER BY e.version`, map[string]any{"uid": uniqueID}) {
		h, _ := rec.Get("h")
		p, _ := rec.Get("p")
		out = append(out, promotedEdge{hash: h.(string), props: p.(map[string]any)})
	}
	return out
}

// contractPointer returns the hash uniqueID's :CURRENT_CONTRACT names and the
// pointer's properties, or "" and nil when it has none.
func (f *swapFixture) contractPointer(uniqueID string) (string, map[string]any) {
	f.t.Helper()
	recs := f.rows(`
		MATCH (:Node {tenant_id: $tenant_id, unique_id: $uid})-[c:CURRENT_CONTRACT]->(v:ContractVersion {tenant_id: $tenant_id})
		RETURN v.contract_hash AS h, properties(c) AS p`, map[string]any{"uid": uniqueID})
	require.LessOrEqual(f.t, len(recs), 1, "a node has at most one :CURRENT_CONTRACT")
	if len(recs) == 0 {
		return "", nil
	}
	h, _ := recs[0].Get("h")
	p, _ := recs[0].Get("p")
	return h.(string), p.(map[string]any)
}

// contractVersions returns the properties of uniqueID's :ContractVersion
// nodes, keyed by contract hash.
func (f *swapFixture) contractVersions(uniqueID string) map[string]map[string]any {
	f.t.Helper()
	out := map[string]map[string]any{}
	for _, rec := range f.rows(`
		MATCH (v:ContractVersion {tenant_id: $tenant_id, unique_id: $uid}) RETURN properties(v) AS p`,
		map[string]any{"uid": uniqueID}) {
		p, _ := rec.Get("p")
		props := p.(map[string]any)
		out[props["contract_hash"].(string)] = props
	}
	return out
}

// edgeSummary is an edge's version, seq and the version it repeats.
type edgeSummary struct {
	Version, Seq int64
	Hash         string
	SameAs       any
}

func summaries(edges []promotedEdge) []edgeSummary {
	out := make([]edgeSummary, 0, len(edges))
	for _, e := range edges {
		out = append(out, edgeSummary{
			Version: e.props["version"].(int64), Seq: e.props["promotion_seq"].(int64),
			Hash: e.hash, SameAs: e.props["same_as_version"],
		})
	}
	return out
}

// assertUTC checks a stored promoted_at is the promotion's instant, as a
// datetime in UTC.
func assertUTC(t *testing.T, want time.Time, got any) {
	t.Helper()
	at, ok := got.(time.Time)
	require.True(t, ok, "promoted_at is a datetime, got %T", got)
	assert.True(t, at.Equal(want), "promoted_at %v, want %v", at, want)
	assert.Equal(t, "UTC", at.Location().String(), "promoted_at is stored in UTC")
}

func TestContractRepository_AFirstVersion(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)

	_, res := f.writeContracts(contractPromotion("rel-4e5f6a7-212", 41), contractOf(kpisNode, contractA, "service-py"))
	assert.Equal(t, contractversion.WriteResult{VersionsCreated: 1, EdgesCreated: 1, PointersMoved: 1}, res)

	versions := f.contractVersions(kpisNode)
	require.Len(t, versions, 1)
	v := versions[contractA]
	assert.Equal(t, "python", v["family"])
	assert.Equal(t, "service-py", v["service"])
	assert.Equal(t, "rel-4e5f6a7-212", v["first_release_id"])
	assert.Equal(t,
		`{"description":{"purpose":"count & sum <EUR>"},"id":"analytics.py_daily_kpis","schema":[{"name":"py_daily_kpis","properties":[{"name":"total_amount","physicalType":"NUMERIC(12,2)","required":true}]}],"team":{"name":"service-py"},"version":"0"}`,
		v["document"], "the stored form is canonical JSON text: keys sorted, no HTML escaping, no spaces")

	edges := f.promotions(kpisNode)
	require.Len(t, edges, 1)
	e := edges[0]
	assert.Equal(t, contractA, e.hash)
	for key, want := range map[string]any{
		"version": int64(1), "promotion_seq": int64(41), "release_id": "rel-4e5f6a7-212",
		"content_hash": kpisContent, "continuo_contracts": "0.2.0",
		"source_path": "contracts/py_daily_kpis.odcs.yaml",
	} {
		assert.Equal(t, want, e.props[key], "the edge's %s", key)
	}
	assertUTC(t, promotedAtOf(41), e.props["promoted_at"])
	assert.NotContains(t, e.props, "same_as_version", "a first version repeats none")
	assert.NotContains(t, e.props, "ontology_version", "a promotion with no ontology stores none")

	hash, pointer := f.contractPointer(kpisNode)
	assert.Equal(t, contractA, hash)
	assert.Equal(t, int64(1), pointer["version"])
	assert.Equal(t, int64(41), pointer["promotion_seq"])
	assert.Equal(t, "rel-4e5f6a7-212", pointer["release_id"])
	assertUTC(t, promotedAtOf(41), pointer["promoted_at"])

	assert.Equal(t, int64(41), f.node(kpisNode)["contract_promotion_seq"])
}

func TestContractRepository_AChangeAndARevert(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)

	f.writeContracts(contractPromotion("rel-4e5f6a7-212", 41), contractOf(kpisNode, contractA, "service-py"))
	_, second := f.writeContracts(contractPromotion("rel-5d6e7f8-219", 47), contractOf(kpisNode, contractB, "analytics-eng"))
	assert.Equal(t, contractversion.WriteResult{VersionsCreated: 1, EdgesCreated: 1, PointersMoved: 1}, second)
	_, third := f.writeContracts(contractPromotion("rel-77aa0bc-230", 52), contractOf(kpisNode, contractA, "service-py"))
	assert.Equal(t, contractversion.WriteResult{VersionsCreated: 0, EdgesCreated: 1, PointersMoved: 1}, third,
		"a revert reuses the stored version and still records the change")

	assert.Equal(t, []edgeSummary{
		{Version: 1, Seq: 41, Hash: contractA, SameAs: nil},
		{Version: 2, Seq: 47, Hash: contractB, SameAs: nil},
		{Version: 3, Seq: 52, Hash: contractA, SameAs: int64(1)},
	}, summaries(f.promotions(kpisNode)))
	assert.Len(t, f.contractVersions(kpisNode), 2, "one version per contract hash")

	hash, pointer := f.contractPointer(kpisNode)
	assert.Equal(t, contractA, hash)
	assert.Equal(t, int64(3), pointer["version"])
	assert.Equal(t, int64(52), pointer["promotion_seq"])
	assert.Equal(t, "rel-77aa0bc-230", pointer["release_id"])
	assertUTC(t, promotedAtOf(52), pointer["promoted_at"])
	assert.Equal(t, int64(52), f.node(kpisNode)["contract_promotion_seq"])

	assert.Equal(t, map[string]contractversion.NodeState{kpisNode: {
		UniqueID:           kpisNode,
		Watermark:          52,
		CurrentHash:        contractA,
		CurrentVersion:     3,
		MaxVersion:         3,
		FirstVersionByHash: map[string]int64{contractA: 1, contractB: 2},
	}}, f.contractStates(kpisNode))
}

func TestContractRepository_AVersionIsWrittenOnce(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.writeContracts(contractPromotion("rel-41", 41), contractOf(kpisNode, contractA, "service-py"))
	before := f.contractVersions(kpisNode)

	// The same hash arrives again with every other fact changed: the stored
	// version is immutable.
	changed := contractOf(kpisNode, contractA, "someone-else")
	changed.Family, changed.Service = "dbt", "service-other"
	plan, res := f.writeContracts(contractPromotion("rel-47", 47), changed)
	assert.Equal(t, 1, plan.Unchanged)
	assert.Equal(t, contractversion.WriteResult{}, res)
	assert.Equal(t, before, f.contractVersions(kpisNode), "a second write of the same hash changes no property")
	assert.Len(t, f.promotions(kpisNode), 1, "an unchanged contract records no promotion")
	assert.Equal(t, int64(47), f.node(kpisNode)["contract_promotion_seq"],
		"the watermark rises even when the contract is unchanged")

	// The same promotion delivered again writes nothing.
	_, again := f.writeContracts(contractPromotion("rel-47", 47), changed)
	assert.Equal(t, contractversion.WriteResult{}, again)
	assert.Len(t, f.promotions(kpisNode), 1)
}

func TestContractRepository_ANodeTheGraphDoesNotHoldIsCreatedInactive(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.table_a")
	liveBefore := f.node("analytics.table_a")

	_, res := f.writeContracts(contractPromotion("rel-41", 41),
		contractOf(kpisNode, contractA, "service-py"), contractOf("analytics.table_a", contractB, "svc"))
	assert.Equal(t, contractversion.WriteResult{VersionsCreated: 2, EdgesCreated: 2, PointersMoved: 2}, res)

	n := f.node(kpisNode)
	require.NotNil(t, n, "the contract's node is created")
	for key, want := range map[string]any{
		"active": false, "release_id": "rel-41", "service_name": "service-py", "contract_promotion_seq": int64(41),
	} {
		assert.Equal(t, want, n[key], "the created node's %s", key)
	}
	assert.NotContains(t, n, "retired_at", "a node that was never live has no retired_at")
	hash, _ := f.contractPointer(kpisNode)
	assert.Equal(t, contractA, hash, "the created node holds its contract")

	a := f.node("analytics.table_a")
	for key, value := range liveBefore {
		assert.Equal(t, value, a[key], "the live node keeps its %s", key)
	}

	// The topology swap of a release that carries the node makes it live with
	// its contract history.
	f.promoteAt("rel-41", 41, []topology.ReleasePromotedTopologyNode{modelNode(kpisNode), modelNode("analytics.table_a")}, time.Now())
	assert.Equal(t, true, f.node(kpisNode)["active"])
	hash, _ = f.contractPointer(kpisNode)
	assert.Equal(t, contractA, hash)
}

func TestContractRepository_ALateOlderPromotionChangesNothing(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.writeContracts(contractPromotion("rel-52", 52), contractOf(kpisNode, contractA, "service-py"))

	plan, res := f.writeContracts(contractPromotion("rel-47", 47), contractOf(kpisNode, contractB, "analytics-eng"))
	assert.Equal(t, 1, plan.Superseded)
	assert.Equal(t, contractversion.WriteResult{}, res, "the older contract is not stored")
	assert.NotContains(t, f.contractVersions(kpisNode), contractB, "no contract is left that nothing points at")
	assert.Len(t, f.promotions(kpisNode), 1)
	hash, pointer := f.contractPointer(kpisNode)
	assert.Equal(t, contractA, hash)
	assert.Equal(t, int64(52), pointer["promotion_seq"])
	assert.Equal(t, int64(52), f.node(kpisNode)["contract_promotion_seq"], "the watermark never goes back")
}

// The write guards every edge and pointer on the watermark itself, so a plan
// made from a stale read cannot record an older promotion as a change.
func TestContractRepository_TheWatermarkGuardIsEnforcedInTheWrite(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.writeContracts(contractPromotion("rel-52", 52), contractOf(kpisNode, contractA, "service-py"))

	older := contractPromotion("rel-47", 47)
	b := contractOf(kpisNode, contractB, "analytics-eng")
	res := f.writePlan(older, contractversion.WritePlan{
		Versions: []contractversion.Version{{UniqueID: kpisNode, ContractHash: contractB, Family: "python",
			Service: "service-py", FirstReleaseID: "rel-47", Document: b.Document}},
		Edges: []contractversion.Edge{{UniqueID: kpisNode, ContractHash: contractB, ContentHash: kpisContent,
			SourcePath: b.SourcePath, Version: 2}},
	})
	assert.Equal(t, contractversion.WriteResult{VersionsCreated: 1}, res)
	assert.Len(t, f.promotions(kpisNode), 1)
	hash, _ := f.contractPointer(kpisNode)
	assert.Equal(t, contractA, hash)
}

func TestContractRepository_ReadStatesOfNodesWithNoContract(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode("analytics.table_a")

	assert.Equal(t, map[string]contractversion.NodeState{
		"analytics.table_a": {UniqueID: "analytics.table_a", FirstVersionByHash: map[string]int64{}},
	}, f.contractStates("analytics.table_a", "analytics.absent"),
		"a node with no contract has a zero state and a node the graph does not hold has none")
}

func TestContractRepository_RefusesAnotherTenant(t *testing.T) {
	f := newSwapFixture(t)
	err := f.uow.Write(context.Background(), f.contractsMarker(), true, func(ctx context.Context, tx uow.Tx) error {
		_, err := tx.Contracts().ReadStates(ctx, "another-tenant", []string{kpisNode})
		return err
	})
	assert.ErrorIs(t, err, errTenantMismatch)
	err = f.uow.Write(context.Background(), f.contractsMarker(), true, func(ctx context.Context, tx uow.Tx) error {
		_, err := tx.Contracts().Write(ctx, "another-tenant", contractPromotion("rel-1", 1), contractversion.WritePlan{})
		return err
	})
	assert.ErrorIs(t, err, errTenantMismatch)
}

func TestContractRepository_LeavesOtherTenantsAlone(t *testing.T) {
	f := newSwapFixture(t)
	other := f.another()
	f.seedLiveNode(kpisNode)
	other.seedLiveNode(kpisNode)
	other.writeContracts(contractPromotion("rel-41", 41), contractOf(kpisNode, contractB, "theirs"))

	f.writeContracts(contractPromotion("rel-41", 41), contractOf(kpisNode, contractA, "mine"))

	assert.Equal(t, []edgeSummary{{Version: 1, Seq: 41, Hash: contractA}}, summaries(f.promotions(kpisNode)))
	assert.Equal(t, []edgeSummary{{Version: 1, Seq: 41, Hash: contractB}}, summaries(other.promotions(kpisNode)))
}

// A change written twice under fresh message ids, and once more from a plan
// made before the first write committed, records one edge and moves the
// pointer once: the write never records a change to the contract the pointer
// already names.
func TestContractRepository_AChangeWrittenTwiceIsRecordedOnce(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	f.writeContracts(contractPromotion("rel-41", 41), contractOf(kpisNode, contractA, "service-py"))
	p47 := contractPromotion("rel-47", 47)
	b := contractOf(kpisNode, contractB, "analytics-eng")

	_, first := f.writeContracts(p47, b)
	assert.Equal(t, contractversion.WriteResult{VersionsCreated: 1, EdgesCreated: 1, PointersMoved: 1}, first)
	_, again := f.writeContracts(p47, b)
	assert.Equal(t, contractversion.WriteResult{}, again, "a redelivery under a fresh message id writes nothing")

	// The plan a read taken before the first write would have made: the node
	// at version 1 on contract a, so b would be version 2 again.
	stale := contractversion.Plan(p47, []contractversion.Incoming{b}, map[string]contractversion.NodeState{kpisNode: {
		UniqueID: kpisNode, Watermark: 41, CurrentHash: contractA, CurrentVersion: 1, MaxVersion: 1,
		FirstVersionByHash: map[string]int64{contractA: 1},
	}})
	require.Len(t, stale.Edges, 1)
	assert.Equal(t, contractversion.WriteResult{}, f.writePlan(p47, stale),
		"a stale plan at the same seq records no edge and moves no pointer")

	assert.Equal(t, []edgeSummary{
		{Version: 1, Seq: 41, Hash: contractA},
		{Version: 2, Seq: 47, Hash: contractB},
	}, summaries(f.promotions(kpisNode)))
	hash, pointer := f.contractPointer(kpisNode)
	assert.Equal(t, contractB, hash)
	assert.Equal(t, int64(2), pointer["version"])
}

// A promotion that names an ontology stores its version on each edge it
// creates, and an edge of a promotion that names none has no such property.
func TestContractRepository_RecordsTheOntologyVersionOnTheEdge(t *testing.T) {
	f := newSwapFixture(t)
	f.seedLiveNode(kpisNode)
	p := contractPromotion("rel-4e5f6a7-212", 41)
	p.OntologyVersion = "rel-9f8e7d6-14@sha256:bd4d098b"

	f.writeContracts(p, contractOf(kpisNode, contractA, "service-py"))
	f.writeContracts(contractPromotion("rel-5d6e7f8-219", 47), contractOf(kpisNode, contractB, "analytics-eng"))

	edges := f.promotions(kpisNode)
	require.Len(t, edges, 2)
	assert.Equal(t, "rel-9f8e7d6-14@sha256:bd4d098b", edges[0].props["ontology_version"])
	assert.NotContains(t, edges[1].props, "ontology_version")
}
