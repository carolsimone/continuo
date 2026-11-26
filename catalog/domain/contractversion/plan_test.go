package contractversion_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
)

const (
	hashA   = "sha256:3f1c"
	hashB   = "sha256:b07e"
	content = "sha256:91d4"
	kpis    = "analytics.py_daily_kpis"
)

// incoming is one bundle contract whose stored form names its contract hash,
// so a test can tell which document a Version carries.
func incoming(uniqueID, contractHash, contentHash string) contractversion.Incoming {
	return contractversion.Incoming{
		UniqueID:     uniqueID,
		ContractHash: contractHash,
		ContentHash:  contentHash,
		Family:       "python",
		Project:      "service-py",
		SourcePath:   "contracts/" + uniqueID + ".odcs.yaml",
		Document:     map[string]any{"id": uniqueID, "marker": contractHash},
	}
}

func promo(seq int64) contractversion.Promotion {
	return contractversion.Promotion{
		TenantID:          "default",
		ReleaseID:         fmt.Sprintf("rel-%d", seq),
		ContinuoContracts: "0.2.0",
		PromotionSeq:      seq,
		PromotedAt:        time.Date(2026, 11, 20, 10, 2, 11, 0, time.UTC),
	}
}

// stateOf is a node whose newest edge is `newest`, whose current contract is
// `currentHash` at version `current`, and whose watermark is `watermark`.
func stateOf(uniqueID string, watermark int64, currentHash string, current, newest int64, firsts map[string]int64) map[string]contractversion.NodeState {
	return map[string]contractversion.NodeState{uniqueID: {
		UniqueID:           uniqueID,
		Watermark:          watermark,
		CurrentHash:        currentHash,
		CurrentVersion:     current,
		MaxVersion:         newest,
		FirstVersionByHash: firsts,
	}}
}

// applied folds a plan's edges into the states the way a repository's write
// does: the watermark rises to the promotion's seq, and each edge becomes the
// node's current contract and newest version.
func applied(states map[string]contractversion.NodeState, p contractversion.Promotion, plan contractversion.WritePlan) map[string]contractversion.NodeState {
	next := map[string]contractversion.NodeState{}
	for id, st := range states {
		next[id] = st
	}
	for _, e := range plan.Edges {
		st := next[e.UniqueID]
		st.UniqueID = e.UniqueID
		st.Watermark = max(st.Watermark, p.PromotionSeq)
		st.CurrentHash = e.ContractHash
		st.CurrentVersion = e.Version
		st.MaxVersion = e.Version
		firsts := map[string]int64{}
		for h, v := range st.FirstVersionByHash {
			firsts[h] = v
		}
		if _, seen := firsts[e.ContractHash]; !seen {
			firsts[e.ContractHash] = e.Version
		}
		st.FirstVersionByHash = firsts
		next[e.UniqueID] = st
	}
	return next
}

func TestPlan_TheDesignsHistory(t *testing.T) {
	a := incoming(kpis, hashA, content)
	b := incoming(kpis, hashB, content)

	p41, p47, p52 := promo(41), promo(47), promo(52)
	first := contractversion.Plan(p41, []contractversion.Incoming{a}, map[string]contractversion.NodeState{})
	assert.Equal(t, []contractversion.Edge{{
		UniqueID: a.UniqueID, ContractHash: a.ContractHash, ContentHash: a.ContentHash, SourcePath: a.SourcePath, Version: 1,
	}}, first.Edges)

	states := applied(map[string]contractversion.NodeState{}, p41, first)
	second := contractversion.Plan(p47, []contractversion.Incoming{b}, states)
	require.Len(t, second.Edges, 1)
	assert.Equal(t, int64(2), second.Edges[0].Version)
	assert.Equal(t, int64(0), second.Edges[0].SameAsVersion, "a contract no earlier version holds is not a revert")

	states = applied(states, p47, second)
	third := contractversion.Plan(p52, []contractversion.Incoming{a}, states)
	require.Len(t, third.Edges, 1)
	assert.Equal(t, int64(3), third.Edges[0].Version, "a revert is a change, so it takes the next number")
	assert.Equal(t, int64(1), third.Edges[0].SameAsVersion, "the revert says which version it repeats")
}

func TestPlan_EveryIncomingContractIsAVersionStampedWithTheRelease(t *testing.T) {
	a := incoming(kpis, hashA, content)
	plan := contractversion.Plan(promo(41), []contractversion.Incoming{a}, nil)
	assert.Equal(t, []contractversion.Version{{
		UniqueID:       kpis,
		ContractHash:   hashA,
		Family:         "python",
		Project:        "service-py",
		FirstReleaseID: "rel-41",
		Document:       a.Document,
	}}, plan.Versions)
}

func TestPlan_ARevertGetsTheNextNumberAndSameAs(t *testing.T) {
	a := incoming(kpis, hashA, content)
	states := stateOf(kpis, 47, hashB, 2, 2, map[string]int64{hashA: 1, hashB: 2})

	plan := contractversion.Plan(promo(52), []contractversion.Incoming{a}, states)

	assert.Equal(t, []contractversion.Edge{{
		UniqueID: kpis, ContractHash: hashA, ContentHash: content, SourcePath: a.SourcePath, Version: 3, SameAsVersion: 1,
	}}, plan.Edges)
	assert.Zero(t, plan.Superseded)
	assert.Zero(t, plan.Unchanged)
}

func TestPlan_ARevertNamesTheEarliestVersionOfThatContent(t *testing.T) {
	// a was version 1, then b, then a again (3), then b again (4): a's earliest
	// version stays 1, so the next revert to it says "same as 1".
	a := incoming(kpis, hashA, content)
	states := stateOf(kpis, 60, hashB, 4, 4, map[string]int64{hashA: 1, hashB: 2})

	plan := contractversion.Plan(promo(61), []contractversion.Incoming{a}, states)

	require.Len(t, plan.Edges, 1)
	assert.Equal(t, int64(5), plan.Edges[0].Version)
	assert.Equal(t, int64(1), plan.Edges[0].SameAsVersion)
}

func TestPlan_NumbersFromTheNewestEdgeNotTheCurrentOne(t *testing.T) {
	// A retention sweep may leave a node whose newest edge is above the version
	// its pointer names; the counter keeps counting from the newest edge.
	b := incoming(kpis, hashB, content)
	states := stateOf(kpis, 47, hashA, 1, 3, map[string]int64{hashA: 1})

	plan := contractversion.Plan(promo(52), []contractversion.Incoming{b}, states)

	require.Len(t, plan.Edges, 1)
	assert.Equal(t, int64(4), plan.Edges[0].Version)
}

func TestPlan_AnUnchangedContractWritesNoEdge(t *testing.T) {
	a := incoming(kpis, hashA, content)
	states := stateOf(kpis, 41, hashA, 1, 1, map[string]int64{hashA: 1})

	plan := contractversion.Plan(promo(47), []contractversion.Incoming{a}, states)

	assert.Empty(t, plan.Edges)
	assert.Equal(t, 1, plan.Unchanged)
	assert.Zero(t, plan.Superseded)
	require.Len(t, plan.Versions, 1, "the version is still listed: the write creates it only when missing")
	assert.Equal(t, hashA, plan.Versions[0].ContractHash)
}

func TestPlan_ALateOlderPromotionChangesNothing(t *testing.T) {
	b := incoming(kpis, hashB, content)
	states := stateOf(kpis, 52, hashA, 3, 3, map[string]int64{hashA: 1, hashB: 2})

	plan := contractversion.Plan(promo(47), []contractversion.Incoming{b}, states)

	assert.Empty(t, plan.Edges)
	assert.Empty(t, plan.Versions)
	assert.Equal(t, 1, plan.Superseded)
	assert.Zero(t, plan.Unchanged)
}

// A late promotion carrying a contract the node never held stores nothing:
// no version, so no contract that nothing points at.
func TestPlan_ALateOlderPromotionWithANewHashPlansNoVersion(t *testing.T) {
	c := incoming(kpis, "sha256:never-seen", content)
	states := stateOf(kpis, 52, hashA, 3, 3, map[string]int64{hashA: 1, hashB: 2})

	plan := contractversion.Plan(promo(47), []contractversion.Incoming{c}, states)

	assert.Empty(t, plan.Versions)
	assert.Empty(t, plan.Edges)
	assert.Equal(t, 1, plan.Superseded)
}

func TestPlan_ASupersededNodeIsNotCountedAsUnchangedWhenItsHashMatches(t *testing.T) {
	a := incoming(kpis, hashA, content)
	states := stateOf(kpis, 52, hashA, 3, 3, map[string]int64{hashA: 1})

	plan := contractversion.Plan(promo(47), []contractversion.Incoming{a}, states)

	assert.Equal(t, 1, plan.Superseded)
	assert.Zero(t, plan.Unchanged)
	assert.Empty(t, plan.Edges)
	assert.Empty(t, plan.Versions)
}

func TestPlan_ARedeliveryOfTheCurrentPromotionChangesNothing(t *testing.T) {
	a := incoming(kpis, hashA, content)
	states := stateOf(kpis, 52, hashA, 3, 3, map[string]int64{hashA: 1, hashB: 2})

	plan := contractversion.Plan(promo(52), []contractversion.Incoming{a}, states)

	assert.Empty(t, plan.Edges)
	assert.Equal(t, 1, plan.Unchanged)
	assert.Zero(t, plan.Superseded)
}

func TestPlan_OnlyAWatermarkAboveThePromotionSupersedes(t *testing.T) {
	// A write advances a node's watermark to the promotion's seq before it
	// decides the edge, so a node at the promotion's own seq is still planned.
	b := incoming(kpis, hashB, content)
	states := stateOf(kpis, 47, hashA, 1, 1, map[string]int64{hashA: 1})

	plan := contractversion.Plan(promo(47), []contractversion.Incoming{b}, states)

	require.Len(t, plan.Edges, 1)
	assert.Equal(t, int64(2), plan.Edges[0].Version)
	assert.Zero(t, plan.Superseded)
}

func TestPlan_AMixedPromotionSortsEverythingByUniqueID(t *testing.T) {
	added := incoming("analytics.z_new", hashA, content)
	changed := incoming("analytics.b_changed", hashB, content)
	same := incoming("analytics.m_same", hashA, content)
	late := incoming("analytics.a_late", hashB, content)
	states := map[string]contractversion.NodeState{
		"analytics.b_changed": {UniqueID: "analytics.b_changed", Watermark: 40, CurrentHash: hashA, CurrentVersion: 1, MaxVersion: 1, FirstVersionByHash: map[string]int64{hashA: 1}},
		"analytics.m_same":    {UniqueID: "analytics.m_same", Watermark: 40, CurrentHash: hashA, CurrentVersion: 1, MaxVersion: 1, FirstVersionByHash: map[string]int64{hashA: 1}},
		"analytics.a_late":    {UniqueID: "analytics.a_late", Watermark: 99, CurrentHash: hashA, CurrentVersion: 4, MaxVersion: 4, FirstVersionByHash: map[string]int64{hashA: 1}},
	}

	plan := contractversion.Plan(promo(47), []contractversion.Incoming{added, changed, same, late}, states)

	var versionIDs, edgeIDs []string
	for _, v := range plan.Versions {
		versionIDs = append(versionIDs, v.UniqueID)
	}
	for _, e := range plan.Edges {
		edgeIDs = append(edgeIDs, e.UniqueID)
	}
	assert.Equal(t, []string{"analytics.b_changed", "analytics.m_same", "analytics.z_new"}, versionIDs,
		"the superseded node contributes no version")
	assert.Equal(t, []string{"analytics.b_changed", "analytics.z_new"}, edgeIDs)
	assert.Equal(t, 1, plan.Superseded)
	assert.Equal(t, 1, plan.Unchanged)
}

func TestPlan_TheInputOrderDoesNotChangeThePlan(t *testing.T) {
	ids := []string{"analytics.c", "analytics.a", "analytics.d", "analytics.b"}
	var contracts []contractversion.Incoming
	for _, id := range ids {
		contracts = append(contracts, incoming(id, hashA, content))
	}
	want := contractversion.Plan(promo(41), contracts, nil)

	for shift := 1; shift < len(contracts); shift++ {
		rotated := append(append([]contractversion.Incoming{}, contracts[shift:]...), contracts[:shift]...)
		assert.Equal(t, want, contractversion.Plan(promo(41), rotated, nil), "rotated by %d", shift)
	}
}

func TestPlan_LeavesItsInputsAlone(t *testing.T) {
	contracts := []contractversion.Incoming{incoming("analytics.b", hashA, content), incoming("analytics.a", hashA, content)}
	states := stateOf("analytics.a", 1, hashB, 1, 1, map[string]int64{hashB: 1})

	contractversion.Plan(promo(41), contracts, states)

	assert.Equal(t, "analytics.b", contracts[0].UniqueID, "the caller's slice keeps its order")
	assert.Equal(t, map[string]int64{hashB: 1}, states["analytics.a"].FirstVersionByHash)
}

func TestPlan_NothingIncomingPlansNothing(t *testing.T) {
	plan := contractversion.Plan(promo(41), nil, nil)
	assert.Equal(t, contractversion.WritePlan{}, plan)
}
