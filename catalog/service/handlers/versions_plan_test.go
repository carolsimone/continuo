package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/pkg/codebundle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func versionsTestLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func bundleFixture() codebundle.Bundle {
	return codebundle.Bundle{
		ContractVersion: 1,
		ReleaseID:       "rel-1",
		Nodes: map[string]codebundle.Node{
			"analytics.revenue": {
				Runtime: "dbt", RawCode: "select 1", CompiledCode: "select 1 compiled",
				Config:     map[string]any{"materialized": "table"},
				SourceHash: "s1", SharedCodeHash: "m1", ConfigHash: "c1",
				ContentHash: "sha256:abc",
				CodeUnitIDs: []string{"svc:m1"},
			},
		},
		SharedCode: map[string]codebundle.SharedCodeUnit{
			"svc:m1":     {Source: "macro one", Checksum: "u1", DependsOn: []string{"svc:m2"}},
			"svc:m2":     {Source: "macro two", Checksum: "u2", DependsOn: []string{"svc:m3"}},
			"svc:m3":     {Source: "macro three", Checksum: "u3"},
			"svc:unused": {Source: "unused", Checksum: "u9"},
		},
	}
}

// nodesFixture is the release's nodes as the swap writes them: the bundle's
// model and one the bundle does not carry.
func nodesFixture() []topology.ReleasePromotedTopologyNode {
	return []topology.ReleasePromotedTopologyNode{
		{UniqueID: "analytics.revenue", SchemaName: "analytics", TableName: "revenue", NodeType: "dbt-model", ContentHash: "sha256:abc"},
		{UniqueID: "analytics.seed_only", SchemaName: "analytics", TableName: "seed_only", NodeType: "dbt-seed"},
	}
}

func inputFixture(changed, bootstrap bool) domainModel.PromoteReleaseInput {
	in := domainModel.PromoteReleaseInput{
		ReleaseID: "rel-1", PromotionSeq: 4, Repo: "org/svc", CommitSHA: "deadbeef",
		Bootstrap: bootstrap, ChangedNodeIDs: []string{},
	}
	if changed {
		in.ChangedNodeIDs = []string{"analytics.revenue"}
	}
	return in
}

func TestPlanVersionWrite_CopiesTheBundleNode(t *testing.T) {
	in := planVersionWrite(inputFixture(true, false), bundleFixture(), nodesFixture(), versionsTestLogger())
	require.Len(t, in.Nodes, 1)
	n := in.Nodes[0]
	assert.Equal(t, "analytics.revenue", n.UniqueID)
	assert.Equal(t, "sha256:abc", n.ContentHash)
	assert.Equal(t, "s1", n.SourceHash)
	assert.Equal(t, "m1", n.SharedCodeHash)
	assert.Equal(t, "c1", n.ConfigHash)
	assert.Equal(t, "dbt", n.Runtime)
	assert.Equal(t, "select 1", n.RawCode)
	assert.Equal(t, "select 1 compiled", n.CompiledCode)
	assert.False(t, n.CompiledTruncated)
	assert.Equal(t, "rel-1", in.ReleaseID)
	assert.Equal(t, "org/svc", in.Repo)
	assert.Equal(t, "deadbeef", in.CommitSHA)
}

// config_json lets a reader see a materialization change without diffing SQL, so
// it must be canonical: two equal configs have to encode to the same string.
func TestPlanVersionWrite_EncodesConfigAsCanonicalJSON(t *testing.T) {
	b := bundleFixture()
	n := b.Nodes["analytics.revenue"]
	n.Config = map[string]any{"materialized": "incremental", "alias": "rev"}
	b.Nodes["analytics.revenue"] = n

	in := planVersionWrite(inputFixture(true, false), b, nodesFixture(), versionsTestLogger())
	assert.Equal(t, `{"alias":"rev","materialized":"incremental"}`, in.Nodes[0].ConfigJSON)

	var round map[string]any
	require.NoError(t, json.Unmarshal([]byte(in.Nodes[0].ConfigJSON), &round))
	assert.Equal(t, "incremental", round["materialized"])
}

func TestPlanVersionWrite_EmptyConfigEncodesAsEmptyObject(t *testing.T) {
	b := bundleFixture()
	n := b.Nodes["analytics.revenue"]
	n.Config = nil
	b.Nodes["analytics.revenue"] = n
	in := planVersionWrite(inputFixture(true, false), b, nodesFixture(), versionsTestLogger())
	assert.Equal(t, "{}", in.Nodes[0].ConfigJSON)
}

// The closure — not just the direct dependencies — is what the shared-code edges
// must point at, so "the model did not change but its hash flipped" resolves to
// the exact unit that did.
func TestPlanVersionWrite_ExpandsSharedCodeClosure(t *testing.T) {
	in := planVersionWrite(inputFixture(true, false), bundleFixture(), nodesFixture(), versionsTestLogger())
	var ids []string
	for _, r := range in.Nodes[0].UnitRefs {
		ids = append(ids, r.UnitID)
	}
	assert.ElementsMatch(t, []string{"svc:m1", "svc:m2", "svc:m3"}, ids)

	var unitIDs []string
	for _, u := range in.Units {
		unitIDs = append(unitIDs, u.UnitID)
	}
	assert.ElementsMatch(t, []string{"svc:m1", "svc:m2", "svc:m3"}, unitIDs,
		"a unit no written node depends on must not be written")
}

func TestPlanVersionWrite_ClosureCarriesEachUnitsChecksum(t *testing.T) {
	in := planVersionWrite(inputFixture(true, false), bundleFixture(), nodesFixture(), versionsTestLogger())
	got := map[string]string{}
	for _, r := range in.Nodes[0].UnitRefs {
		got[r.UnitID] = r.Checksum
	}
	assert.Equal(t, map[string]string{"svc:m1": "u1", "svc:m2": "u2", "svc:m3": "u3"}, got)
}

func TestPlanVersionWrite_ClosureSurvivesADependencyCycle(t *testing.T) {
	b := bundleFixture()
	b.SharedCode["svc:m3"] = codebundle.SharedCodeUnit{Source: "macro three", Checksum: "u3",
		DependsOn: []string{"svc:m1"}}
	in := planVersionWrite(inputFixture(true, false), b, nodesFixture(), versionsTestLogger())
	assert.Len(t, in.Nodes[0].UnitRefs, 3, "a cycle must terminate, not hang or duplicate")
}

func TestPlanVersionWrite_UnknownUnitIDIsSkipped(t *testing.T) {
	b := bundleFixture()
	n := b.Nodes["analytics.revenue"]
	n.CodeUnitIDs = []string{"svc:m1", "svc:missing"}
	b.Nodes["analytics.revenue"] = n
	in := planVersionWrite(inputFixture(true, false), b, nodesFixture(), versionsTestLogger())
	for _, r := range in.Nodes[0].UnitRefs {
		assert.NotEqual(t, "svc:missing", r.UnitID)
	}
}

func TestPlanVersionWrite_TruncatesOversizedCompiledCode(t *testing.T) {
	b := bundleFixture()
	n := b.Nodes["analytics.revenue"]
	n.CompiledCode = strings.Repeat("x", MaxCompiledCodeBytes+512)
	b.Nodes["analytics.revenue"] = n

	in := planVersionWrite(inputFixture(true, false), b, nodesFixture(), versionsTestLogger())
	assert.True(t, in.Nodes[0].CompiledTruncated)
	assert.LessOrEqual(t, len(in.Nodes[0].CompiledCode), MaxCompiledCodeBytes)
	assert.Equal(t, "select 1", in.Nodes[0].RawCode, "raw_code is not truncated")
}

func TestPlanVersionWrite_TruncationKeepsValidUTF8(t *testing.T) {
	b := bundleFixture()
	n := b.Nodes["analytics.revenue"]
	// "é" is two bytes, so a naive byte cut lands mid-rune.
	n.CompiledCode = strings.Repeat("é", MaxCompiledCodeBytes)
	b.Nodes["analytics.revenue"] = n

	in := planVersionWrite(inputFixture(true, false), b, nodesFixture(), versionsTestLogger())
	assert.True(t, in.Nodes[0].CompiledTruncated)
	assert.True(t, utf8.ValidString(in.Nodes[0].CompiledCode))
}

// changed_node_ids qualifies a write; it never triggers one.
func TestPlanVersionWrite_HealedProvenance(t *testing.T) {
	assert.False(t, planVersionWrite(inputFixture(true, false), bundleFixture(), nodesFixture(), versionsTestLogger()).Nodes[0].Healed,
		"changed and not a bootstrap: this release authored the code")
	assert.True(t, planVersionWrite(inputFixture(false, false), bundleFixture(), nodesFixture(), versionsTestLogger()).Nodes[0].Healed,
		"unchanged: a carrier release is healing a lost write")
	assert.True(t, planVersionWrite(inputFixture(true, true), bundleFixture(), nodesFixture(), versionsTestLogger()).Nodes[0].Healed,
		"bootstrap: baseline seeding, so the stamps are approximate")
}

func TestPlanVersionWrite_BundleNodeOutsideChangedNodeIDsIsHealed(t *testing.T) {
	in := inputFixture(true, false)
	in.ChangedNodeIDs = []string{"analytics.other"}
	assert.True(t, planVersionWrite(in, bundleFixture(), nodesFixture(), versionsTestLogger()).Nodes[0].Healed)
}

func TestPlanVersionWrite_CarriesThePromotionSeq(t *testing.T) {
	assert.Equal(t, int64(4), planVersionWrite(inputFixture(true, false), bundleFixture(), nodesFixture(), versionsTestLogger()).PromotionSeq)
}

func TestPlanVersionWrite_IsDeterministicallyOrdered(t *testing.T) {
	b := bundleFixture()
	b.Nodes["analytics.aaa"] = b.Nodes["analytics.revenue"]
	first := planVersionWrite(inputFixture(true, false), b, nodesFixture(), versionsTestLogger())
	second := planVersionWrite(inputFixture(true, false), b, nodesFixture(), versionsTestLogger())
	assert.Equal(t, first, second, "map iteration must not leak into the write order")
	require.Len(t, first.Nodes, 2)
	assert.Equal(t, "analytics.aaa", first.Nodes[0].UniqueID)
}

// The plan carries the release's nodes as the swap writes them, keyed by
// unique_id, so the repository can create a node the graph does not hold yet.
func TestPlanVersionWrite_CarriesTheTopology(t *testing.T) {
	in := planVersionWrite(inputFixture(true, false), bundleFixture(), nodesFixture(), versionsTestLogger())
	require.Len(t, in.Topology, 2)
	assert.Equal(t, "revenue", in.Topology["analytics.revenue"].TableName)
	assert.Equal(t, "dbt-seed", in.Topology["analytics.seed_only"].NodeType)
}

func planOf(n int) codeversion.WriteInput {
	in := codeversion.WriteInput{ReleaseID: "rel-1", PromotionSeq: 4, Repo: "org/svc",
		Topology: map[string]topology.ReleasePromotedTopologyNode{}}
	for i := 0; i < n; i++ {
		uid := fmt.Sprintf("analytics.n%03d", i)
		unit := "svc:even"
		if i%2 == 1 {
			unit = "svc:odd"
		}
		in.Nodes = append(in.Nodes, codeversion.NodeVersion{UniqueID: uid, ContentHash: "h",
			UnitRefs: []codeversion.UnitRef{{UnitID: unit, Checksum: "c"}}})
		in.Topology[uid] = topology.ReleasePromotedTopologyNode{UniqueID: uid}
	}
	in.Units = []codeversion.CodeUnitVersion{{UnitID: "svc:even", Checksum: "c"}, {UnitID: "svc:odd", Checksum: "c"}}
	return in
}

// Batches keep the plan's order and its promotion, hold at most size nodes,
// and carry only the shared code and artifact entries of their own nodes.
func TestVersionBatches_SplitThePlanInOrder(t *testing.T) {
	batches := versionBatches(planOf(5), 2)
	require.Len(t, batches, 3)
	var ids []string
	for i, b := range batches {
		assert.Equal(t, "rel-1", b.ReleaseID, "batch %d", i)
		assert.Equal(t, int64(4), b.PromotionSeq, "batch %d", i)
		assert.Equal(t, "org/svc", b.Repo, "batch %d", i)
		assert.Len(t, b.Topology, len(b.Nodes), "batch %d", i)
		for _, n := range b.Nodes {
			ids = append(ids, n.UniqueID)
			assert.Contains(t, b.Topology, n.UniqueID)
		}
	}
	assert.Equal(t, []string{"analytics.n000", "analytics.n001", "analytics.n002", "analytics.n003", "analytics.n004"}, ids)
	assert.Equal(t, []codeversion.CodeUnitVersion{{UnitID: "svc:even", Checksum: "c"}}, batches[2].Units,
		"the last batch's only node uses svc:even")
	assert.Len(t, batches[0].Units, 2)
}

func TestVersionBatches_EmptyPlanHasNoBatch(t *testing.T) {
	assert.Empty(t, versionBatches(planOf(0), 2))
}

// A plan node with no artifact entry stays in its batch: the repository
// decides what to do with it.
func TestVersionBatches_KeepANodeWithoutArtifactEntry(t *testing.T) {
	in := planOf(1)
	delete(in.Topology, "analytics.n000")
	batches := versionBatches(in, 2)
	require.Len(t, batches, 1)
	assert.Len(t, batches[0].Nodes, 1)
	assert.Empty(t, batches[0].Topology)
}
