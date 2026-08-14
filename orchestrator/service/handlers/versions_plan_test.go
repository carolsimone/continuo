package handlers

import (
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/carolsimone/continuo/pkg/codebundle"
	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
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
				Config:      map[string]any{"materialized": "table"},
				SourceHash:  "s1", SharedCodeHash: "m1", ConfigHash: "c1",
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

func inputFixture(changed, bootstrap bool) domainModel.PromoteReleaseInput {
	return domainModel.PromoteReleaseInput{
		ReleaseID: "rel-1", Repo: "org/svc", CommitSHA: "deadbeef", Bootstrap: bootstrap,
		Topology: []domainEvent.ReleasePromotedNode{
			{UniqueID: "analytics.revenue", ContentHash: "sha256:abc", Changed: changed},
		},
	}
}

func TestPlanVersionWrite_CopiesTheBundleNode(t *testing.T) {
	in := planVersionWrite(inputFixture(true, false), bundleFixture(), versionsTestLogger())
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

	in := planVersionWrite(inputFixture(true, false), b, versionsTestLogger())
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
	in := planVersionWrite(inputFixture(true, false), b, versionsTestLogger())
	assert.Equal(t, "{}", in.Nodes[0].ConfigJSON)
}

// The closure — not just the direct dependencies — is what the shared-code edges
// must point at, so "the model did not change but its hash flipped" resolves to
// the exact unit that did.
func TestPlanVersionWrite_ExpandsSharedCodeClosure(t *testing.T) {
	in := planVersionWrite(inputFixture(true, false), bundleFixture(), versionsTestLogger())
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
	in := planVersionWrite(inputFixture(true, false), bundleFixture(), versionsTestLogger())
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
	in := planVersionWrite(inputFixture(true, false), b, versionsTestLogger())
	assert.Len(t, in.Nodes[0].UnitRefs, 3, "a cycle must terminate, not hang or duplicate")
}

func TestPlanVersionWrite_UnknownUnitIDIsSkipped(t *testing.T) {
	b := bundleFixture()
	n := b.Nodes["analytics.revenue"]
	n.CodeUnitIDs = []string{"svc:m1", "svc:missing"}
	b.Nodes["analytics.revenue"] = n
	in := planVersionWrite(inputFixture(true, false), b, versionsTestLogger())
	for _, r := range in.Nodes[0].UnitRefs {
		assert.NotEqual(t, "svc:missing", r.UnitID)
	}
}

func TestPlanVersionWrite_TruncatesOversizedCompiledCode(t *testing.T) {
	b := bundleFixture()
	n := b.Nodes["analytics.revenue"]
	n.CompiledCode = strings.Repeat("x", MaxCompiledCodeBytes+512)
	b.Nodes["analytics.revenue"] = n

	in := planVersionWrite(inputFixture(true, false), b, versionsTestLogger())
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

	in := planVersionWrite(inputFixture(true, false), b, versionsTestLogger())
	assert.True(t, in.Nodes[0].CompiledTruncated)
	assert.True(t, utf8.ValidString(in.Nodes[0].CompiledCode))
}

// The event's flags qualify a write; they never trigger one.
func TestPlanVersionWrite_HealedProvenance(t *testing.T) {
	assert.False(t, planVersionWrite(inputFixture(true, false), bundleFixture(), versionsTestLogger()).Nodes[0].Healed,
		"changed and not a bootstrap: this release authored the code")
	assert.True(t, planVersionWrite(inputFixture(false, false), bundleFixture(), versionsTestLogger()).Nodes[0].Healed,
		"unchanged: a carrier release is healing a lost write")
	assert.True(t, planVersionWrite(inputFixture(true, true), bundleFixture(), versionsTestLogger()).Nodes[0].Healed,
		"bootstrap: baseline seeding, so the stamps are approximate")
}

func TestPlanVersionWrite_BundleNodeAbsentFromTopologyIsHealed(t *testing.T) {
	in := inputFixture(true, false)
	in.Topology = nil
	assert.True(t, planVersionWrite(in, bundleFixture(), versionsTestLogger()).Nodes[0].Healed)
}

func TestPlanVersionWrite_IsDeterministicallyOrdered(t *testing.T) {
	b := bundleFixture()
	b.Nodes["analytics.aaa"] = b.Nodes["analytics.revenue"]
	first := planVersionWrite(inputFixture(true, false), b, versionsTestLogger())
	second := planVersionWrite(inputFixture(true, false), b, versionsTestLogger())
	assert.Equal(t, first, second, "map iteration must not leak into the write order")
	require.Len(t, first.Nodes, 2)
	assert.Equal(t, "analytics.aaa", first.Nodes[0].UniqueID)
}
