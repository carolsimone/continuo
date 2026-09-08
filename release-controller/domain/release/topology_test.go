package release_test

import (
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
)

// TestWithoutTests_DropsOnlyDbtTestNodes verifies WithoutTests strips only
// dbt-test nodes, leaves every other NodeType untouched, and does not mutate
// the receiver — the promoted topology and current_prod must diverge on
// tests without either one aliasing the other's backing array.
func TestWithoutTests_DropsOnlyDbtTestNodes(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "a.m", NodeType: "dbt-model"},
		{UniqueID: "test.p.not_null_m_id.1", NodeType: "dbt-test"},
		{UniqueID: "a.s", NodeType: "dbt-seed"},
	}

	got := topo.WithoutTests()

	assert.Equal(t, release.Topology{
		{UniqueID: "a.m", NodeType: "dbt-model"},
		{UniqueID: "a.s", NodeType: "dbt-seed"},
	}, got)
	assert.Len(t, topo, 3, "the receiver is not mutated")
}
