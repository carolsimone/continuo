package release_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

func seedNode(id, hash string) release.Node { return release.Node{UniqueID: id, ContentHash: hash} }

// TestVerificationBuildSets_EveryCase walks one node per rule of the partition:
//   - core.orders: the rejected release changed it and validated it ok, the fix
//     did not touch it -> context: rebuilt for its descendants but never expanded
//   - finance.report: the fix's own edit -> scope
//   - svc3.sibling: changed vs prod, untouched by the fix, recorded failing -> a
//     still-unfixed failure another verification answers for -> neither (cloned)
//   - svc2.skipped: changed vs prod, untouched, recorded not-ok because its
//     upstream failed -> neither (cloned)
//   - other.promoted: matches current_prod (another release promoted it since)
//     even though it differs from the rejected candidate -> not changed at all
//   - finance.new_model: absent from both prod and the rejected candidate -> the
//     fix added it -> scope
func TestVerificationBuildSets_EveryCase(t *testing.T) {
	prod := release.Topology{
		seedNode("core.orders", "A1"), seedNode("finance.report", "R1"), seedNode("svc3.sibling", "S_old"),
		seedNode("other.promoted", "P_new"), seedNode("svc2.skipped", "K_old"),
	}
	rejected := release.Topology{
		seedNode("core.orders", "A2"), seedNode("finance.report", "R1"), seedNode("svc3.sibling", "S_broken"),
		seedNode("other.promoted", "P_old"), seedNode("svc2.skipped", "K_new"),
	}
	verification := release.Topology{
		seedNode("core.orders", "A2"), seedNode("finance.report", "R2"), seedNode("svc3.sibling", "S_broken"),
		seedNode("other.promoted", "P_new"), seedNode("svc2.skipped", "K_new"), seedNode("finance.new_model", "N1"),
	}
	failing := []string{"finance.report", "svc3.sibling", "svc2.skipped"}

	scope, context := release.VerificationBuildSets(verification, prod, rejected, failing)

	assert.Equal(t, []string{"finance.new_model", "finance.report"}, scope, "the fix's own delta seeds the scope")
	assert.Equal(t, []string{"core.orders"}, context, "the rejected release's validated-ok change is a context rebuild")
}

// A rejection that recorded no per-node failure (a structural rejection such as
// duplicate_table with no claimants, or unbuildable upstream) drops nothing that
// differs from production: the fix's own edit seeds the scope, and every other
// node that differs is a context rebuild rather than being excluded.
func TestVerificationBuildSets_NoFailingRecordedDropsNothing(t *testing.T) {
	prod := release.Topology{seedNode("a", "1"), seedNode("b", "1")}
	rejected := release.Topology{seedNode("a", "2"), seedNode("b", "2")}
	verification := release.Topology{seedNode("a", "3"), seedNode("b", "2")}

	scope, context := release.VerificationBuildSets(verification, prod, rejected, nil)

	assert.Equal(t, []string{"a"}, scope, "the fix touched a (differs from the rejected candidate) -> scope")
	assert.Equal(t, []string{"b"}, context, "b differs from prod, untouched, not failing -> context, not dropped")
}

// A candidate identical to production has nothing to rebuild: the fix is proven
// by identity with production's own validated code.
func TestVerificationBuildSets_ProdIdenticalIsEmpty(t *testing.T) {
	prod := release.Topology{seedNode("a", "1"), seedNode("b", "1")}
	rejected := release.Topology{seedNode("a", "broken"), seedNode("b", "1")}
	verification := release.Topology{seedNode("a", "1"), seedNode("b", "1")}

	scope, context := release.VerificationBuildSets(verification, prod, rejected, []string{"a"})

	assert.Empty(t, scope)
	assert.Empty(t, context)
}

// Each set is sorted and deduplicated regardless of topology order.
func TestVerificationBuildSets_SortedAndDeduplicated(t *testing.T) {
	prod := release.Topology{}
	rejected := release.Topology{}
	verification := release.Topology{seedNode("z", "1"), seedNode("a", "1"), seedNode("z", "1")}

	scope, context := release.VerificationBuildSets(verification, prod, rejected, nil)

	assert.Equal(t, []string{"a", "z"}, scope, "absent from the rejected candidate -> the fix added them -> scope")
	assert.Empty(t, context)
}
