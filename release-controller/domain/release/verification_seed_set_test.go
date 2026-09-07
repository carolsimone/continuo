package release_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

func seedNode(id, hash string) release.Node { return release.Node{UniqueID: id, ContentHash: hash} }

// TestVerificationSeedSet_EveryCase walks one node per rule of the seed set:
//   - core.orders: the rejected release changed it and validated it ok, the fix
//     did not touch it -> rebuilt, so descendants see the shape the release produced
//   - finance.report: the fix's own edit -> rebuilt
//   - svc3.sibling: changed vs prod, untouched by the fix, recorded failing -> a
//     still-unfixed failure another verification answers for -> cloned
//   - svc2.skipped: changed vs prod, untouched, recorded not-ok because its
//     upstream failed -> cloned
//   - other.promoted: matches current_prod (another release promoted it since)
//     even though it differs from the rejected candidate -> not changed at all
//   - finance.new_model: absent from both prod and the rejected candidate -> the
//     fix added it -> rebuilt
func TestVerificationSeedSet_EveryCase(t *testing.T) {
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

	got := release.VerificationSeedSet(verification, prod, rejected, failing)

	assert.Equal(t, []string{"core.orders", "finance.new_model", "finance.report"}, got)
}

// A rejection that recorded no per-node failure (a structural rejection such
// as duplicate_table with no claimants, or unbuildable upstream) excludes
// nothing: every node that differs from production is re-measured.
func TestVerificationSeedSet_NoFailingRecordedExcludesNothing(t *testing.T) {
	prod := release.Topology{seedNode("a", "1"), seedNode("b", "1")}
	rejected := release.Topology{seedNode("a", "2"), seedNode("b", "2")}
	verification := release.Topology{seedNode("a", "2"), seedNode("b", "2")}

	got := release.VerificationSeedSet(verification, prod, rejected, nil)

	assert.Equal(t, []string{"a", "b"}, got)
}

// A candidate identical to production has nothing to re-measure: the fix is
// proven by identity with production's own validated code.
func TestVerificationSeedSet_ProdIdenticalIsEmpty(t *testing.T) {
	prod := release.Topology{seedNode("a", "1"), seedNode("b", "1")}
	rejected := release.Topology{seedNode("a", "broken"), seedNode("b", "1")}
	verification := release.Topology{seedNode("a", "1"), seedNode("b", "1")}

	got := release.VerificationSeedSet(verification, prod, rejected, []string{"a"})

	assert.Empty(t, got)
}

// Output is sorted and deduplicated regardless of topology order.
func TestVerificationSeedSet_SortedAndDeduplicated(t *testing.T) {
	prod := release.Topology{}
	rejected := release.Topology{}
	verification := release.Topology{seedNode("z", "1"), seedNode("a", "1"), seedNode("z", "1")}

	got := release.VerificationSeedSet(verification, prod, rejected, nil)

	assert.Equal(t, []string{"a", "z"}, got)
}
