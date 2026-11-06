package release

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func failed(ids ...string) []FailedNode {
	nodes := make([]FailedNode, len(ids))
	for i, id := range ids {
		nodes[i] = FailedNode{NodeID: id, Service: "svc"}
	}
	return nodes
}

func numberedNodes(n int) []FailedNode {
	nodes := make([]FailedNode, n)
	for i := range nodes {
		nodes[i] = FailedNode{NodeID: fmt.Sprintf("n%d", i+1), Service: "svc"}
	}
	return nodes
}

func TestSummarizeValidationFailure_NamesTheFailedNodeAndItsService(t *testing.T) {
	got := SummarizeValidationFailure(133,
		[]FailedNode{{NodeID: "analytics.daily_transactions", Service: "core"}}, nil)

	assert.Equal(t, "1 of 133 nodes did not pass validation: analytics.daily_transactions (core)", got)
}

func TestSummarizeValidationFailure_JoinsSeveralNodesWithAnd(t *testing.T) {
	got := SummarizeValidationFailure(10, []FailedNode{
		{NodeID: "a", Service: "core"},
		{NodeID: "b", Service: "finance"},
		{NodeID: "c", Service: "finance"},
	}, nil)

	assert.Equal(t, "3 of 10 nodes did not pass validation: a (core), b (finance) and c (finance)", got)
}

func TestSummarizeValidationFailure_OmitsTheServiceWhenTheTopologyDoesNotNameOne(t *testing.T) {
	got := SummarizeValidationFailure(4, []FailedNode{{NodeID: "orphan"}}, nil)

	assert.Equal(t, "1 of 4 nodes did not pass validation: orphan", got)
}

func TestSummarizeValidationFailure_NamesAtMostFiveNodesAndCountsTheRest(t *testing.T) {
	got := SummarizeValidationFailure(40, numberedNodes(12), nil)

	assert.Equal(t,
		"12 of 40 nodes did not pass validation: n1 (svc), n2 (svc), n3 (svc), n4 (svc), n5 (svc) and 7 more",
		got)
}

func TestSummarizeValidationFailure_ExactlyFiveNodesAreAllNamed(t *testing.T) {
	got := SummarizeValidationFailure(9, numberedNodes(5), nil)

	assert.Equal(t,
		"5 of 9 nodes did not pass validation: n1 (svc), n2 (svc), n3 (svc), n4 (svc) and n5 (svc)",
		got)
}

func TestSummarizeValidationFailure_NamesTheChangedUpstreamNearestFirst(t *testing.T) {
	got := SummarizeValidationFailure(10, failed("a"), []ChangedAncestor{
		{NodeID: "far", Service: "finance", Depth: 3},
		{NodeID: "near", Service: "core", Depth: 1},
	})

	assert.Equal(t,
		"1 of 10 nodes did not pass validation: a (svc); changed upstream of the failing nodes: near (core) and far (finance)",
		got)
}

func TestSummarizeValidationFailure_BreaksADepthTieById(t *testing.T) {
	got := SummarizeValidationFailure(10, failed("a"), []ChangedAncestor{
		{NodeID: "z", Service: "s", Depth: 1},
		{NodeID: "m", Service: "s", Depth: 1},
	})

	assert.Contains(t, got, "changed upstream of the failing nodes: m (s) and z (s)")
}

func TestSummarizeValidationFailure_ListsASharedAncestorOnceAtItsNearestDepth(t *testing.T) {
	// Two failing nodes reach the same changed ancestor: once directly, once
	// through a longer path. It is one upstream node, not two.
	got := SummarizeValidationFailure(10, failed("a", "b"), []ChangedAncestor{
		{NodeID: "x", Service: "finance", Depth: 4},
		{NodeID: "y", Service: "core", Depth: 2},
		{NodeID: "x", Service: "finance", Depth: 1},
	})

	assert.Contains(t, got, "changed upstream of the failing nodes: x (finance) and y (core)")
}

func TestSummarizeValidationFailure_DoesNotRepeatAFailingNodeAsUpstream(t *testing.T) {
	// b failed and so did its changed ancestor a: a is already named as failing.
	// Only a changed ancestor that did not fail adds information.
	got := SummarizeValidationFailure(10, failed("a", "b"), []ChangedAncestor{
		{NodeID: "a", Service: "svc", Depth: 1},
		{NodeID: "x", Service: "finance", Depth: 2},
	})

	assert.Equal(t,
		"2 of 10 nodes did not pass validation: a (svc) and b (svc); changed upstream of the failing nodes: x (finance)",
		got)
}

func TestSummarizeValidationFailure_OmitsTheUpstreamClauseWhenEveryAncestorFailed(t *testing.T) {
	got := SummarizeValidationFailure(10, failed("a", "b"), []ChangedAncestor{
		{NodeID: "a", Service: "svc", Depth: 1},
	})

	assert.Equal(t, "2 of 10 nodes did not pass validation: a (svc) and b (svc)", got)
}

func TestSummarizeValidationFailure_NamesAtMostThreeUpstreamNodesAndCountsTheRest(t *testing.T) {
	ancestors := make([]ChangedAncestor, 6)
	for i := range ancestors {
		ancestors[i] = ChangedAncestor{NodeID: fmt.Sprintf("u%d", i+1), Service: "s", Depth: i + 1}
	}

	got := SummarizeValidationFailure(10, failed("a"), ancestors)

	assert.Contains(t, got, "changed upstream of the failing nodes: u1 (s), u2 (s), u3 (s) and 3 more")
}

func TestSummarizeValidationFailure_SaysSoWhenNoNodeReportedAFailure(t *testing.T) {
	// The aggregate status said failed, but no per-node row survives.
	got := SummarizeValidationFailure(7, nil, nil)

	assert.Equal(t, "validation failed, but no per-node failure was recorded", got)
}

func TestSummarizeValidationFailure_DoesNotReorderTheCallersAncestors(t *testing.T) {
	in := []ChangedAncestor{
		{NodeID: "far", Depth: 3},
		{NodeID: "near", Depth: 1},
	}

	SummarizeValidationFailure(10, failed("a"), in)

	assert.Equal(t, "far", in[0].NodeID, "the ancestors also feed the rejection payload and must keep their order")
}

// A stage can fail for thousands of nodes, each reaching many changed
// ancestors; the summary must stay linear in their number and cost no more to
// print than a handful of names.
func BenchmarkSummarizeValidationFailure(b *testing.B) {
	failing := numberedNodes(10_000)
	ancestors := make([]ChangedAncestor, 50_000)
	for i := range ancestors {
		ancestors[i] = ChangedAncestor{NodeID: fmt.Sprintf("u%d", i%2_000), Service: "svc", Depth: i%7 + 1}
	}

	b.ReportAllocs()
	for b.Loop() {
		SummarizeValidationFailure(100_000, failing, ancestors)
	}
}

func TestSummarizeSeedFailure_NamesTheSeedsThatDidNotBuild(t *testing.T) {
	got := SummarizeSeedFailure([]FailedNode{
		{NodeID: "seed_a", Service: "finance"},
		{NodeID: "seed_b", Service: "core"},
	})

	assert.Equal(t, "2 seeds did not build: seed_a (finance) and seed_b (core)", got)
}

func TestSummarizeSeedFailure_SingularForOneSeed(t *testing.T) {
	got := SummarizeSeedFailure([]FailedNode{{NodeID: "seed_a", Service: "finance"}})

	assert.Equal(t, "1 seed did not build: seed_a (finance)", got)
}

func TestSummarizeSeedFailure_NamesAtMostFiveAndCountsTheRest(t *testing.T) {
	got := SummarizeSeedFailure(numberedNodes(8))

	assert.Equal(t,
		"8 seeds did not build: n1 (svc), n2 (svc), n3 (svc), n4 (svc), n5 (svc) and 3 more",
		got)
}

func TestSummarizeSeedFailure_SaysSoWhenNoNodeReportedAFailure(t *testing.T) {
	assert.Equal(t, "seed build failed, but no per-node failure was recorded", SummarizeSeedFailure(nil))
}

func TestSummarizeCompileFailure_NamesWhatFailedToCompile(t *testing.T) {
	assert.Equal(t, "dbt compile failed for: core",
		SummarizeCompileFailure([]FailedNode{{NodeID: "core"}}))
	assert.Equal(t, "dbt compile failed for: core and finance",
		SummarizeCompileFailure([]FailedNode{{NodeID: "core"}, {NodeID: "finance"}}))
}

func TestSummarizeCompileFailure_SaysSoWhenNoNodeReportedAFailure(t *testing.T) {
	assert.Equal(t, "dbt compile failed, but no per-node failure was recorded", SummarizeCompileFailure(nil))
}
