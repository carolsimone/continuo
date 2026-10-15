package runplan_test

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// counts is a preview's tally of its tasks by reason.
type counts struct {
	FailedOrSkipped, New, ChangedUpstream, Downstream, Inherited int
}

// countsOf returns pv's tally after checking that it accounts for every task
// of its plan: WillRun is the number of tasks the plan executes, and WillRun
// plus Inherited is Total, the number of tasks.
func countsOf(t *testing.T, pv runplan.Preview) counts {
	t.Helper()
	executed := 0
	for _, task := range pv.Plan.Tasks {
		if !task.Inherited {
			executed++
		}
	}
	require.Equal(t, executed, pv.WillRun(), "WillRun counts the executed tasks")
	require.Equal(t, pv.FailedOrSkipped+pv.New+pv.ChangedUpstream+pv.Downstream, pv.WillRun())
	require.Equal(t, pv.WillRun()+pv.Inherited, pv.Total())
	require.Len(t, pv.Plan.Tasks, pv.Total(), "Total counts every task")
	return counts{
		FailedOrSkipped: pv.FailedOrSkipped, New: pv.New, ChangedUpstream: pv.ChangedUpstream,
		Downstream: pv.Downstream, Inherited: pv.Inherited,
	}
}

// node is a fixture node of the rebase schedule, owned by service-1.
func node(table string, upstream ...string) fixtureNode {
	return fixtureNode{table, "service-1", "rebase", upstream}
}

// twoBranches is the design's example: x feeds y, which feeds z, and p feeds
// q.
func twoBranches() topology.Graph {
	return releaseGraph([]fixtureNode{node("x"), node("y", "x"), node("z", "y"), node("p"), node("q", "p")})
}

// edited returns g with edit applied to table's node.
func edited(g topology.Graph, table string, edit func(n *topology.Node)) topology.Graph {
	nodes := make([]topology.Node, 0, g.Len())
	var edges []topology.Edge
	for _, k := range g.Keys() {
		n, _ := g.Node(k)
		if k == key(table) {
			edit(&n)
		}
		nodes = append(nodes, n)
		for _, d := range g.Downstreams(k) {
			edges = append(edges, topology.Edge{Upstream: k, Downstream: d})
		}
	}
	return topology.NewGraph(nodes, edges)
}

// rehashed returns g with each table's content hash changed.
func rehashed(g topology.Graph, tables ...string) topology.Graph {
	for _, table := range tables {
		g = edited(g, table, func(n *topology.Node) { n.ContentHash += "-fixed" })
	}
	return g
}

// rewired returns g with table reading upstream instead of what it read.
func rewired(g topology.Graph, table string, upstream ...string) topology.Graph {
	nodes := make([]topology.Node, 0, g.Len())
	var edges []topology.Edge
	for _, k := range g.Keys() {
		n, _ := g.Node(k)
		nodes = append(nodes, n)
		if k != key(table) {
			for _, u := range g.Upstreams(k) {
				edges = append(edges, topology.Edge{Upstream: u, Downstream: k})
			}
		}
	}
	for _, u := range upstream {
		edges = append(edges, topology.Edge{Upstream: key(u), Downstream: key(table)})
	}
	return topology.NewGraph(nodes, edges)
}

// executedTables returns the table of every task plan executes, in key order.
func executedTables(plan runplan.Plan) []string {
	var out []string
	for _, task := range plan.Tasks {
		if !task.Inherited {
			out = append(out, tableOf(task.Key))
		}
	}
	return out
}

// inheritedTables returns the table of every task plan inherits, in key
// order.
func inheritedTables(plan runplan.Plan) []string {
	var out []string
	for _, task := range plan.Tasks {
		if task.Inherited {
			out = append(out, tableOf(task.Key))
		}
	}
	return out
}

func tableOf(k topology.NodeKey) string { return string(k)[len(e2eSchema)+1:] }

// changedNode is the ChangedNode of a service-1 fixture table.
func changedNode(table string, downstream int) runplan.ChangedNode {
	return runplan.ChangedNode{Key: key(table), ServiceName: "service-1", SchemaName: e2eSchema, TableName: table, Downstream: downstream}
}

// The design's example: z failed because y wrote a wrong column. The live
// release fixes y and changes q on the other branch. With the option the
// rebase rebuilds y and then z; without it z alone. Either way q, on a branch
// nothing re-executed reads, stays inherited, and y is named.
func TestRebasePreview_TheDesignExample(t *testing.T) {
	src := sourceRun(model.OperationRun, []string{"x", "y", "p", "q"}, []string{"z"})
	live := rehashed(twoBranches(), "y", "q")

	on, err := runplan.Rebase{Source: src, SourceGraph: twoBranches(), IncludeChangedUpstream: true}.Preview(live)
	require.NoError(t, err)
	assert.Equal(t, []string{"y", "z"}, executedTables(on.Plan))
	assert.Equal(t, []string{"p", "q", "x"}, inheritedTables(on.Plan))
	assert.Equal(t, keys("y"), on.Plan.Ready)
	assert.Equal(t, []runplan.ChangedNode{changedNode("y", 1)}, on.Changed)
	assert.Equal(t, counts{FailedOrSkipped: 1, ChangedUpstream: 1, Inherited: 3}, countsOf(t, on))

	off, err := runplan.Rebase{Source: src, SourceGraph: twoBranches()}.Preview(live)
	require.NoError(t, err)
	assert.Equal(t, []string{"z"}, executedTables(off.Plan))
	assert.Equal(t, []string{"p", "q", "x", "y"}, inheritedTables(off.Plan))
	assert.Equal(t, keys("z"), off.Plan.Ready)
	assert.Equal(t, []runplan.ChangedNode{changedNode("y", 1)}, off.Changed, "the option's nodes are named whether or not it is set")
	assert.Equal(t, counts{FailedOrSkipped: 1, Inherited: 4}, countsOf(t, off))
}

// Select plans what Preview plans, with or without the option.
func TestRebasePreview_SelectPlansThePreviewsPlan(t *testing.T) {
	src := sourceRun(model.OperationRun, []string{"x", "y", "p", "q"}, []string{"z"})
	live := rehashed(twoBranches(), "y", "q")
	for _, include := range []bool{true, false} {
		s := runplan.Rebase{Source: src, SourceGraph: twoBranches(), IncludeChangedUpstream: include}
		pv, err := s.Preview(live)
		require.NoError(t, err)
		plan, err := s.Select(live)
		require.NoError(t, err)
		assert.Equal(t, pv.Plan, plan, "include=%v", include)
	}
}

// A service release moves every node's image, so an image alone is not a
// change.
func TestRebasePreview_AnImageAloneIsNotAChange(t *testing.T) {
	src := sourceRun(model.OperationRun, []string{"x", "y", "p", "q"}, []string{"z"})
	live := edited(twoBranches(), "y", func(n *topology.Node) { n.ImageTag = "service-1:2" })

	pv, err := runplan.Rebase{Source: src, SourceGraph: twoBranches(), IncludeChangedUpstream: true}.Preview(live)
	require.NoError(t, err)
	assert.Equal(t, []string{"z"}, executedTables(pv.Plan))
	assert.Empty(t, pv.Changed)
}

// A node is changed when what it executes changes: its content hash, its node
// type, the secret it runs with or the nodes it reads. The test count, the
// unique id and the owning service are not compared.
func TestRebasePreview_WhatMakesANodeChanged(t *testing.T) {
	src := sourceRun(model.OperationRun, []string{"x", "y", "p", "q"}, []string{"z"})
	for name, tc := range map[string]struct {
		live    topology.Graph
		changed bool
	}{
		"content hash": {rehashed(twoBranches(), "y"), true},
		"node type":    {edited(twoBranches(), "y", func(n *topology.Node) { n.NodeType = "dbt-snapshot" }), true},
		"secret ref":   {edited(twoBranches(), "y", func(n *topology.Node) { n.SecretRef = "warehouse-b" }), true},
		"upstream set": {rewired(twoBranches(), "y", "x", "p"), true},
		"test count":   {edited(twoBranches(), "y", func(n *topology.Node) { n.TestCount = count(4) }), false},
		"unique id":    {edited(twoBranches(), "y", func(n *topology.Node) { n.UniqueID = "model.other.y" }), false},
		"service":      {edited(twoBranches(), "y", func(n *topology.Node) { n.ServiceName = "service-9" }), false},
	} {
		t.Run(name, func(t *testing.T) {
			pv, err := runplan.Rebase{Source: src, SourceGraph: twoBranches(), IncludeChangedUpstream: true}.Preview(tc.live)
			require.NoError(t, err)
			if !tc.changed {
				assert.Equal(t, []string{"z"}, executedTables(pv.Plan))
				assert.Empty(t, pv.Changed)
				return
			}
			assert.Equal(t, []string{"y", "z"}, executedTables(pv.Plan))
			require.Len(t, pv.Changed, 1)
			assert.Equal(t, key("y"), pv.Changed[0].Key)
		})
	}
}

// A node the source release does not hold, among the source run's nodes, has
// nothing to compare against, so it is never changed.
func TestRebasePreview_ANodeTheSourceGraphLacksIsNeverChanged(t *testing.T) {
	src := sourceRun(model.OperationRun, []string{"x", "y", "p", "q"}, []string{"z"})
	sourceGraph := twoBranches().Restrict(keys("x", "z", "p", "q"))

	pv, err := runplan.Rebase{Source: src, SourceGraph: sourceGraph, IncludeChangedUpstream: true}.Preview(rehashed(twoBranches(), "y"))
	require.NoError(t, err)
	assert.Equal(t, []string{"z"}, executedTables(pv.Plan))
	assert.Empty(t, pv.Changed)
}

// f failed and skipped k, which also reads c. c changed: it is upstream of
// the skipped node only, and is run with it.
func TestRebasePreview_AChangedNodeUpstreamOfASkippedNode(t *testing.T) {
	g := releaseGraph([]fixtureNode{node("f"), node("c"), node("k", "f", "c")})
	src := sourceRun(model.OperationRun, []string{"c"}, []string{"f", "k"})

	pv, err := runplan.Rebase{Source: src, SourceGraph: g, IncludeChangedUpstream: true}.Preview(rehashed(g, "c"))
	require.NoError(t, err)
	assert.Equal(t, []string{"c", "f", "k"}, executedTables(pv.Plan))
	assert.Equal(t, keys("c", "f"), pv.Plan.Ready)
	assert.Equal(t, []runplan.ChangedNode{changedNode("c", 1)}, pv.Changed)
	assert.Equal(t, counts{FailedOrSkipped: 2, ChangedUpstream: 1}, countsOf(t, pv))
}

// v feeds the failed s and w; u feeds w only, so u is no ancestor of s. Both
// changed. Rebuilding v rebuilds w, and w reads u, so u joins on a second
// pass; the widening stops once a pass adds nothing.
func TestRebasePreview_ChangedAncestorsAreAddedUntilNoneIsLeftOut(t *testing.T) {
	g := releaseGraph([]fixtureNode{node("v"), node("u"), node("s", "v"), node("w", "v", "u")})
	require.NotContains(t, g.Ancestors(keys("s")), key("u"))
	src := sourceRun(model.OperationRun, []string{"u", "v", "w"}, []string{"s"})
	live := rehashed(g, "u", "v")

	on, err := runplan.Rebase{Source: src, SourceGraph: g, IncludeChangedUpstream: true}.Preview(live)
	require.NoError(t, err)
	assert.Equal(t, []string{"s", "u", "v", "w"}, executedTables(on.Plan))
	assert.Equal(t, keys("u", "v"), on.Plan.Ready)
	assert.Equal(t, []runplan.ChangedNode{changedNode("u", 1), changedNode("v", 2)}, on.Changed)
	assert.Equal(t, counts{FailedOrSkipped: 1, ChangedUpstream: 2, Downstream: 1}, countsOf(t, on))

	off, err := runplan.Rebase{Source: src, SourceGraph: g}.Preview(live)
	require.NoError(t, err)
	assert.Equal(t, []string{"s"}, executedTables(off.Plan))
	assert.Equal(t, on.Changed, off.Changed)
	assert.Equal(t, counts{FailedOrSkipped: 1, Inherited: 3}, countsOf(t, off))
}

// u, changed, feeds the failed s and the leaf l, which changed too. l is run
// because it reads u; with nothing below it, it is upstream of nothing the
// rebase runs, so it is counted downstream and not named.
func TestRebasePreview_AChangedLeafBelowAChangedNodeIsDownstream(t *testing.T) {
	g := releaseGraph([]fixtureNode{node("u"), node("s", "u"), node("l", "u")})
	src := sourceRun(model.OperationRun, []string{"u", "l"}, []string{"s"})

	pv, err := runplan.Rebase{Source: src, SourceGraph: g, IncludeChangedUpstream: true}.Preview(rehashed(g, "u", "l"))
	require.NoError(t, err)
	assert.Equal(t, []string{"l", "s", "u"}, executedTables(pv.Plan))
	assert.Equal(t, []runplan.ChangedNode{changedNode("u", 2)}, pv.Changed)
	assert.Equal(t, counts{FailedOrSkipped: 1, ChangedUpstream: 1, Downstream: 1}, countsOf(t, pv))
}

// On e2e-schedule-failure ftable_e and ftable_g failed. A fix to ftable_c,
// which feeds ftable_e, rebuilds ftable_c and therefore ftable_d, a success
// that reads it; ftable_a and ftable_b stay inherited.
func TestRebasePreview_OfTheFailureSchedule(t *testing.T) {
	sourceGraph := scheduleGraph(e2eDAG, "e2e-schedule-failure")
	live := rehashed(sourceGraph, "ftable_c")

	pv, err := runplan.Rebase{Source: failedFailureRun(), SourceGraph: sourceGraph, IncludeChangedUpstream: true}.Preview(live)
	require.NoError(t, err)
	assert.Equal(t, planOf(append(
		inherited("ftable_a", "ftable_b"),
		pending("ftable_c", "ftable_d", "ftable_e", "ftable_f", "ftable_g", "ftable_h")...,
	), "ftable_c", "ftable_g"), pv.Plan)
	assert.Equal(t, []runplan.ChangedNode{{
		Key: key("ftable_c"), ServiceName: "service-3", SchemaName: e2eSchema, TableName: "ftable_c", Downstream: 3,
	}}, pv.Changed)
	assert.Equal(t, counts{FailedOrSkipped: 4, ChangedUpstream: 1, Downstream: 1, Inherited: 2}, countsOf(t, pv))
}

// The changed nodes are listed by service, schema and table, whatever the
// order of their keys; their names are the live node's.
func TestRebasePreview_ChangedNodesAreOrderedByServiceSchemaAndTable(t *testing.T) {
	g := releaseGraph([]fixtureNode{
		{"a_node", "service-2", "rebase", nil},
		{"b_node", "service-1", "rebase", nil},
		{"z", "service-1", "rebase", []string{"a_node", "b_node"}},
	})
	src := sourceRun(model.OperationRun, []string{"a_node", "b_node"}, []string{"z"})
	live := edited(rehashed(g, "a_node", "b_node"), "b_node", func(n *topology.Node) { n.TableName = "B_Node" })

	pv, err := runplan.Rebase{Source: src, SourceGraph: g}.Preview(live)
	require.NoError(t, err)
	assert.Equal(t, []runplan.ChangedNode{
		{Key: key("b_node"), ServiceName: "service-1", SchemaName: e2eSchema, TableName: "B_Node", Downstream: 1},
		{Key: key("a_node"), ServiceName: "service-2", SchemaName: e2eSchema, TableName: "a_node", Downstream: 1},
	}, pv.Changed)
}

// A changed node the option pulls in is executed, so its node type must be
// one the vocabulary declares; without the option it stays inherited and the
// rebase is not refused.
func TestRebasePreview_AnUndeclaredNodeTypeTheOptionPullsInIsRefused(t *testing.T) {
	src := sourceRun(model.OperationRun, []string{"x", "y", "p", "q"}, []string{"z"})
	live := edited(twoBranches(), "y", func(n *topology.Node) { n.NodeType = "dbt-macro" })

	_, err := runplan.Rebase{Source: src, SourceGraph: twoBranches(), IncludeChangedUpstream: true}.Preview(live)
	require.ErrorIs(t, err, runplan.ErrInvalidNodeType)
	assert.Contains(t, err.Error(), "e2e_schema.y")

	_, err = runplan.Rebase{Source: src, SourceGraph: twoBranches()}.Preview(live)
	assert.NoError(t, err)
}

// Preview refuses what Select refuses.
func TestRebasePreview_Refusals(t *testing.T) {
	_, err := runplan.Rebase{Source: sourceRun(model.OperationTest, nil, []string{"seed_table_1"})}.Preview(scheduleGraph(e2eDAG, "seed"))
	assert.ErrorIs(t, err, runplan.ErrRerunOfTestUnsupported)

	all := sourceRun(model.OperationRun, []string{"x", "y", "z", "p", "q"}, nil)
	_, err = runplan.Rebase{Source: all, SourceGraph: twoBranches(), IncludeChangedUpstream: true}.Preview(rehashed(twoBranches(), "y"))
	assert.ErrorIs(t, err, runplan.ErrNothingToRerun, "a change alone, with nothing failed or new, is no rebase")
}
