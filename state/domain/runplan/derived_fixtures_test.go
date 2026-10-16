package runplan_test

import (
	"cmp"
	"slices"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

// sourceTaskID is the id of table's task in a fixture source run.
func sourceTaskID(table string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("continuo/source-task/"+table))
}

// sourceRelease is the release a fixture source run is on.
const sourceRelease = "rel-source"

// sourceRun builds a source run of op on sourceRelease whose succeeded
// tables' tasks succeeded and whose other tables' tasks did not, each built on
// sourceRelease, sorted by key as TaskCollection.ListRunTasks returns them.
func sourceRun(op model.Operation, succeeded, unsucceeded []string) runplan.Source {
	var tasks []runplan.SourceTask
	for _, t := range succeeded {
		tasks = append(tasks, runplan.SourceTask{Key: key(t), TaskID: sourceTaskID(t), Succeeded: true, BuiltOn: sourceRelease})
	}
	for _, t := range unsucceeded {
		tasks = append(tasks, runplan.SourceTask{Key: key(t), TaskID: sourceTaskID(t), BuiltOn: sourceRelease})
	}
	slices.SortFunc(tasks, func(a, b runplan.SourceTask) int { return cmp.Compare(a.Key, b.Key) })
	return runplan.Source{Operation: op, Tasks: tasks}
}

// failedFailureRun is a run of e2e-schedule-failure:
// ftable_e and ftable_g exhausted their retries, which skipped ftable_f and
// ftable_h; every other task succeeded.
func failedFailureRun() runplan.Source {
	return sourceRun(model.OperationRun,
		[]string{"ftable_a", "ftable_b", "ftable_c", "ftable_d"},
		[]string{"ftable_e", "ftable_f", "ftable_g", "ftable_h"})
}

// inherited returns a planned task per table carried over from its fixture
// source task.
func inherited(tables ...string) []runplan.PlannedTask {
	out := make([]runplan.PlannedTask, 0, len(tables))
	for _, t := range tables {
		out = append(out, runplan.PlannedTask{Key: key(t), Inherited: true, InheritedFrom: sourceTaskID(t)})
	}
	return out
}

// sourceKeys returns the key of every task of src.
func sourceKeys(src runplan.Source) []topology.NodeKey {
	out := make([]topology.NodeKey, 0, len(src.Tasks))
	for _, t := range src.Tasks {
		out = append(out, t.Key)
	}
	return out
}

// changed returns dag without the tables in drop and with add appended. dag
// itself is left as it is.
func changed(dag []fixtureNode, drop []string, add ...fixtureNode) []fixtureNode {
	out := make([]fixtureNode, 0, len(dag)+len(add))
	for _, n := range dag {
		if !slices.Contains(drop, n.table) {
			out = append(out, n)
		}
	}
	return append(out, add...)
}

// retyped returns g with table's node type replaced by nodeType.
func retyped(g topology.Graph, table, nodeType string) topology.Graph {
	var nodes []topology.Node
	var edges []topology.Edge
	for _, k := range g.Keys() {
		n, _ := g.Node(k)
		if k == key(table) {
			n.NodeType = nodeType
		}
		nodes = append(nodes, n)
		for _, d := range g.Downstreams(k) {
			edges = append(edges, topology.Edge{Upstream: k, Downstream: d})
		}
	}
	return topology.NewGraph(nodes, edges)
}
