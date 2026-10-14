package runplan_test

import (
	"cmp"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"
)

// e2eSchema is the schema every e2e fixture model builds in
// (dbt/services/service-*/profiles.yml).
const e2eSchema = "e2e_schema"

// fixtureNode is one node of the e2e suite's fixed DAG: the table it builds,
// the service that owns it, its schedule and the tables it reads.
type fixtureNode struct {
	table    string
	service  string
	schedule string
	upstream []string
}

// e2eDAG mirrors tests/e2e/seed_topology_test.go and the dbt projects under
// dbt/services: the seed schedule's three seeds, e2e-schedule's layered DAG
// over them, and e2e-schedule-failure, whose ftable_e and ftable_g fail.
var e2eDAG = []fixtureNode{
	{"seed_table_1", "service-1", "seed", nil},
	{"seed_table_2", "service-1", "seed", nil},
	{"seed_table_3", "service-1", "seed", nil},
	{"table_a", "service-1", "e2e-schedule", []string{"seed_table_1"}},
	{"table_b", "service-1", "e2e-schedule", []string{"seed_table_2"}},
	{"table_c", "service-1", "e2e-schedule", []string{"seed_table_3"}},
	{"table_d", "service-3", "e2e-schedule", []string{"table_a", "table_b"}},
	{"table_e", "service-3", "e2e-schedule", []string{"table_b", "table_c"}},
	{"table_f", "service-3", "e2e-schedule", []string{"table_a", "table_c"}},
	{"table_g", "service-2", "e2e-schedule", []string{"table_d", "table_e"}},
	{"table_h", "service-2", "e2e-schedule", []string{"table_e", "table_f"}},
	{"table_i", "service-3", "e2e-schedule", []string{"table_g", "table_h"}},
	{"table_j", "service-3", "e2e-schedule", []string{"table_g", "table_h"}},
	{"ftable_a", "service-1", "e2e-schedule-failure", nil},
	{"ftable_b", "service-1", "e2e-schedule-failure", nil},
	{"ftable_c", "service-3", "e2e-schedule-failure", []string{"ftable_a", "ftable_b"}},
	{"ftable_d", "service-2", "e2e-schedule-failure", []string{"ftable_c"}},
	{"ftable_e", "service-2", "e2e-schedule-failure", []string{"ftable_c"}},
	{"ftable_f", "service-3", "e2e-schedule-failure", []string{"ftable_d", "ftable_e"}},
	{"ftable_g", "service-3", "e2e-schedule-failure", []string{"ftable_a"}},
	{"ftable_h", "service-2", "e2e-schedule-failure", []string{"ftable_g"}},
}

// e2eTestCounts is the data-test count of the seeds that carry tests
// (dbt/services/service-1/seeds/schema.yml); every other node has a known 0.
var e2eTestCounts = map[string]int{"seed_table_1": 2, "seed_table_3": 1}

func key(table string) topology.NodeKey { return topology.KeyOf(e2eSchema, table) }

func keys(tables ...string) []topology.NodeKey {
	out := make([]topology.NodeKey, 0, len(tables))
	for _, t := range tables {
		out = append(out, key(t))
	}
	return out
}

func count(n int) *int { return &n }

// storedNode is a fixture node as the release stores it.
func storedNode(n fixtureNode) topology.Node {
	nodeType := "dbt-model"
	if strings.HasPrefix(n.table, "seed_table") {
		nodeType = "dbt-seed"
	}
	return topology.Node{
		Key: key(n.table), UniqueID: e2eSchema + "." + n.table, ServiceName: n.service,
		SchemaName: e2eSchema, TableName: n.table, NodeType: nodeType,
		TestCount: count(e2eTestCounts[n.table]), ContentHash: "h-" + n.table,
		ImageTag: n.service + ":1", ScheduleName: n.schedule,
	}
}

// releaseGraph returns the whole release dag describes.
func releaseGraph(dag []fixtureNode) topology.Graph {
	nodes := make([]topology.Node, 0, len(dag))
	var edges []topology.Edge
	for _, n := range dag {
		nodes = append(nodes, storedNode(n))
		for _, u := range n.upstream {
			edges = append(edges, topology.Edge{Upstream: key(u), Downstream: key(n.table)})
		}
	}
	return topology.NewGraph(nodes, edges)
}

// scheduleGraph returns what TopologyRepository.ScheduleGraph loads for
// schedule from the release dag describes: the schedule's nodes, their
// one-hop dbt-seed upstreams from any schedule, and the edges among them.
func scheduleGraph(dag []fixtureNode, schedule string) topology.Graph {
	release := releaseGraph(dag)
	var selected []topology.NodeKey
	for _, n := range dag {
		if n.schedule != schedule {
			continue
		}
		selected = append(selected, key(n.table))
		for _, u := range release.Upstreams(key(n.table)) {
			if up, _ := release.Node(u); up.NodeType == "dbt-seed" {
				selected = append(selected, u)
			}
		}
	}
	return release.Restrict(selected)
}

// pending returns a pending planned task per table.
func pending(tables ...string) []runplan.PlannedTask {
	out := make([]runplan.PlannedTask, 0, len(tables))
	for _, t := range tables {
		out = append(out, runplan.PlannedTask{Key: key(t)})
	}
	return out
}

// planOf returns the plan of tasks, sorted by key, with ready dispatched at
// creation.
func planOf(tasks []runplan.PlannedTask, ready ...string) runplan.Plan {
	sorted := slices.Clone(tasks)
	slices.SortFunc(sorted, func(a, b runplan.PlannedTask) int { return cmp.Compare(a.Key, b.Key) })
	return runplan.Plan{Tasks: sorted, Ready: keys(ready...)}
}

// withNode returns a one-node graph holding table, owned by service-1, with
// the given node type and data-test count.
func withNode(table, nodeType string, testCount *int) topology.Graph {
	return topology.NewGraph([]topology.Node{{
		Key: key(table), ServiceName: "service-1", SchemaName: e2eSchema, TableName: table,
		NodeType: nodeType, TestCount: testCount,
	}}, nil)
}
