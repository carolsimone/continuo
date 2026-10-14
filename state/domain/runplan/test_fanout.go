package runplan

import "github.com/carolsimone/continuo/state/domain/topology"

// testFanOut selects a whole-schedule test run: one task per node of g with a
// known, positive data-test count, none waiting for another. A node whose
// count is zero or unknown is left out, so no test run dispatches a dbt test
// that selects nothing; a g without such a node is ErrNoTests.
func testFanOut(g topology.Graph) (Plan, error) {
	var keys []topology.NodeKey
	for _, k := range g.Keys() {
		if n, _ := g.Node(k); hasTests(n) {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return Plan{}, ErrNoTests
	}
	if err := checkNodeTypes(g, keys); err != nil {
		return Plan{}, err
	}
	return flatPlan(keys), nil
}

// hasTests reports whether n has a known, positive data-test count.
func hasTests(n topology.Node) bool { return n.TestCount != nil && *n.TestCount > 0 }
