package runplan

import (
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
)

// WholeDAG selects a schedule's run, fired by cron or triggered. g is the
// schedule's graph on the live release: the schedule's nodes, their one-hop
// dbt-seed upstreams from any schedule, and the edges among them. A run or a
// build executes every node of g along its edges, and a node with no upstream
// in g is ready at creation. A test run is the flat fan-out testFanOut
// selects.
type WholeDAG struct {
	Operation model.Operation
}

var _ Selector = WholeDAG{}

// Select plans the run over g. An empty g is ErrEmptySelection whatever the
// operation.
func (s WholeDAG) Select(g topology.Graph) (Plan, error) {
	if g.Len() == 0 {
		return Plan{}, ErrEmptySelection
	}
	if s.Operation == model.OperationTest {
		return testFanOut(g)
	}
	keys := g.Keys()
	if err := checkNodeTypes(g, keys); err != nil {
		return Plan{}, err
	}
	all := make(map[topology.NodeKey]bool, len(keys))
	for _, k := range keys {
		all[k] = true
	}
	return dagPlan(g, all, nil), nil
}
