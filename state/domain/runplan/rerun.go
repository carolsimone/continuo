package runplan

import (
	"fmt"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/topology"
)

// Rerun selects a rerun of Source on the source run's own release, so it
// reproduces the source's DAG whatever has been promoted since. g is that
// release restricted to the source run's nodes; Select restricts it again, so
// a wider slice selects the same plan. The source tasks that did not succeed
// and every descendant of theirs among the source's tasks are executed along
// the source release's edges; every other source task is carried over as
// succeeded.
type Rerun struct {
	Source Source
}

var _ Selector = Rerun{}

// Select refuses a test source (ErrRerunOfTestUnsupported), a source task
// whose node g does not hold (ErrTargetNotFound), a source with nothing to
// execute again (ErrNothingToRerun) and an undeclared node type among the
// tasks it executes (ErrInvalidNodeType).
func (s Rerun) Select(g topology.Graph) (Plan, error) {
	if s.Source.Operation == model.OperationTest {
		return Plan{}, ErrRerunOfTestUnsupported
	}
	ran := make([]topology.NodeKey, 0, len(s.Source.Tasks))
	for _, t := range s.Source.Tasks {
		if _, ok := g.Node(t.Key); !ok {
			return Plan{}, fmt.Errorf("%w: source task %s (%s)", ErrTargetNotFound, t.TaskID, t.Key)
		}
		ran = append(ran, t.Key)
	}
	g = g.Restrict(ran)

	pending := withDescendants(g, unsucceededIn(g, s.Source.Tasks))
	if len(pending) == 0 {
		return Plan{}, ErrNothingToRerun
	}
	if err := checkNodeTypes(g, keysOf(pending)); err != nil {
		return Plan{}, err
	}
	return dagPlan(g, pending, succeededIn(g, s.Source.Tasks)), nil
}
