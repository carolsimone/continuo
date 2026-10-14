package runplan

import (
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/topology"
)

// Rebase selects a rebase of Source onto the live release. g is the source
// schedule's graph on the live release. Executed: the source tasks that did
// not succeed and whose node g still holds, every descendant of theirs in g,
// and every node of g the source never ran. Carried over as succeeded: every
// other source success g still holds. A source task whose node g no longer
// holds is left out of the run.
type Rebase struct {
	Source Source
}

var _ Selector = Rebase{}

// Select refuses a test source (ErrRerunOfTestUnsupported), a selection with
// nothing to execute (ErrNothingToRerun) and an undeclared node type among
// the tasks it executes (ErrInvalidNodeType).
func (s Rebase) Select(g topology.Graph) (Plan, error) {
	if s.Source.Operation == model.OperationTest {
		return Plan{}, ErrRerunOfTestUnsupported
	}
	pending := withDescendants(g, unsucceededIn(g, s.Source.Tasks))
	ran := make(map[topology.NodeKey]bool, len(s.Source.Tasks))
	for _, t := range s.Source.Tasks {
		ran[t.Key] = true
	}
	for _, k := range g.Keys() {
		if !ran[k] {
			pending[k] = true
		}
	}
	if len(pending) == 0 {
		return Plan{}, ErrNothingToRerun
	}
	if err := checkNodeTypes(g, keysOf(pending)); err != nil {
		return Plan{}, err
	}
	return dagPlan(g, pending, succeededIn(g, s.Source.Tasks)), nil
}
