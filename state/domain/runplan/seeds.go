package runplan

import (
	"fmt"

	"github.com/carolsimone/continuo/state/domain/topology"
)

// Seeds selects a promote-seed run: the seeds of the live release the seed
// reconcile chose to build. g is the live release, or any slice of it that
// holds them. Seeds read nothing from one another, so every task is ready at
// creation; a key named twice is one task.
type Seeds struct {
	Keys []topology.NodeKey
}

var _ Selector = Seeds{}

// Select refuses an empty set (ErrEmptySelection), a key g does not hold
// (ErrTargetNotFound) and an undeclared node type (ErrInvalidNodeType).
func (s Seeds) Select(g topology.Graph) (Plan, error) {
	if len(s.Keys) == 0 {
		return Plan{}, ErrEmptySelection
	}
	keys := sortedDistinct(s.Keys)
	for _, k := range keys {
		if _, ok := g.Node(k); !ok {
			return Plan{}, fmt.Errorf("%w: %s", ErrTargetNotFound, k)
		}
	}
	if err := checkNodeTypes(g, keys); err != nil {
		return Plan{}, err
	}
	return flatPlan(keys), nil
}
