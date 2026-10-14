package runplan

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

// isReady is the readiness predicate every DAG-ordered plan applies at
// creation: a pending task is ready when no upstream of it in g is a pending
// task of the plan. An upstream that is inherited has already succeeded, and
// an upstream that is not a task of the run does not hold it back.
func isReady(g topology.Graph, k topology.NodeKey, pending map[topology.NodeKey]bool) bool {
	if !pending[k] {
		return false
	}
	for _, u := range g.Upstreams(k) {
		if pending[u] {
			return false
		}
	}
	return true
}

// dagPlan builds the plan of a run whose tasks wait for their upstreams along
// g's edges. pending holds the keys the run executes; inherited maps each key
// carried over as succeeded to the task that last ran it. A key in both is
// pending.
func dagPlan(g topology.Graph, pending map[topology.NodeKey]bool, inherited map[topology.NodeKey]uuid.UUID) Plan {
	tasks := make([]PlannedTask, 0, len(pending)+len(inherited))
	ready := make([]topology.NodeKey, 0)
	for k := range pending {
		tasks = append(tasks, PlannedTask{Key: k})
		if isReady(g, k, pending) {
			ready = append(ready, k)
		}
	}
	for k, root := range inherited {
		if pending[k] {
			continue
		}
		tasks = append(tasks, PlannedTask{Key: k, Inherited: true, InheritedFrom: root})
	}
	slices.SortFunc(tasks, func(a, b PlannedTask) int { return cmp.Compare(a.Key, b.Key) })
	slices.Sort(ready)
	return Plan{Tasks: tasks, Ready: ready}
}

// flatPlan builds the plan of a run whose tasks wait for nothing: one pending
// task per distinct key, every one ready.
func flatPlan(keys []topology.NodeKey) Plan {
	distinct := sortedDistinct(keys)
	tasks := make([]PlannedTask, 0, len(distinct))
	for _, k := range distinct {
		tasks = append(tasks, PlannedTask{Key: k})
	}
	return Plan{Tasks: tasks, Ready: distinct}
}

// checkNodeTypes refuses a selection that would execute a node whose type the
// node_type vocabulary does not declare. keys are checked in sorted order, so
// the refusal names the same node whatever order they come in.
func checkNodeTypes(g topology.Graph, keys []topology.NodeKey) error {
	for _, k := range sortedDistinct(keys) {
		n, ok := g.Node(k)
		if ok && !model.NodeType(n.NodeType).IsValid() {
			return fmt.Errorf("%w: %s has node type %q", ErrInvalidNodeType, k, n.NodeType)
		}
	}
	return nil
}

func sortedDistinct(keys []topology.NodeKey) []topology.NodeKey {
	out := slices.Clone(keys)
	slices.Sort(out)
	return slices.Compact(out)
}
