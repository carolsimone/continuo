package runplan

import (
	"slices"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
)

// root returns the task that last ran t's node: t's own inherited-from task
// when t was itself inherited, else t.
func (t SourceTask) root() uuid.UUID {
	if t.InheritedFrom != uuid.Nil {
		return t.InheritedFrom
	}
	return t.TaskID
}

// unsucceededIn returns the keys of the source tasks that did not succeed and
// whose node g holds.
func unsucceededIn(g topology.Graph, tasks []SourceTask) []topology.NodeKey {
	var out []topology.NodeKey
	for _, t := range tasks {
		if _, ok := g.Node(t.Key); ok && !t.Succeeded {
			out = append(out, t.Key)
		}
	}
	return out
}

// succeededIn maps each succeeded source task whose node g holds to the task
// that last ran the node.
func succeededIn(g topology.Graph, tasks []SourceTask) map[topology.NodeKey]uuid.UUID {
	out := make(map[topology.NodeKey]uuid.UUID)
	for _, t := range tasks {
		if _, ok := g.Node(t.Key); ok && t.Succeeded {
			out[t.Key] = t.root()
		}
	}
	return out
}

// withDescendants returns keys and every descendant of theirs in g.
func withDescendants(g topology.Graph, keys []topology.NodeKey) map[topology.NodeKey]bool {
	out := make(map[topology.NodeKey]bool)
	for _, k := range keys {
		out[k] = true
	}
	for _, k := range g.Descendants(keys) {
		out[k] = true
	}
	return out
}

// carryRebuilds returns p with each pending task that re-executes a source
// rebuild given the rebuild's operation (when the run's operation does not
// already apply it), reason and seed, when that seed is an inherited task of
// p: the seed's load already dropped the relation, so the rebuild is still
// owed, and no seed reload of the new run plans it again. A rebuild whose seed
// the new run loads again is planned by that load.
func carryRebuilds(p Plan, tasks []SourceTask, runOperation model.Operation) Plan {
	inherited := make(map[topology.NodeKey]bool)
	for _, t := range p.Tasks {
		if t.Inherited {
			inherited[t.Key] = true
		}
	}
	rebuilds := make(map[topology.NodeKey]SourceTask)
	for _, t := range tasks {
		if t.RebuildOf != "" && inherited[t.RebuildOf] {
			rebuilds[t.Key] = t
		}
	}
	if len(rebuilds) == 0 {
		return p
	}
	out := Plan{Tasks: slices.Clone(p.Tasks), Ready: p.Ready}
	for i, t := range out.Tasks {
		src, ok := rebuilds[t.Key]
		if !ok || t.Inherited {
			continue
		}
		out.Tasks[i].Operation = ownOperation(src.Operation, runOperation)
		out.Tasks[i].Reason = src.Reason
		out.Tasks[i].RebuildOf = src.RebuildOf
	}
	return out
}

func keysOf(set map[topology.NodeKey]bool) []topology.NodeKey {
	out := make([]topology.NodeKey, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
