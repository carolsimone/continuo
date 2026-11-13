package runplan

import (
	"cmp"
	"slices"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
)

// SeedReload is the plan of one seed's load in a run: the seed, what the load
// and the rebuilds of the views it drops are, and the preflight its task runs.
type SeedReload struct {
	Seed      topology.NodeKey
	Plan      seedreload.Plan
	Preflight *seedreload.Preflight
}

// ApplySeedReloads returns p with each reload merged in. g holds every node of
// p and every rebuild. The reloads apply in seed-key order. A seed's task
// takes the load's reason, preflight and refusal, and its operation when that
// differs from runOperation. Each rebuild becomes a pending task, an
// inherited one pending again, with its operation under the same rule; the
// first reload that names a rebuild sets its reason and the seed it rebuilds
// for, so a view below two seeds is one task that waits for both. A reload
// whose seed is not a task of p changes nothing.
//
// Ready is every pending task that no pending upstream in g holds back,
// except a refused task and every task below one.
func ApplySeedReloads(p Plan, g topology.Graph, runOperation model.Operation, reloads []SeedReload) Plan {
	byKey := make(map[topology.NodeKey]PlannedTask, len(p.Tasks))
	for _, t := range p.Tasks {
		byKey[t.Key] = t
	}
	ordered := slices.Clone(reloads)
	slices.SortStableFunc(ordered, func(a, b SeedReload) int { return cmp.Compare(a.Seed, b.Seed) })
	for _, r := range ordered {
		task, ok := byKey[r.Seed]
		if !ok {
			continue
		}
		task.Operation = ownOperation(r.Plan.Load.Operation, runOperation)
		task.Reason = r.Plan.Load.Reason
		task.Preflight = r.Preflight
		task.Refusal = r.Plan.Load.Refusal
		byKey[r.Seed] = task
		for _, rb := range r.Plan.Rebuilds {
			key := topology.NodeKey(rb.Key)
			task := byKey[key]
			task.Key = key
			task.Inherited = false
			task.InheritedFrom = uuid.Nil
			if task.RebuildOf == "" {
				task.Operation = ownOperation(rb.Operation, runOperation)
				task.Reason = rb.Reason
				task.RebuildOf = r.Seed
			}
			byKey[key] = task
		}
	}

	tasks := make([]PlannedTask, 0, len(byKey))
	pending := make(map[topology.NodeKey]bool, len(byKey))
	var refused []topology.NodeKey
	for k, t := range byKey {
		tasks = append(tasks, t)
		if !t.Inherited {
			pending[k] = true
		}
		if t.Refusal != "" {
			refused = append(refused, k)
		}
	}
	slices.SortFunc(tasks, func(a, b PlannedTask) int { return cmp.Compare(a.Key, b.Key) })
	blocked := make(map[topology.NodeKey]bool, len(refused))
	for _, k := range refused {
		blocked[k] = true
	}
	for _, k := range g.Descendants(refused) {
		blocked[k] = true
	}
	ready := make([]topology.NodeKey, 0)
	for _, t := range tasks {
		if !blocked[t.Key] && isReady(g, t.Key, pending) {
			ready = append(ready, t.Key)
		}
	}
	return Plan{Tasks: tasks, Ready: ready}
}

// ownOperation returns a pointer to op when a task must carry it, that is when
// the run's operation does not already apply it. model.OperationRun is the
// empty operation, so an empty op is the run's when the run's is run; a build
// run builds a node, which runs it, so it covers run too.
func ownOperation(op, runOperation model.Operation) *model.Operation {
	if op == runOperation || (runOperation == model.OperationBuild && op == model.OperationRun) {
		return nil
	}
	return &op
}

// SeedReloadGraph returns g as the graph the seed reload planner reads.
func SeedReloadGraph(g topology.Graph) seedreload.Graph { return seedGraph{g} }

// SeedNode returns n as the node the seed reload planner reads.
func SeedNode(n topology.Node) seedreload.Node {
	return seedreload.Node{
		Key:          string(n.Key),
		NodeType:     n.NodeType,
		Materialized: n.Materialized,
		FullRefresh:  n.FullRefresh,
		Schedule:     n.ScheduleName,
		Relation:     n.Relation(),
	}
}

type seedGraph struct{ g topology.Graph }

func (s seedGraph) Node(key string) (seedreload.Node, bool) {
	n, ok := s.g.Node(topology.NodeKey(key))
	if !ok {
		return seedreload.Node{}, false
	}
	return SeedNode(n), true
}

func (s seedGraph) Upstreams(key string) []string {
	return keyStrings(s.g.Upstreams(topology.NodeKey(key)))
}

func (s seedGraph) Downstreams(key string) []string {
	return keyStrings(s.g.Downstreams(topology.NodeKey(key)))
}

func keyStrings(keys []topology.NodeKey) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, string(k))
	}
	return out
}
