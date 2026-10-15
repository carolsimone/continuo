package runplan

import (
	"cmp"
	"slices"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/topology"
)

// Rebase selects a rebase of Source onto the live release: a run that
// restarts the source from its failure. g, the graph Select and Preview take,
// is the source schedule's graph on the live release; SourceGraph is the
// source run's release restricted to the source run's nodes.
//
// The seed is every source task that did not succeed and whose node g still
// holds, and every node of g the source never ran. The rebase executes the
// seed and every descendant of it in g and, with IncludeChangedUpstream, every
// changed ancestor of what it executes together with that ancestor's
// descendants, repeated until no changed ancestor is left out. Every other
// source success g still holds is carried over as succeeded; a source task
// whose node g no longer holds is left out of the run.
//
// A node is changed when both releases hold it and its content hash, node
// type, secret ref or upstream nodes differ between SourceGraph and g. Its
// image tag, test count, unique id and owning service are not compared: a
// service release moves the image of every node it owns.
type Rebase struct {
	Source                 Source
	SourceGraph            topology.Graph
	IncludeChangedUpstream bool
}

var _ Selector = Rebase{}

// Preview is a rebase's selection with what it executes counted by reason.
// Each executed task is counted once, under the first reason that holds:
// FailedOrSkipped (the source task did not succeed), New (the source never
// ran the node), ChangedUpstream (a node of Changed, executed because
// IncludeChangedUpstream is set) and Downstream (every other executed task).
// Inherited counts the tasks carried over as succeeded.
type Preview struct {
	Plan            Plan
	FailedOrSkipped int
	New             int
	ChangedUpstream int
	Downstream      int
	Inherited       int
	// Changed holds every changed node IncludeChangedUpstream adds to the
	// execution that is upstream of another node it executes, whether or not
	// the option is set, ordered by service, schema and table.
	Changed []ChangedNode
}

// WillRun returns the number of tasks the rebase executes.
func (p Preview) WillRun() int {
	return p.FailedOrSkipped + p.New + p.ChangedUpstream + p.Downstream
}

// Total returns the number of tasks of the rebase.
func (p Preview) Total() int { return p.WillRun() + p.Inherited }

// ChangedNode is a changed node upstream of what a rebase executes, named as
// the live release names it. Downstream is the number of its descendants in
// the live schedule graph.
type ChangedNode struct {
	Key         topology.NodeKey
	ServiceName string
	SchemaName  string
	TableName   string
	Downstream  int
}

// Select returns the plan Preview selects.
func (s Rebase) Select(g topology.Graph) (Plan, error) {
	pv, err := s.Preview(g)
	return pv.Plan, err
}

// Preview selects the rebase over g and counts its tasks. It refuses a test
// source (ErrRerunOfTestUnsupported), a selection with nothing to execute
// (ErrNothingToRerun) and an undeclared node type among the tasks it executes
// (ErrInvalidNodeType).
func (s Rebase) Preview(g topology.Graph) (Preview, error) {
	if s.Source.Operation == model.OperationTest {
		return Preview{}, ErrRerunOfTestUnsupported
	}
	unsucceeded := make(map[topology.NodeKey]bool)
	for _, k := range unsucceededIn(g, s.Source.Tasks) {
		unsucceeded[k] = true
	}
	ran := make(map[topology.NodeKey]bool, len(s.Source.Tasks))
	for _, t := range s.Source.Tasks {
		ran[t.Key] = true
	}
	seed := keysOf(unsucceeded)
	for _, k := range g.Keys() {
		if !ran[k] {
			seed = append(seed, k)
		}
	}

	closure := withDescendants(g, seed)
	widened := s.widen(g, closure)
	changed := s.changedUpstream(g, closure, widened)
	executed := closure
	if s.IncludeChangedUpstream {
		executed = widened
	}
	if len(executed) == 0 {
		return Preview{}, ErrNothingToRerun
	}
	if err := checkNodeTypes(g, keysOf(executed)); err != nil {
		return Preview{}, err
	}

	pv := Preview{Plan: dagPlan(g, executed, succeededIn(g, s.Source.Tasks)), Changed: changed}
	named := make(map[topology.NodeKey]bool, len(changed))
	for _, c := range changed {
		named[c.Key] = true
	}
	for k := range executed {
		switch {
		case unsucceeded[k]:
			pv.FailedOrSkipped++
		case !ran[k]:
			pv.New++
		case named[k]:
			pv.ChangedUpstream++
		default:
			pv.Downstream++
		}
	}
	pv.Inherited = len(pv.Plan.Tasks) - len(executed)
	return pv, nil
}

// widen returns closure with every changed ancestor of its members and every
// descendant of those, repeated until a pass adds nothing. Each pass looks at
// the ancestors of the members the previous pass added only: the result stays
// closed under descendants, so an earlier member's ancestors have been looked
// at already.
func (s Rebase) widen(g topology.Graph, closure map[topology.NodeKey]bool) map[topology.NodeKey]bool {
	widened := make(map[topology.NodeKey]bool, len(closure))
	for k := range closure {
		widened[k] = true
	}
	added := keysOf(closure)
	for len(added) > 0 {
		var roots []topology.NodeKey
		for _, a := range g.Ancestors(added) {
			if !widened[a] && s.changed(g, a) {
				roots = append(roots, a)
			}
		}
		added = nil
		for _, k := range append(roots, g.Descendants(roots)...) {
			if !widened[k] {
				widened[k] = true
				added = append(added, k)
			}
		}
	}
	return widened
}

// changedUpstream returns the changed nodes widening added to closure that
// have a descendant in g, ordered by service, schema and table. widened is
// closed under descendants, so each such descendant is one widened executes.
func (s Rebase) changedUpstream(g topology.Graph, closure, widened map[topology.NodeKey]bool) []ChangedNode {
	var out []ChangedNode
	for k := range widened {
		if closure[k] || !s.changed(g, k) || len(g.Downstreams(k)) == 0 {
			continue
		}
		n, _ := g.Node(k)
		out = append(out, ChangedNode{
			Key: k, ServiceName: n.ServiceName, SchemaName: n.SchemaName, TableName: n.TableName,
			Downstream: len(g.Descendants([]topology.NodeKey{k})),
		})
	}
	slices.SortFunc(out, func(a, b ChangedNode) int {
		return cmp.Or(
			cmp.Compare(a.ServiceName, b.ServiceName),
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.TableName, b.TableName),
			cmp.Compare(a.Key, b.Key),
		)
	})
	return out
}

// changed reports whether g and SourceGraph both hold k and what k executes
// differs between them.
func (s Rebase) changed(g topology.Graph, k topology.NodeKey) bool {
	live, ok := g.Node(k)
	if !ok {
		return false
	}
	was, ok := s.SourceGraph.Node(k)
	if !ok {
		return false
	}
	return live.ContentHash != was.ContentHash ||
		live.NodeType != was.NodeType ||
		live.SecretRef != was.SecretRef ||
		!slices.Equal(g.Upstreams(k), s.SourceGraph.Upstreams(k))
}
