package runplan

import (
	"fmt"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
)

// SingleNode selects a run of one node, named by key and by the project that
// owns it. g is the release the run reads: the live release, or the source
// run's release for a run of a node as that run built it. The task is ready
// at creation.
type SingleNode struct {
	Key         topology.NodeKey
	ProjectName string
	Operation   model.Operation
}

var _ Selector = SingleNode{}

// Select refuses a node g does not hold or another project owns
// (ErrTargetNotFound), an unknown-external node (ErrExternalNode), an undeclared node
// type (ErrInvalidNodeType), a test of a node without a known, positive
// data-test count (ErrNoTests) and a full refresh of a type that cannot be
// rebuilt from scratch (ErrFullRefreshUnsupported). An unknown-external node has no
// project, so it is refused as such before the project is compared.
func (s SingleNode) Select(g topology.Graph) (Plan, error) {
	n, ok := g.Node(s.Key)
	if ok {
		if err := checkNotExternal(s.Key, n); err != nil {
			return Plan{}, err
		}
	}
	if !ok || n.ProjectName != s.ProjectName {
		return Plan{}, fmt.Errorf("%w: %s of project %q", ErrTargetNotFound, s.Key, s.ProjectName)
	}
	if err := checkNodeTypes(g, []topology.NodeKey{s.Key}); err != nil {
		return Plan{}, err
	}
	switch s.Operation {
	case model.OperationTest:
		if !hasTests(n) {
			return Plan{}, fmt.Errorf("%w: %s", ErrNoTests, s.Key)
		}
	case model.OperationFullRefresh:
		if !model.NodeType(n.NodeType).SupportsFullRefresh() {
			return Plan{}, fmt.Errorf("%w: %s is %q", ErrFullRefreshUnsupported, s.Key, n.NodeType)
		}
	}
	return flatPlan([]topology.NodeKey{s.Key}), nil
}
