package runplan

import (
	"fmt"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
)

// SingleNode selects a run of one node, named by key and by the service that
// owns it. g is the release the run reads: the live release, or the source
// run's release for a run of a node as that run built it. The task is ready
// at creation.
type SingleNode struct {
	Key         topology.NodeKey
	ServiceName string
	Operation   model.Operation
}

var _ Selector = SingleNode{}

// Select refuses a node g does not hold or another service owns
// (ErrTargetNotFound), an undeclared node type (ErrInvalidNodeType), a test
// of a node without a known, positive data-test count (ErrNoTests) and a full
// refresh of a type that cannot be rebuilt from scratch
// (ErrFullRefreshUnsupported).
func (s SingleNode) Select(g topology.Graph) (Plan, error) {
	n, ok := g.Node(s.Key)
	if !ok || n.ServiceName != s.ServiceName {
		return Plan{}, fmt.Errorf("%w: %s of service %q", ErrTargetNotFound, s.Key, s.ServiceName)
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
