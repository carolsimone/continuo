package runplan

import (
	"errors"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
)

// PlannedTask is one task of a planned run.
type PlannedTask struct {
	Key topology.NodeKey
	// Inherited marks a task carried over from the source run as succeeded.
	// An inherited task is never dispatched.
	Inherited bool
	// InheritedFrom is the task that last ran the node: the source task's own
	// InheritedFrom when it was itself inherited, else the source task.
	// uuid.Nil when Inherited is false.
	InheritedFrom uuid.UUID
}

// Plan is the selection for one run.
type Plan struct {
	// Tasks holds every task of the run, sorted by Key.
	Tasks []PlannedTask
	// Ready holds the pending tasks to dispatch when the run is created,
	// sorted.
	Ready []topology.NodeKey
}

// HasPending reports whether the plan holds a task that is not inherited. A
// plan without one has nothing to execute, so its run is finished as soon as
// it starts.
func (p Plan) HasPending() bool {
	for _, t := range p.Tasks {
		if !t.Inherited {
			return true
		}
	}
	return false
}

// SourceTask is one task of a source run as a derived run sees it.
type SourceTask struct {
	Key    topology.NodeKey
	TaskID uuid.UUID
	// Succeeded reports whether the task ended succeeded. A failed,
	// cancelled, skipped or unfinished task did not.
	Succeeded bool
	// InheritedFrom is the source task's own inherited-from task, uuid.Nil
	// when the source task ran the node itself.
	InheritedFrom uuid.UUID
	// BuiltOn is the release the task's result was built on: the source
	// run's release when the source run ran the node itself, the release of
	// the run of its InheritedFrom task when it inherited the result.
	BuiltOn string
}

// Source is the run a rerun or a rebase derives from: its operation and its
// tasks.
type Source struct {
	Operation model.Operation
	Tasks     []SourceTask
}

// Selector chooses the tasks of a run from g, the slice of a release the
// run's kind reads.
type Selector interface {
	Select(g topology.Graph) (Plan, error)
}

// Refusals. A Selector wraps one of these, naming the node or the value that
// caused it; callers compare with errors.Is.
var (
	// ErrEmptySelection: the selection holds no node.
	ErrEmptySelection = errors.New("runplan: the selection is empty")
	// ErrNoTests: a test run selected no node with a known, positive
	// data-test count.
	ErrNoTests = errors.New("runplan: no node in the selection has data tests")
	// ErrTargetNotFound: a node the run names is not in the release, or not
	// in the service the run names.
	ErrTargetNotFound = errors.New("runplan: the target node is not in the release")
	// ErrFullRefreshUnsupported: a full refresh names a node whose type
	// cannot be rebuilt from scratch.
	ErrFullRefreshUnsupported = errors.New("runplan: the node type does not support full refresh")
	// ErrRerunOfTestUnsupported: the source run is a test run. A derived run
	// re-executes nodes with the source's tasks as its only input, which say
	// nothing about the tests a test run selected.
	ErrRerunOfTestUnsupported = errors.New("runplan: a test run cannot be rerun or rebased")
	// ErrInvalidNodeType: a node the run would execute has a node type the
	// node_type vocabulary does not declare, so no toolchain can build it.
	ErrInvalidNodeType = errors.New("runplan: a selected node has a node type the vocabulary does not declare")
	// ErrNothingToRerun: a rerun or a rebase would execute no task.
	ErrNothingToRerun = errors.New("runplan: the source run has no task to re-execute")
)
