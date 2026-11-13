package runplan

import (
	"errors"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
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
	// Operation is the dbt verb the task applies to its node; nil keeps the
	// run's.
	Operation *model.Operation
	// Reason says why the task runs with its operation; empty when the plan
	// states none.
	Reason string
	// Preflight is the check a seed's load runs before it drops its table;
	// nil on every other task.
	Preflight *seedreload.Preflight
	// RebuildOf is the seed whose load drops the relation this task
	// rebuilds; empty on every other task.
	RebuildOf topology.NodeKey
	// Refusal is why the planner refused the task, which never runs: the
	// run stores it failed and skips every pending task below it. Empty on a
	// task the run may run.
	Refusal model.FailureReason
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
	// Operation is the operation the source task applied to its node.
	Operation model.Operation
	// Reason is the source task's plan reason; empty when its plan stated
	// none.
	Reason string
	// RebuildOf is the seed whose load dropped the relation the source task
	// rebuilt; empty on every other task.
	RebuildOf topology.NodeKey
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
	// ErrIngestedNode: a run names a node of the ingested type, a table that
	// nodes read and no node builds. It is never a task, so no run executes
	// it.
	ErrIngestedNode = errors.New("an ingested node is never run")
	// ErrSeedFullRefreshDisabled: an operator's full refresh names a seed
	// whose +full_refresh: false keeps its table, so no command rebuilds it.
	ErrSeedFullRefreshDisabled = errors.New("seed_full_refresh_disabled: the seed sets +full_refresh: false, so a full refresh cannot rebuild it")
	// ErrAcknowledgementNotAllowed: a request names acknowledged dependents
	// for a run that is not a full refresh of a dbt seed, the only run whose
	// load drops objects an operator can accept losing.
	ErrAcknowledgementNotAllowed = errors.New("acknowledged_dependents is valid only with operation full_refresh on a dbt seed")
)
