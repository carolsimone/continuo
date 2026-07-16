package run

import "fmt"

// Run is the aggregate root. It enforces all invariants over a single pipeline run.
// nodes holds only the operation-scoped subgraph loaded by AggregateRepository.Rehydrate.
// FailedCount is persisted on :Run so finalisation can answer "did any node fail?"
// without needing every node in the loaded subgraph.
type Run struct {
	RunID         string
	ScheduleName  string
	Status        RunStatus
	TotalNodes    int
	TerminalCount int
	FailedCount   int
	Version       int
	// Operation is the run's operation ("" | "test" | "build"), set from the
	// :Run node on rehydrate. It is stamped onto every NodeUnblocked so the
	// downstream unblock dispatch runs the same dbt verb as the frontier.
	Operation string
	nodes     map[NodeKey]*RunNode
}

// NewRun constructs a Run aggregate from its initial node set.
// It is called once after the snapshot creates the Neo4j :Run node and
// :EXECUTES edges. TotalNodes is set to len(nodes).
func NewRun(runID, scheduleName string, nodes []*RunNode) *Run {
	r := &Run{
		RunID:        runID,
		ScheduleName: scheduleName,
		Status:       RunStatusInitialized,
		TotalNodes:   len(nodes),
		nodes:        make(map[NodeKey]*RunNode, len(nodes)),
	}
	for _, n := range nodes {
		r.nodes[n.Key] = n
	}
	return r
}

// Nodes returns a copy of the loaded subgraph. Read-only; used by the adapter
// to determine which nodes to persist in Save.
func (r *Run) Nodes() []*RunNode {
	out := make([]*RunNode, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, n)
	}
	return out
}

// CompleteNode transitions the target node to status and enforces all downstream
// invariants within the loaded subgraph. Returns domain events describing side effects.
func (r *Run) CompleteNode(key NodeKey, status string) ([]DomainEvent, error) {
	n, ok := r.nodes[key]
	if !ok {
		return nil, ErrNodeNotInScope
	}
	if n.isTerminal() {
		return nil, ErrNodeAlreadyTerminal
	}

	n.Status = status
	r.TerminalCount++
	r.Version++
	if status == "FAILED" {
		r.FailedCount++
	}

	var events []DomainEvent

	switch status {
	case "FAILED":
		events = append(events, r.cascadeSkip(key)...)
	case "SUCCEEDED", "SKIPPED":
		events = append(events, r.checkUnblocked(key)...)
	}

	if r.TerminalCount == r.TotalNodes {
		terminalStatus := "SUCCEEDED"
		if r.FailedCount > 0 {
			terminalStatus = "FAILED"
		}
		r.Status = RunStatus(terminalStatus)
		events = append(events, RunFinalized{
			RunID:          r.RunID,
			ScheduleName:   r.ScheduleName,
			TerminalStatus: terminalStatus,
		})
	} else if r.Status == RunStatusInitialized {
		r.Status = RunStatusInProgress
	}

	return events, nil
}

// EffectsForTerminal recomputes the side-effect events that should have been
// emitted when key transitioned to its current terminal status. It does not
// mutate state. The handler uses it on idempotent re-delivery — when Neo4j
// already reflects the terminal transition but the Postgres outbox writes
// from the prior attempt were rolled back — to rebuild the outbox.
func (r *Run) EffectsForTerminal(key NodeKey) ([]DomainEvent, error) {
	n, ok := r.nodes[key]
	if !ok {
		return nil, ErrNodeNotInScope
	}
	if !n.isTerminal() {
		return nil, fmt.Errorf("run: EffectsForTerminal called on non-terminal node %v", key)
	}
	var events []DomainEvent
	switch n.Status {
	case "FAILED":
		events = append(events, r.gatherCascadedSkipped(key)...)
	case "SUCCEEDED", "SKIPPED":
		events = append(events, r.checkUnblocked(key)...)
	}
	return events, nil
}

// gatherCascadedSkipped walks the same path as cascadeSkip but reads current
// status instead of mutating. The fan-out mirrors cascadeSkip exactly so the
// reconstructed events match what the original mutation produced.
func (r *Run) gatherCascadedSkipped(from NodeKey) []DomainEvent {
	var events []DomainEvent
	n, ok := r.nodes[from]
	if !ok {
		return events
	}
	for _, dk := range n.Downstreams {
		downstream, ok := r.nodes[dk]
		if !ok {
			continue
		}
		if downstream.Status == "SKIPPED" {
			events = append(events, NodeCascadeSkipped{Key: dk, TaskID: downstream.TaskID})
			events = append(events, r.gatherCascadedSkipped(dk)...)
			events = append(events, r.checkUnblocked(dk)...)
		}
	}
	return events
}

// cascadeSkip recursively marks all reachable PENDING downstream nodes as SKIPPED.
func (r *Run) cascadeSkip(from NodeKey) []DomainEvent {
	var events []DomainEvent
	n := r.nodes[from]
	for _, dk := range n.Downstreams {
		downstream, ok := r.nodes[dk]
		if !ok || downstream.isTerminal() {
			continue
		}
		downstream.Status = "SKIPPED"
		r.TerminalCount++
		events = append(events, NodeCascadeSkipped{Key: dk, TaskID: downstream.TaskID})
		events = append(events, r.cascadeSkip(dk)...)
		events = append(events, r.checkUnblocked(dk)...)
	}
	return events
}

// checkUnblocked emits NodeUnblocked for each immediate downstream node whose
// every upstream is now terminal.
func (r *Run) checkUnblocked(from NodeKey) []DomainEvent {
	var events []DomainEvent
	n := r.nodes[from]
	for _, dk := range n.Downstreams {
		downstream, ok := r.nodes[dk]
		if !ok || downstream.isTerminal() {
			continue
		}
		allTerminal := true
		for _, upKey := range downstream.Upstreams {
			up, inScope := r.nodes[upKey]
			if !inScope || !up.isTerminal() {
				allTerminal = false
				break
			}
		}
		if allTerminal {
			events = append(events, NodeUnblocked{
				Key:                dk,
				TaskID:             downstream.TaskID,
				ScheduleName:       downstream.ScheduleName,
				NodeType:           downstream.NodeType,
				ManifestVersion:    downstream.ManifestVersion,
				ImageTag:           downstream.ImageTag,
				DBTUniqueID:        downstream.DBTUniqueID,
				RuntimeManifestRef: downstream.RuntimeManifestRef,
				Operation:          r.Operation,
			})
		}
	}
	return events
}
