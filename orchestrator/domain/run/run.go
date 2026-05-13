package run

// Run is the aggregate root. It enforces all invariants over a single pipeline run.
// nodes holds only the operation-scoped subgraph loaded by AggregateRepository.Load.
type Run struct {
	RunID         string
	ScheduleName  string
	Status        RunStatus
	TotalNodes    int
	TerminalCount int
	Version       int
	nodes         map[NodeKey]*RunNode
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
