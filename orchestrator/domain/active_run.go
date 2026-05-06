package domain

// ActiveRun is a read-side projection of an in-flight :Run node — a run that
// has been started but not yet finalized (i.e. completed_at IS NULL on Neo4j).
//
// It carries the run's identity plus the topology_generation it was pinned to
// at SnapshotGraph time. Consumers compare TopologyGeneration against the
// orchestrator's current topology_state.topology_generation to determine drift.
type ActiveRun struct {
	ScheduleName       string
	RunID              string
	TopologyGeneration int64
}
