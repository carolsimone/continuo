package domain

// ActiveRun is a read-side projection of an in-flight :Run node — a run that
// has been started but not yet finalized (i.e. completed_at IS NULL on Neo4j).
//
// It carries the run's identity plus the promotion seq it was created under.
// Consumers compare TopologyGeneration against the live topology's promotion
// seq to determine drift; 0 means the run predates promotion seqs.
type ActiveRun struct {
	ScheduleName       string
	RunID              string
	TopologyGeneration int64
}
