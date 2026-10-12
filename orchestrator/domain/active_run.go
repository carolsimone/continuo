package domain

// ActiveRun is a read-side projection of an in-flight :Run node — a run that
// has been started but not yet finalized (i.e. completed_at IS NULL on Neo4j).
//
// It carries the run's identity plus the promotion seq it was created under.
// Consumers compare PromotionSeq against the live promotion seq to determine
// drift; 0 means the run carries no promotion seq (drift unknown).
type ActiveRun struct {
	ScheduleName string
	RunID        string
	PromotionSeq int64
}
