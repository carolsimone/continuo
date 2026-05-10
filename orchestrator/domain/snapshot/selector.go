package snapshot

import "context"

// Selector picks the projection for a given run. Implementations are pure Go
// and read all data they need through the TopologyReader port. The TopologyReader
// is bound to a Neo4j tx by the adapter, so reads inside SelectTasks and the
// subsequent SnapshotWriter call commit in one Cypher transaction.
type Selector interface {
	SelectTasks(ctx context.Context, r TopologyReader, p Params) ([]TaskProjection, error)
}
