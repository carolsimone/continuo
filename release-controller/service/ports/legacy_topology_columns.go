package ports

import "context"

// LegacyTopologyColumns reads the release-controller database schema for the
// columns that held topologies inline before topology artifacts:
// release_pipeline_runs.candidate_topology,
// release_pipeline_runs.parsing_at_upgrade and current_prod.topology_snapshot.
// Release migration V26 drops them.
type LegacyTopologyColumns interface {
	// Present lists the columns that still exist, as "<table>.<column>" in
	// name order. An empty list means the migration has applied.
	Present(ctx context.Context) ([]string, error)
}
