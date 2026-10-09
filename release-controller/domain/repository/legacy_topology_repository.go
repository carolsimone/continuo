package repository

import (
	"context"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// LegacyRunTopology is a run that still holds its candidate topology inline in
// the candidate_topology column, with no topology artifact.
type LegacyRunTopology struct {
	RunID    string
	Topology release.Topology
}

// LegacyCurrentProd is current_prod as stored before topology artifacts
// existed: the release it names and that release's topology snapshot.
type LegacyCurrentProd struct {
	ReleaseID string
	Topology  release.Topology
}

// LegacyTopologyRepository reads and settles the runs that hold an inline
// candidate topology or were left waiting on a parse result. Only the one-time
// upgrade step uses it.
type LegacyTopologyRepository interface {
	// ListRunsWithLegacyTopology returns every run holding an inline candidate
	// topology and no artifact reference, oldest first.
	ListRunsWithLegacyTopology(ctx context.Context) ([]LegacyRunTopology, error)
	// SetRunTopologyRef records the artifact a legacy run's topology was moved into.
	SetRunTopologyRef(ctx context.Context, runID string, ref release.TopologyRef) error
	// ListParsingAtUpgrade returns the runs that were parsing when the move to
	// topology artifacts was migrated and are parsing still, oldest first.
	ListParsingAtUpgrade(ctx context.Context) ([]string, error)
	// ClearParsingAtUpgrade removes runID's mark once the run is settled.
	ClearParsingAtUpgrade(ctx context.Context, runID string) error
	// GetLegacyCurrentProd returns current_prod's release and legacy topology
	// snapshot when current_prod names a release but references no topology
	// artifact, and nil otherwise.
	GetLegacyCurrentProd(ctx context.Context) (*LegacyCurrentProd, error)
}
