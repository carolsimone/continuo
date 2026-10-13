package repository

import (
	"context"
	"errors"
	"time"

	"github.com/carolsimone/continuo/state/domain/topology"
)

// ErrReleaseChecksumConflict reports a promotion of a release state has already
// stored, announced with a different topology artifact checksum. A release's
// topology never changes, so no retry can reconcile the two.
var ErrReleaseChecksumConflict = errors.New("a loaded release arrived with a different topology checksum")

// TopologyRepository stores promoted releases' topologies and each tenant's
// live-release pointer. A release's nodes and edges are written once, with its
// row, and never change.
type TopologyRepository interface {
	// LockTopology takes the transaction-scoped topology lock. Every pointer
	// step and seed reconcile holds it, so two of them never interleave,
	// whichever pod runs them. It needs an open transaction.
	LockTopology(ctx context.Context) error

	// SaveRelease stores rel once. A new release is inserted with its nodes and
	// edges and reports loaded. A stored one reports not loaded: a different
	// checksum wraps ErrReleaseChecksumConflict, and a higher promotion seq is
	// recorded. It needs an open transaction.
	SaveRelease(ctx context.Context, rel topology.Release) (loaded bool, err error)

	// LivePointer returns the tenant's live release, or the zero Pointer when
	// none is live.
	LivePointer(ctx context.Context, tenantID string) (topology.Pointer, error)

	// SetLivePointer makes p the tenant's live release as of at.
	SetLivePointer(ctx context.Context, tenantID string, p topology.Pointer, at time.Time) error

	// ReleaseNodes returns the release's nodes of nodeType ("" for every type)
	// in key order.
	ReleaseNodes(ctx context.Context, tenantID, releaseID, nodeType string) ([]topology.Node, error)

	// NodeContentHash returns the content hash of one node of a release, and
	// whether the release has that node.
	NodeContentHash(ctx context.Context, tenantID, releaseID string, key topology.NodeKey) (hash string, found bool, err error)

	// PruneReleases deletes, with their nodes and edges, the releases loaded
	// before loadedBefore that are not live and that no pending or running run
	// builds, and returns how many it deleted. Callers hold LockTopology in the
	// same transaction.
	PruneReleases(ctx context.Context, loadedBefore time.Time) (int64, error)
}
